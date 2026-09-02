package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/codes"
)

// Bus combines publishing, subscribing and lifecycle management.
type Bus interface {
	Publisher
	Subscriber
	Close(ctx context.Context) error
}

// StreamSpec declares a JetStream stream and the subjects it captures.
type StreamSpec struct {
	Name     string
	Subjects []string
}

// JetStreamConfig configures ConnectJetStream.
type JetStreamConfig struct {
	URL         string
	Streams     []StreamSpec
	DedupWindow time.Duration
	// Consumer tuning; zero values use AckWait 30s and MaxDeliver 10.
	// Handler errors are NAKed with a paced delay (see redeliveryDelay),
	// so MaxDeliver bounds how long a retryable command — e.g. one that
	// raced its state projection — stays retryable before the DLQ.
	AckWait    time.Duration
	MaxDeliver int
	BackOff    []time.Duration
}

// JetStream is the production Bus backed by NATS JetStream. Message.ID
// becomes Nats-Msg-Id, enabling server-side deduplication; consumers are
// durable pull consumers and exhausted deliveries surface as DLQ
// advisories.
type JetStream struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	log     *slog.Logger
	cfg     JetStreamConfig
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	closeMu sync.Mutex
	closed  bool
}

// ConnectJetStream connects to the broker, ensures every declared stream
// exists and starts the DLQ advisory listener.
func ConnectJetStream(ctx context.Context, cfg JetStreamConfig, log *slog.Logger) (*JetStream, error) {
	if len(cfg.Streams) == 0 {
		return nil, errors.New("eventbus: at least one stream must be declared")
	}
	if cfg.DedupWindow <= 0 {
		cfg.DedupWindow = 2 * time.Minute
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = 30 * time.Second
	}
	if cfg.MaxDeliver <= 0 {
		cfg.MaxDeliver = 10
	}

	nc, err := nats.Connect(cfg.URL,
		nats.Name("pulsarpass"),
		nats.Timeout(5*time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	for _, spec := range cfg.Streams {
		_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:       spec.Name,
			Subjects:   spec.Subjects,
			Duplicates: cfg.DedupWindow,
		})
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("eventbus: ensure stream %s: %w", spec.Name, err)
		}
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	j := &JetStream{nc: nc, js: js, log: log, cfg: cfg, cancel: cancel}
	j.wg.Add(1)
	go j.listenDLQ(loopCtx)
	return j, nil
}

// Publish sends msg using its ID as the deduplication key. The active
// span (if any) is stamped onto the headers as W3C trace context, so
// the consumer's span joins the same trace.
func (j *JetStream) Publish(ctx context.Context, msg Message) error {
	pctx, span := publishSpan(ctx, &msg)
	defer span.End()
	injectTraceContext(pctx, &msg)

	nm := &nats.Msg{Subject: msg.Subject, Data: msg.Payload}
	if len(msg.Headers) > 0 {
		nm.Header = make(nats.Header, len(msg.Headers))
		for k, v := range msg.Headers {
			nm.Header.Set(k, v)
		}
	}
	_, err := j.js.PublishMsg(pctx, nm, jetstream.WithMsgID(msg.ID))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
	}
	return err
}

// Subscribe ensures a durable pull consumer (named by queue) and starts
// its fetch loop. Sharing the queue name across instances forms a queue
// group: each message is handled by exactly one instance.
func (j *JetStream) Subscribe(subject, queue string, handler Handler) error {
	if queue == "" {
		return errors.New("eventbus: durable consumer name (queue) is required")
	}
	stream, err := j.streamFor(subject)
	if err != nil {
		return err
	}
	cons, err := j.js.CreateOrUpdateConsumer(context.Background(), stream, jetstream.ConsumerConfig{
		Durable:       queue,
		FilterSubject: subject,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       j.cfg.AckWait,
		MaxDeliver:    j.cfg.MaxDeliver,
		BackOff:       j.cfg.BackOff,
	})
	if err != nil {
		return fmt.Errorf("eventbus: ensure consumer %s: %w", queue, err)
	}
	j.wg.Add(1)
	go j.consumeLoop(cons, queue, handler)
	return nil
}

// Close stops the fetch loops and drains the connection.
func (j *JetStream) Close(_ context.Context) error {
	j.closeMu.Lock()
	if j.closed {
		j.closeMu.Unlock()
		return nil
	}
	j.closed = true
	j.closeMu.Unlock()

	j.cancel()
	j.wg.Wait()
	return j.nc.Drain()
}

func (j *JetStream) consumeLoop(cons jetstream.Consumer, name string, handler Handler) {
	defer j.wg.Done()
	for {
		if j.isClosed() {
			return
		}
		batch, err := cons.Fetch(16, jetstream.FetchMaxWait(time.Second))
		if err != nil && !errors.Is(err, jetstream.ErrNoMessages) {
			j.log.Error("jetstream fetch failed", "consumer", name, "error", err)
			time.Sleep(time.Second)
			continue
		}
		for msg := range batch.Messages() {
			j.handle(msg, name, handler)
		}
	}
}

func (j *JetStream) handle(msg jetstream.Msg, consumer string, handler Handler) {
	headers := make(map[string]string, len(msg.Headers()))
	for k := range msg.Headers() {
		headers[k] = msg.Headers().Get(k)
	}
	m := Message{
		ID:      msg.Headers().Get("Nats-Msg-Id"),
		Subject: msg.Subject(),
		Payload: msg.Data(),
		Headers: headers,
	}
	cctx, span := consumeSpan(context.Background(), &m)
	err := handler(cctx, m)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "handler failed")
		span.End()
		j.log.Warn("handler failed; requesting redelivery",
			"consumer", consumer,
			"subject", msg.Subject(),
			"error", err,
		)
		// A plain NAK requests an immediate redelivery, so a command that
		// keeps failing burns its whole delivery budget in milliseconds —
		// a payment racing its context projection never survives to see
		// the projection land. Pace the retries instead.
		delay := 2 * time.Second
		if md, mdErr := msg.Metadata(); mdErr == nil {
			delay = redeliveryDelay(md.NumDelivered)
		}
		_ = msg.NakWithDelay(delay)
		return
	}
	span.End()
	if err := msg.Ack(); err != nil {
		j.log.Debug("ack failed; broker will redeliver", "consumer", consumer, "error", err)
	}
}

// redeliveryDelay paces NAK-driven redeliveries by delivery attempt:
// quick first retries, then backing off. The budget is still bounded by
// MaxDeliver — pacing changes how the budget is spent, not its size.
func redeliveryDelay(numDelivered uint64) time.Duration {
	switch numDelivered {
	case 1:
		return 100 * time.Millisecond
	case 2:
		return 250 * time.Millisecond
	case 3:
		return 500 * time.Millisecond
	case 4:
		return time.Second
	default:
		return 2 * time.Second
	}
}

func (j *JetStream) listenDLQ(ctx context.Context) {
	defer j.wg.Done()
	sub, err := j.nc.Subscribe("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>", func(m *nats.Msg) {
		var adv struct {
			Stream   string `json:"stream"`
			Consumer string `json:"consumer"`
			Subject  string `json:"subject"`
		}
		_ = json.Unmarshal(m.Data, &adv)
		incDLQAdvisory(adv.Stream, adv.Consumer)
		j.log.Error("message exceeded max deliveries (DLQ advisory)",
			"stream", adv.Stream,
			"consumer", adv.Consumer,
			"subject", adv.Subject,
		)
	})
	if err != nil {
		j.log.Error("failed to subscribe DLQ advisories", "error", err)
		return
	}
	<-ctx.Done()
	_ = sub.Unsubscribe()
}

func (j *JetStream) streamFor(subject string) (string, error) {
	for _, spec := range j.cfg.Streams {
		for _, pattern := range spec.Subjects {
			prefix := strings.TrimSuffix(pattern, ">")
			if strings.HasPrefix(subject, prefix) {
				return spec.Name, nil
			}
		}
	}
	return "", fmt.Errorf("eventbus: no stream declared for subject %s", subject)
}

func (j *JetStream) isClosed() bool {
	j.closeMu.Lock()
	defer j.closeMu.Unlock()
	return j.closed
}
