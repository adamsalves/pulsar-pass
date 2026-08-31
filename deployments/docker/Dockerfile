ARG GO_VERSION=1.25

FROM golang:${GO_VERSION}-alpine AS build
ARG SVC
# Release metadata injected by the release workflow; "dev" keeps local
# `docker build` runs honest about being unreleased binaries.
ARG VERSION=dev
WORKDIR /src
COPY go.mod ./
COPY cmd/ cmd/
COPY internal/ internal/
COPY pkg/ pkg/
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w -X github.com/adamsalves/pulsar-pass/pkg/version.Version=${VERSION}" \
    -o /out/app ./cmd/${SVC}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
