# AGENTS.md — convenções para agentes neste repositório

## Documentação de ciclos (obrigatório)

Ao **encerrar um ciclo de desenvolvimento** (assim que o ciclo estiver pronto — código, testes e docs consistentes), adicionar uma seção em `docs/CYCLES.md` seguindo o template ao final desse arquivo:

- Objetivo do ciclo (1–2 frases), faixa de commits (`<hash-inicial>` → `<hash-final>`).
- Entregas por área/serviço com referências ao código: `caminho/arquivo.go` + símbolos relevantes e decisões tomadas.
- Garantias implementadas (tabela "garantia → onde vive") quando aplicável.
- Testes novos/de integração e o que eles provam.

O documento é o retrato *state of the art* do projeto — a seção mais recente descreve o estado atual. Atualizar também a tabela de índice no topo e o status na seção 12 (Roadmap de Ciclos) de `docs/ARCHITECTURE.md`.

## Git conventions

Em inglês, Conventional Commits (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`); branches `feat/<scope>`, `fix/<scope>`, `chore/<scope>`, `docs/<scope>`; commits atômicos (um commit lógico, testes junto com o código que cobrem). Descrições de PR em português e detalhadas.

## Fluxo de PR (obrigatório)

A ordem é sempre: PR aberto → CI verde → **revisão humana**. Agentes devem aguardar a revisão e **nunca fazer merge sem pedido explícito do usuário** — a não ser que o usuário peça, o papel do agente é reportar o estado do CI e sinalizar que o PR aguarda revisão.

## Qualidade

- `make fmt vet lint test` antes de considerar trabalho pronto (testes de integração usam testcontainers/Docker).
- Docs em `docs/` são escritas em português (BR).
