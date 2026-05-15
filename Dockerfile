# ── Build stage ────────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /src
COPY go.mod ./
RUN go mod download 2>/dev/null || true

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/gossip-node ./cmd/node

# ── Runtime stage ───────────────────────────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

COPY --from=builder /bin/gossip-node /bin/gossip-node
COPY topology/topology.json /etc/gossip/topology.json

EXPOSE 8080

ENTRYPOINT ["/bin/gossip-node"]
