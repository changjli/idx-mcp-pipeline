# ─── builder ────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# embed.FS (ticker list, broker map) is compiled in from the source tree.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/mcp-server ./cmd/mcp-server

# ─── runtime ────────────────────────────────────────────────────
# No bundled Redis: SnapDeploy's service-dependency scanner blocks
# redis-server in the app container. Redis is external (Redis Cloud)
# via REDIS_* env vars.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -H app
WORKDIR /app
COPY --from=builder /out/mcp-server ./mcp-server
COPY config.json ./config.json
EXPOSE 8080
USER app
CMD ["./mcp-server"]