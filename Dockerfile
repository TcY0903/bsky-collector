# ==================== Build stage ====================
FROM golang:1.26-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build a static binary
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /build/bsky-collector .


# ==================== Runtime stage ====================
FROM alpine:3.20

# ca-certificates for HTTPS connections to Bluesky
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy the binary
COPY --from=builder /build/bsky-collector /app/bsky-collector

# Copy the labeler list
COPY labelers.csv /app/labelers.csv

# Run as non-root
RUN adduser -D -u 1000 collector && chown -R collector:collector /app
USER collector

CMD ["/app/bsky-collector"]