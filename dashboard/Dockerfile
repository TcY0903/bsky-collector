# Build stage
FROM golang:1.26.5-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" \
    -o /out/dashboard ./dashboard

# Runtime stage
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=build /out/dashboard ./dashboard-bin
COPY dashboard/templates ./dashboard/templates

EXPOSE 8080

CMD ["./dashboard-bin"]