FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go mod download all
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=mod -o /out/backend_server ./main.go

FROM alpine:3.20

WORKDIR /app

RUN adduser -D -H -u 10001 appuser
ENV UPLOADS_DIR=/app/uploads
RUN mkdir -p /app/uploads/main-products && chown -R appuser:appuser /app/uploads

COPY --from=builder /out/backend_server /app/backend_server

USER appuser

EXPOSE 8080

ENTRYPOINT ["/app/backend_server"]
