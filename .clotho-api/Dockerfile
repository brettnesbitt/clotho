# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy module files (from clotho-api/ context)
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o clotho-api .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/clotho-api .

EXPOSE 3000
CMD ["./clotho-api"]

#cd clotho/.clotho-api && docker build -t us-central1-docker.pkg.dev/quotopia-391900/clotho/clotho-api:latest . 2>&1 | tail -5 && docker push us-central1-docker.pkg.dev/quotopia-391900/clotho/clotho-api:latest 2>&1 | tail -3 && kubectl set image deployment/clotho-api -n clotho-system api=us-central1-docker.pkg.dev/quotopia-391900/clotho/clotho-api:latest 2>&1 && kubectl rollout status deployment/clotho-api -n clotho-system --timeout=120s 2>&1