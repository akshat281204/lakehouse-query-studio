# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM alpine:3.20 AS runtime
RUN addgroup -S app && adduser -S app -G app
COPY --from=build /out/api /app/api
COPY --from=build /out/worker /app/worker
USER app
WORKDIR /app
EXPOSE 8080
