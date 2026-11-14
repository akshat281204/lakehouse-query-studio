# syntax=docker/dockerfile:1

# The query engine is single-node DuckDB, linked in through cgo. DuckDB's
# bundled static libraries are built against glibc, so both stages are Debian
# based (not Alpine/musl) and the build needs a C/C++ toolchain, which the
# golang image provides.
FROM golang:1.25-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api && \
    CGO_ENABLED=1 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM debian:bookworm-slim AS runtime
# libstdc++ is the only shared dependency of the DuckDB static library.
RUN apt-get update && \
    apt-get install -y --no-install-recommends libstdc++6 ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --system app && useradd --system --gid app --home-dir /app --shell /usr/sbin/nologin app
COPY --from=build /out/api /app/api
COPY --from=build /out/worker /app/worker
USER app
WORKDIR /app
EXPOSE 8080
