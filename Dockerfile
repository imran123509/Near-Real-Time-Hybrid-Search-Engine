# syntax=docker/dockerfile:1

# One build stage compiles both services; each runtime target copies in only
# its own binary. Build one with:
#
#   docker build --target api .
#   docker build --target consumer .
#
# docker compose picks the target for each service.

FROM golang:1.27 AS build
WORKDIR /src

# Download modules in their own layer, so a code change does not re-download.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
# Static binaries: the runtime image has no C library. -trimpath and -s -w
# leave out local paths and debug symbols.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/api ./cmd/consumer

# distroless/static holds CA certificates and time zone data, which the
# embedding provider's HTTPS calls need, and no shell or package manager.
# :nonroot runs as an unprivileged user.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

FROM runtime AS api
COPY --from=build /out/api /app
EXPOSE 8080
ENTRYPOINT ["/app"]

FROM runtime AS consumer
COPY --from=build /out/consumer /app
ENTRYPOINT ["/app"]
