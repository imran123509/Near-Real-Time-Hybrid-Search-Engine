FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
ARG SERVICE=api
RUN CGO_ENABLED=0 go build -o /out/app ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
