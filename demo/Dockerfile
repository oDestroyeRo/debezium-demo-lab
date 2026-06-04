FROM golang:1.26.4-alpine3.23 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/orders-api ./cmd/orders-api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/fulfillment-consumer ./cmd/fulfillment-consumer

FROM alpine:3.23.4

RUN addgroup -S app && adduser -S -G app app

USER app

COPY --from=build /out/orders-api /usr/local/bin/orders-api
COPY --from=build /out/fulfillment-consumer /usr/local/bin/fulfillment-consumer

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/orders-api"]
