FROM golang:1.26.4-alpine3.23 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/orders-service ./cmd/orders-service
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/fulfillment-service ./cmd/fulfillment-service

FROM alpine:3.23.4

RUN addgroup -S app && adduser -S -G app app

USER app

COPY --from=build /out/orders-service /usr/local/bin/orders-service
COPY --from=build /out/fulfillment-service /usr/local/bin/fulfillment-service

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/orders-service"]
