FROM golang:1.23.12

ENV GOPROXY=off GOSUMDB=off GOFLAGS=-mod=vendor

WORKDIR /app

COPY go.mod go.sum ./
COPY vendor ./vendor
COPY . .

RUN go build -mod=vendor -o /sessionstore ./cmd/sessionstore

EXPOSE 8080

CMD ["/sessionstore"]
