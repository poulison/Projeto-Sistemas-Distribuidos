FROM golang:1.23-alpine
WORKDIR /app
COPY client.go .
RUN go mod init bbs/client && \
    go get github.com/go-zeromq/zmq4@v0.17.0 && \
    go get github.com/vmihailenco/msgpack/v5@latest && \
    go mod tidy && \
    go build -o client .
CMD ["./client"]