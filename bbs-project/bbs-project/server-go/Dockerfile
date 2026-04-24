FROM golang:1.23-alpine
RUN apk add --no-cache gcc musl-dev
WORKDIR /app
COPY server.go .
RUN go mod init bbs/server && \
    go get github.com/go-zeromq/zmq4@v0.17.0 && \
    go get github.com/vmihailenco/msgpack/v5@v5.3.5 && \
    go get modernc.org/sqlite@v1.33.1 && \
    go mod tidy && \
    go build -o server .
CMD ["./server"]