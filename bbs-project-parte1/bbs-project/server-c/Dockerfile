FROM ubuntu:22.04
RUN apt-get update && apt-get install -y \
    gcc make libzmq3-dev libsqlite3-dev wget tar

WORKDIR /app

# baixa a lib mpack (amalgamação em arquivo único)
RUN wget -q https://github.com/ludocode/mpack/releases/download/v1.1.1/mpack-amalgamation-1.1.1.tar.gz \
    && tar xzf mpack-amalgamation-1.1.1.tar.gz \
    && cp mpack-amalgamation-1.1.1/src/mpack/mpack.h . \
    && cp mpack-amalgamation-1.1.1/src/mpack/mpack.c . \
    && rm -rf mpack-amalgamation-1.1.1*

COPY server.c .
RUN gcc -O2 -o server server.c mpack.c -lzmq -lsqlite3 -lm

CMD ["./server"]
