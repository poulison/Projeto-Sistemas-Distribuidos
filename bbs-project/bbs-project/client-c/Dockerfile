FROM ubuntu:22.04
RUN apt-get update && apt-get install -y \
    gcc make libzmq3-dev wget tar

WORKDIR /app

RUN wget -q https://github.com/ludocode/mpack/releases/download/v1.1.1/mpack-amalgamation-1.1.1.tar.gz \
    && tar xzf mpack-amalgamation-1.1.1.tar.gz \
    && cp mpack-amalgamation-1.1.1/src/mpack/mpack.h . \
    && cp mpack-amalgamation-1.1.1/src/mpack/mpack.c . \
    && rm -rf mpack-amalgamation-1.1.1*

COPY client.c .
RUN gcc -O2 -o client client.c mpack.c -lzmq -lm -lpthread

CMD ["./client"]
