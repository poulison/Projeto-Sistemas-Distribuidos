FROM ubuntu:22.04
RUN apt-get update && apt-get install -y \
    lua5.3 liblua5.3-dev luarocks \
    libzmq3-dev gcc make

RUN luarocks install lzmq
RUN luarocks install lua-messagepack
RUN luarocks install luasocket

WORKDIR /app
COPY client.lua .
CMD ["lua5.3", "client.lua"]
