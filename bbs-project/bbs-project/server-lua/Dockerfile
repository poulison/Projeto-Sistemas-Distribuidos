FROM ubuntu:22.04
RUN apt-get update && apt-get install -y \
    lua5.3 liblua5.3-dev luarocks \
    libzmq3-dev libsqlite3-dev gcc make

# instala as bibliotecas Lua necessárias
RUN luarocks install lzmq
RUN luarocks install lua-messagepack
RUN luarocks install lsqlite3
RUN luarocks install luasocket

WORKDIR /app
COPY server.lua .
CMD ["lua5.3", "server.lua"]
