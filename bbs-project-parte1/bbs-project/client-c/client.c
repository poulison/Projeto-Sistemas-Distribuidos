#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <zmq.h>
#include "mpack.h"

#define MAX_BUF 65536

static const char *bot_name;
static void       *socket;

static const char *channels[] = {
    "geral", "random", "noticias", "projetos", "c-talk", NULL
};

//  helpers

static double now_ts(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

// send/recv 


static char *build_msg(const char *type, const char *username,
                        const char *channel_name, size_t *out_size)
{
    char *buf = NULL;
    mpack_writer_t writer;
    mpack_writer_init_growable(&writer, &buf, out_size);

    int fields = 3 + (channel_name && channel_name[0] ? 1 : 0);
    mpack_start_map(&writer, fields);

    mpack_write_cstr(&writer, "type");
    mpack_write_cstr(&writer, type);
    mpack_write_cstr(&writer, "username");
    mpack_write_cstr(&writer, username);
    mpack_write_cstr(&writer, "timestamp");
    mpack_write_double(&writer, now_ts());

    if (channel_name && channel_name[0]) {
        mpack_write_cstr(&writer, "channel_name");
        mpack_write_cstr(&writer, channel_name);
    }

    mpack_finish_map(&writer);
    mpack_writer_destroy(&writer);
    return buf;
}

static void send_recv(const char *type, const char *channel_name) {
    size_t  send_size = 0;
    char   *send_buf  = build_msg(type, bot_name, channel_name, &send_size);

    printf("[%s] SEND | type=%-10s | ts=%.3f\n", bot_name, type, now_ts());
    fflush(stdout);

    zmq_send(socket, send_buf, send_size, 0);
    free(send_buf);

    static char recv_buf[MAX_BUF];
    int nbytes = zmq_recv(socket, recv_buf, MAX_BUF - 1, 0);
    if (nbytes < 0) {
        fprintf(stderr, "[%s] Recv error\n", bot_name);
        return;
    }

    // parse response
    mpack_tree_t tree;
    mpack_tree_init_data(&tree, recv_buf, nbytes);
    mpack_tree_parse(&tree);
    mpack_node_t root = mpack_tree_root(&tree);

    char status[32], message[256];
    status[0] = message[0] = '\0';

    mpack_node_t sn = mpack_node_map_cstr(root, "status");
    if (mpack_node_type(sn) == mpack_type_str)
        mpack_node_copy_utf8_cstr(sn, status, sizeof(status));

    mpack_node_t mn = mpack_node_map_cstr(root, "message");
    if (mpack_node_type(mn) == mpack_type_str)
        mpack_node_copy_utf8_cstr(mn, message, sizeof(message));

    printf("[%s] RECV | status=%-8s | msg=%s\n", bot_name, status, message);

    
    if (strcmp(type, "list") == 0) {
        mpack_node_t dn = mpack_node_map_cstr(root, "data");
        if (mpack_node_type(dn) == mpack_type_array) {
            size_t len = mpack_node_array_length(dn);
            printf("[%s] Channels available: [", bot_name);
            for (size_t i = 0; i < len; i++) {
                mpack_node_t item = mpack_node_array_at(dn, i);
                char name[128] = {0};
                mpack_node_copy_utf8_cstr(item, name, sizeof(name));
                printf("%s%s", name, i + 1 < len ? ", " : "");
            }
            printf("]\n");
        }
    }

    fflush(stdout);
    mpack_tree_destroy(&tree);
}

// main 

int main(void) {
    bot_name = getenv("BOT_NAME");    if (!bot_name)    bot_name    = "bot-c-1";
    const char *host = getenv("SERVER_HOST"); if (!host) host = "server-c";
    const char *port = getenv("SERVER_PORT"); if (!port) port = "5553";

    sleep(3);

    void *ctx = zmq_ctx_new();
    socket    = zmq_socket(ctx, ZMQ_REQ);

    char addr[128];
    snprintf(addr, sizeof(addr), "tcp://%s:%s", host, port);
    zmq_connect(socket, addr);
    printf("[%s] Connected to %s\n", bot_name, addr);
    fflush(stdout);

    // login
    while (1) {
        send_recv("login", NULL);
        
        break;
    }
    sleep(1);

    send_recv("list", NULL);
    sleep(1);

    for (int i = 0; channels[i]; i++) {
        send_recv("channel", channels[i]);
        usleep(300000);
    }

    send_recv("list", NULL);

    printf("[%s] ✔ Part 1 done!\n", bot_name);
    fflush(stdout);

    zmq_close(socket);
    zmq_ctx_destroy(ctx);
    return 0;
}
