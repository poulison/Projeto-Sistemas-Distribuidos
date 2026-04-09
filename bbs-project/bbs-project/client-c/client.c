#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <zmq.h>
#include "mpack.h"

#define MAX_BUF 65536

static const char *bot_name;
static const char *proxy_host;
static const char *xpub_port;
static void       *req_socket;
static char        sub_channels[8][64];
static int         sub_count = 0;

static const char *words[] = {
    "ola","mundo","sistema","distribuido","mensagem","canal",
    "teste","clinguagem","zmq","pubsub","broker","topico","servidor","rede", NULL
};

static double now_ts(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static char *random_msg(char *buf, size_t size) {
    int n = 3 + rand() % 5; buf[0] = '\0';
    int wcount = 0; while (words[wcount]) wcount++;
    for (int i = 0; i < n; i++) {
        if (i > 0) strncat(buf, " ", size - strlen(buf) - 1);
        strncat(buf, words[rand() % wcount], size - strlen(buf) - 1);
    }
    return buf;
}

static char *build_msg(const char *type, const char *username, const char *channel, const char *message, size_t *out_size) {
    char *buf = NULL;
    mpack_writer_t writer;
    mpack_writer_init_growable(&writer, &buf, out_size);
    int fields = 3;
    if (channel && channel[0]) fields++;
    if (message && message[0]) fields++;
    mpack_start_map(&writer, fields);
    mpack_write_cstr(&writer, "type");     mpack_write_cstr(&writer, type);
    mpack_write_cstr(&writer, "username"); mpack_write_cstr(&writer, username);
    mpack_write_cstr(&writer, "timestamp"); mpack_write_double(&writer, now_ts());
    if (channel && channel[0]) { mpack_write_cstr(&writer, "channel_name"); mpack_write_cstr(&writer, channel); }
    if (message && message[0]) { mpack_write_cstr(&writer, "message"); mpack_write_cstr(&writer, message); }
    mpack_finish_map(&writer);
    mpack_writer_destroy(&writer);
    return buf;
}

// envia e recebe, retorna lista de canais (se list) ou NULL
static int send_recv(const char *type, const char *channel, const char *message,
                     char out_channels[][64], int *out_count) {
    size_t send_size = 0;
    char *send_buf = build_msg(type, bot_name, channel, message, &send_size);
    printf("[%s] SEND | type=%-10s | ts=%.3f\n", bot_name, type, now_ts()); fflush(stdout);
    zmq_send(req_socket, send_buf, send_size, 0);
    free(send_buf);

    static char recv_buf[MAX_BUF];
    int nbytes = zmq_recv(req_socket, recv_buf, MAX_BUF-1, 0);
    if (nbytes < 0) return 0;

    mpack_tree_t tree;
    mpack_tree_init_data(&tree, recv_buf, nbytes);
    mpack_tree_parse(&tree);
    mpack_node_t root = mpack_tree_root(&tree);

    char status[32]="", msg[256]="";
    mpack_node_t sn = mpack_node_map_cstr(root, "status");
    if (mpack_node_type(sn)==mpack_type_str) mpack_node_copy_utf8_cstr(sn, status, sizeof(status));
    mpack_node_t mn = mpack_node_map_cstr(root, "message");
    if (mpack_node_type(mn)==mpack_type_str) mpack_node_copy_utf8_cstr(mn, msg, sizeof(msg));
    printf("[%s] RECV | status=%-8s | msg=%s\n", bot_name, status, msg); fflush(stdout);

    if (out_channels && out_count) {
        *out_count = 0;
        mpack_node_t dn = mpack_node_map_cstr(root, "data");
        if (mpack_node_type(dn)==mpack_type_array) {
            size_t len = mpack_node_array_length(dn);
            printf("[%s] Channels available: [", bot_name);
            for (size_t i = 0; i < len && *out_count < 32; i++) {
                mpack_node_t item = mpack_node_array_at(dn, i);
                mpack_node_copy_utf8_cstr(item, out_channels[*out_count], 64);
                printf("%s%s", out_channels[*out_count], i+1<len?", ":"");
                (*out_count)++;
            }
            printf("]\n"); fflush(stdout);
        }
    }
    mpack_tree_destroy(&tree);
    return strcmp(status, "ok") == 0 ? 1 : 0;
}

// thread SUB
static void *subscriber_thread_fn(void *arg) {
    void *ctx = zmq_ctx_new();
    void *sub = zmq_socket(ctx, ZMQ_SUB);
    char addr[128]; snprintf(addr, sizeof(addr), "tcp://%s:%s", proxy_host, xpub_port);
    zmq_connect(sub, addr);
    usleep(500000);

    for (int i = 0; i < sub_count; i++) {
        zmq_setsockopt(sub, ZMQ_SUBSCRIBE, sub_channels[i], strlen(sub_channels[i]));
        printf("[%s] SUB  | subscribed to '%s'\n", bot_name, sub_channels[i]); fflush(stdout);
    }

    static char topic_buf[256], data_buf[MAX_BUF];
    while (1) {
        int t = zmq_recv(sub, topic_buf, sizeof(topic_buf)-1, 0); if (t < 0) continue;
        topic_buf[t] = '\0';
        int d = zmq_recv(sub, data_buf, MAX_BUF-1, 0); if (d < 0) continue;

        mpack_tree_t tree;
        mpack_tree_init_data(&tree, data_buf, d);
        mpack_tree_parse(&tree);
        mpack_node_t root = mpack_tree_root(&tree);
        char ch[64]="", user[64]="", msg[512]="";
        mpack_node_t cn = mpack_node_map_cstr(root, "channel");
        if (mpack_node_type(cn)==mpack_type_str) mpack_node_copy_utf8_cstr(cn, ch, sizeof(ch));
        mpack_node_t un = mpack_node_map_cstr(root, "username");
        if (mpack_node_type(un)==mpack_type_str) mpack_node_copy_utf8_cstr(un, user, sizeof(user));
        mpack_node_t mn = mpack_node_map_cstr(root, "message");
        if (mpack_node_type(mn)==mpack_type_str) mpack_node_copy_utf8_cstr(mn, msg, sizeof(msg));
        double ts_send = mpack_node_double(mpack_node_map_cstr(root, "timestamp"));
        mpack_tree_destroy(&tree);

        printf("[%s] MSG  | channel=%-12s | from=%-15s | sent=%.3f | recv=%.3f | %s\n",
               bot_name, ch, user, ts_send, now_ts(), msg); fflush(stdout);
    }
    return NULL;
}

int main(void) {
    srand((unsigned)time(NULL) ^ (unsigned)getpid());
    bot_name   = getenv("BOT_NAME");    if (!bot_name)   bot_name   = "bot-c-1";
    const char *host = getenv("SERVER_HOST"); if (!host) host = "server-c";
    const char *port = getenv("SERVER_PORT"); if (!port) port = "5553";
    proxy_host = getenv("PROXY_HOST"); if (!proxy_host) proxy_host = "proxy";
    xpub_port  = getenv("XPUB_PORT");  if (!xpub_port)  xpub_port  = "5558";

    sleep(3);
    void *ctx  = zmq_ctx_new();
    req_socket = zmq_socket(ctx, ZMQ_REQ);
    char addr[128]; snprintf(addr, sizeof(addr), "tcp://%s:%s", host, port);
    zmq_connect(req_socket, addr);
    printf("[%s] Connected to %s\n", bot_name, addr); fflush(stdout);

    // login
    while (!send_recv("login", NULL, NULL, NULL, NULL)) sleep(2);
    printf("[%s] Login successful!\n", bot_name); fflush(stdout);

    // lista canais
    static char channels[32][64]; int ch_count = 0;
    send_recv("list", NULL, NULL, channels, &ch_count);

    // cria canal se < 5
    if (ch_count < 5) {
        char new_ch[64]; snprintf(new_ch, sizeof(new_ch), "ch-c-%d", 100 + rand()%900);
        send_recv("channel", new_ch, NULL, NULL, NULL);
        send_recv("list", NULL, NULL, channels, &ch_count);
    }

    // inscreve em até 3 canais
    sub_count = ch_count < 3 ? ch_count : 3;
    for (int i = 0; i < sub_count; i++)
        strncpy(sub_channels[i], channels[i], 64);

    pthread_t tid;
    pthread_create(&tid, NULL, subscriber_thread_fn, NULL);
    sleep(2);

    // loop infinito
    printf("[%s] Starting publish loop\n", bot_name); fflush(stdout);
    char msg_buf[256];
    while (1) {
        int idx = rand() % ch_count;
        for (int i = 0; i < 10; i++) {
            send_recv("publish", channels[idx], random_msg(msg_buf, sizeof(msg_buf)), NULL, NULL);
            sleep(1);
        }
        send_recv("list", NULL, NULL, channels, &ch_count);
    }
    return 0;
}
