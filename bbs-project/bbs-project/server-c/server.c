#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <zmq.h>
#include <sqlite3.h>
#include "mpack.h"

#define DB_PATH  "/data/server.db"
#define MAX_BUF  65536

static sqlite3 *db;
static void    *pub_socket;

static double now_ts(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void get_str(mpack_node_t root, const char *key, char *buf, size_t size) {
    buf[0] = '\0';
    mpack_node_t node = mpack_node_map_cstr(root, key);
    if (mpack_node_type(node) == mpack_type_str)
        mpack_node_copy_utf8_cstr(node, buf, size);
}

static double get_double(mpack_node_t root, const char *key) {
    mpack_node_t node = mpack_node_map_cstr(root, key);
    if (mpack_node_type(node) != mpack_type_missing)
        return mpack_node_double(node);
    return now_ts();
}

static void init_db(void) {
    mkdir("/data", 0755);
    if (sqlite3_open(DB_PATH, &db) != SQLITE_OK) { fprintf(stderr, "Cannot open DB\n"); exit(1); }
    sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);"
        "CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);"
        "CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);"
        "CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL, timestamp REAL NOT NULL);",
        NULL, NULL, NULL);
}

static char *make_resp(const char *status, const char *message, char **data, int data_len, size_t *out_size) {
    char *buf = NULL;
    mpack_writer_t writer;
    mpack_writer_init_growable(&writer, &buf, out_size);
    int fields = (data != NULL) ? 4 : 3;
    mpack_start_map(&writer, fields);
    mpack_write_cstr(&writer, "status");  mpack_write_cstr(&writer, status);
    mpack_write_cstr(&writer, "message"); mpack_write_cstr(&writer, message);
    if (data != NULL) {
        mpack_write_cstr(&writer, "data");
        mpack_start_array(&writer, data_len);
        for (int i = 0; i < data_len; i++) mpack_write_cstr(&writer, data[i]);
        mpack_finish_array(&writer);
    }
    mpack_write_cstr(&writer, "timestamp"); mpack_write_double(&writer, now_ts());
    mpack_finish_map(&writer);
    mpack_writer_destroy(&writer);
    return buf;
}

static char *handle_login(const char *username, double ts, size_t *out_size) {
    if (!username[0]) return make_resp("error", "Username cannot be empty", NULL, 0, out_size);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO users (username, created_at) VALUES (?, ?)", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, username, -1, SQLITE_STATIC); sqlite3_bind_double(stmt, 2, ts);
    sqlite3_step(stmt); sqlite3_finalize(stmt);
    sqlite3_prepare_v2(db, "INSERT INTO logins (username, timestamp) VALUES (?, ?)", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, username, -1, SQLITE_STATIC); sqlite3_bind_double(stmt, 2, ts);
    sqlite3_step(stmt); sqlite3_finalize(stmt);
    char msg[256]; snprintf(msg, sizeof(msg), "Welcome, %s!", username);
    return make_resp("ok", msg, NULL, 0, out_size);
}

static char *handle_create_channel(const char *name, const char *created_by, double ts, size_t *out_size) {
    if (!name[0]) return make_resp("error", "Channel name cannot be empty", NULL, 0, out_size);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, "INSERT INTO channels (name, created_by, created_at) VALUES (?, ?, ?)", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC); sqlite3_bind_text(stmt, 2, created_by, -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 3, ts);
    int rc = sqlite3_step(stmt); sqlite3_finalize(stmt);
    if (rc == SQLITE_CONSTRAINT) {
        char msg[128]; snprintf(msg, sizeof(msg), "Channel '%s' already exists", name);
        return make_resp("error", msg, NULL, 0, out_size);
    }
    char msg[128]; snprintf(msg, sizeof(msg), "Channel '%s' created!", name);
    return make_resp("ok", msg, NULL, 0, out_size);
}

static char *handle_list_channels(size_t *out_size) {
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, "SELECT name FROM channels ORDER BY created_at", -1, &stmt, NULL);
    char *names[256]; int count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && count < 256)
        names[count++] = strdup((const char *)sqlite3_column_text(stmt, 0));
    sqlite3_finalize(stmt);
    char *resp = make_resp("ok", "OK", names, count, out_size);
    for (int i = 0; i < count; i++) free(names[i]);
    return resp;
}

static char *handle_publish(const char *channel, const char *username, const char *message, double ts, size_t *out_size) {
    if (!channel[0] || !message[0]) return make_resp("error", "Channel and message are required", NULL, 0, out_size);

    // verifica canal
    sqlite3_stmt *chk;
    sqlite3_prepare_v2(db, "SELECT name FROM channels WHERE name = ?", -1, &chk, NULL);
    sqlite3_bind_text(chk, 1, channel, -1, SQLITE_STATIC);
    int exists = (sqlite3_step(chk) == SQLITE_ROW);
    sqlite3_finalize(chk);
    if (!exists) {
        char msg[128]; snprintf(msg, sizeof(msg), "Channel '%s' does not exist", channel);
        return make_resp("error", msg, NULL, 0, out_size);
    }

    // persiste
    sqlite3_stmt *ins;
    sqlite3_prepare_v2(db, "INSERT INTO messages (channel,username,message,timestamp) VALUES (?,?,?,?)", -1, &ins, NULL);
    sqlite3_bind_text(ins, 1, channel, -1, SQLITE_STATIC); sqlite3_bind_text(ins, 2, username, -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 3, message, -1, SQLITE_STATIC); sqlite3_bind_double(ins, 4, ts);
    sqlite3_step(ins); sqlite3_finalize(ins);

    // publica via PUB socket (multipart: [topic][payload])
    char *pub_buf = NULL; size_t pub_size = 0;
    mpack_writer_t writer;
    mpack_writer_init_growable(&writer, &pub_buf, &pub_size);
    mpack_start_map(&writer, 5);
    mpack_write_cstr(&writer, "channel");   mpack_write_cstr(&writer, channel);
    mpack_write_cstr(&writer, "username");  mpack_write_cstr(&writer, username);
    mpack_write_cstr(&writer, "message");   mpack_write_cstr(&writer, message);
    mpack_write_cstr(&writer, "timestamp"); mpack_write_double(&writer, ts);
    mpack_write_cstr(&writer, "received");  mpack_write_double(&writer, now_ts());
    mpack_finish_map(&writer);
    mpack_writer_destroy(&writer);

    zmq_send(pub_socket, channel, strlen(channel), ZMQ_SNDMORE);
    zmq_send(pub_socket, pub_buf, pub_size, 0);
    free(pub_buf);

    printf("[SERVER-C] PUB  | channel=%-15s | from=%-15s | msg=%s\n", channel, username, message);
    fflush(stdout);
    return make_resp("ok", "Published!", NULL, 0, out_size);
}

int main(void) {
    const char *port_env  = getenv("PORT");       int port = port_env  ? atoi(port_env)  : 5553;
    const char *proxy_host = getenv("PROXY_HOST"); if (!proxy_host) proxy_host = "proxy";
    const char *xsub_port  = getenv("XSUB_PORT"); if (!xsub_port)  xsub_port  = "5557";

    init_db();

    void *ctx = zmq_ctx_new();

    void *rep_socket = zmq_socket(ctx, ZMQ_REP);
    char addr[64]; snprintf(addr, sizeof(addr), "tcp://*:%d", port);
    if (zmq_bind(rep_socket, addr) != 0) { fprintf(stderr, "Bind error\n"); return 1; }

    pub_socket = zmq_socket(ctx, ZMQ_PUB);
    char pub_addr[128]; snprintf(pub_addr, sizeof(pub_addr), "tcp://%s:%s", proxy_host, xsub_port);
    zmq_connect(pub_socket, pub_addr);
    struct timespec ts = {0, 500000000}; nanosleep(&ts, NULL);

    printf("[SERVER-C] Listening on port %d\n", port);
    printf("[SERVER-C] Publishing to proxy %s\n", pub_addr);
    fflush(stdout);

    static char recv_buf[MAX_BUF];
    while (1) {
        int nbytes = zmq_recv(rep_socket, recv_buf, MAX_BUF-1, 0);
        if (nbytes < 0) continue;

        mpack_tree_t tree;
        mpack_tree_init_data(&tree, recv_buf, nbytes);
        mpack_tree_parse(&tree);
        mpack_node_t root = mpack_tree_root(&tree);

        char type[64], username[256], channel_name[256], message[1024];
        get_str(root, "type",         type,         sizeof(type));
        get_str(root, "username",     username,     sizeof(username));
        get_str(root, "channel_name", channel_name, sizeof(channel_name));
        get_str(root, "message",      message,      sizeof(message));
        double recv_ts = get_double(root, "timestamp");
        mpack_tree_destroy(&tree);

        printf("[SERVER-C] RECV | type=%-10s | from=%-15s | ts=%.3f\n", type, username, recv_ts);
        fflush(stdout);

        size_t resp_size = 0; char *resp_buf = NULL;
        if      (strcmp(type, "login")   == 0) resp_buf = handle_login(username, recv_ts, &resp_size);
        else if (strcmp(type, "channel") == 0) resp_buf = handle_create_channel(channel_name, username, recv_ts, &resp_size);
        else if (strcmp(type, "list")    == 0) resp_buf = handle_list_channels(&resp_size);
        else if (strcmp(type, "publish") == 0) resp_buf = handle_publish(channel_name, username, message, recv_ts, &resp_size);
        else { char em[128]; snprintf(em, sizeof(em), "Unknown type: %s", type); resp_buf = make_resp("error", em, NULL, 0, &resp_size); }

        zmq_send(rep_socket, resp_buf, resp_size, 0);
        free(resp_buf);
    }
    return 0;
}
