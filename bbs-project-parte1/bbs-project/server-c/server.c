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

// ── helpers ──────────────────────────────────────────────────────────────────

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
    if (mpack_node_type(node) == mpack_type_float  ||
        mpack_node_type(node) == mpack_type_double ||
        mpack_node_type(node) == mpack_type_int    ||
        mpack_node_type(node) == mpack_type_uint)
        return mpack_node_double(node);
    return now_ts();
}

// ── banco de dados ────────────────────────────────────────────────────────────

static void init_db(void) {
    mkdir("/data", 0755);
    if (sqlite3_open(DB_PATH, &db) != SQLITE_OK) {
        fprintf(stderr, "[SERVER-C] Cannot open DB: %s\n", sqlite3_errmsg(db));
        exit(1);
    }
    sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS users "
        "(username TEXT PRIMARY KEY, created_at REAL NOT NULL);"
        "CREATE TABLE IF NOT EXISTS logins "
        "(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);"
        "CREATE TABLE IF NOT EXISTS channels "
        "(name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);",
        NULL, NULL, NULL);
}

// ── construção de resposta msgpack ────────────────────────────────────────────

static char *make_resp(const char *status, const char *message,
                       char **data, int data_len, size_t *out_size)
{
    char *buf = NULL;
    mpack_writer_t writer;
    mpack_writer_init_growable(&writer, &buf, out_size);

    int fields = (data != NULL) ? 4 : 3;
    mpack_start_map(&writer, fields);

    mpack_write_cstr(&writer, "status");
    mpack_write_cstr(&writer, status);

    mpack_write_cstr(&writer, "message");
    mpack_write_cstr(&writer, message);

    if (data != NULL) {
        mpack_write_cstr(&writer, "data");
        mpack_start_array(&writer, data_len);
        for (int i = 0; i < data_len; i++)
            mpack_write_cstr(&writer, data[i]);
        mpack_finish_array(&writer);
    }

    mpack_write_cstr(&writer, "timestamp");
    mpack_write_double(&writer, now_ts());

    mpack_finish_map(&writer);

    if (mpack_writer_destroy(&writer) != mpack_ok) {
        free(buf);
        return NULL;
    }
    return buf;
}

// ── handlers ─────────────────────────────────────────────────────────────────

static char *handle_login(const char *username, double ts, size_t *out_size) {
    if (username[0] == '\0')
        return make_resp("error", "Username cannot be empty", NULL, 0, out_size);

    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO users (username, created_at) VALUES (?, ?)", -1, &stmt, NULL);
    sqlite3_bind_text(stmt,   1, username, -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 2, ts);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    sqlite3_prepare_v2(db,
        "INSERT INTO logins (username, timestamp) VALUES (?, ?)", -1, &stmt, NULL);
    sqlite3_bind_text(stmt,   1, username, -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 2, ts);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    char msg[256];
    snprintf(msg, sizeof(msg), "Welcome, %s!", username);
    return make_resp("ok", msg, NULL, 0, out_size);
}

static char *handle_create_channel(const char *name, const char *created_by,
                                    double ts, size_t *out_size) {
    if (name[0] == '\0')
        return make_resp("error", "Channel name cannot be empty", NULL, 0, out_size);
    if (strlen(name) > 32)
        return make_resp("error", "Channel name too long (max 32 chars)", NULL, 0, out_size);

    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db,
        "INSERT INTO channels (name, created_by, created_at) VALUES (?, ?, ?)",
        -1, &stmt, NULL);
    sqlite3_bind_text(stmt,   1, name,       -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt,   2, created_by, -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 3, ts);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc == SQLITE_CONSTRAINT) {
        char msg[128];
        snprintf(msg, sizeof(msg), "Channel '%s' already exists", name);
        return make_resp("error", msg, NULL, 0, out_size);
    }
    char msg[128];
    snprintf(msg, sizeof(msg), "Channel '%s' created!", name);
    return make_resp("ok", msg, NULL, 0, out_size);
}

static char *handle_list_channels(size_t *out_size) {
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db,
        "SELECT name FROM channels ORDER BY created_at", -1, &stmt, NULL);

    char *names[256];
    int   count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && count < 256) {
        names[count] = strdup((const char *)sqlite3_column_text(stmt, 0));
        count++;
    }
    sqlite3_finalize(stmt);

    char *resp = make_resp("ok", "OK", names, count, out_size);

    for (int i = 0; i < count; i++) free(names[i]);
    return resp;
}

// ── main ─────────────────────────────────────────────────────────────────────

int main(void) {
    const char *port_env = getenv("PORT");
    int port = port_env ? atoi(port_env) : 5553;

    init_db();

    void *ctx    = zmq_ctx_new();
    void *socket = zmq_socket(ctx, ZMQ_REP);

    char addr[64];
    snprintf(addr, sizeof(addr), "tcp://*:%d", port);
    if (zmq_bind(socket, addr) != 0) {
        fprintf(stderr, "[SERVER-C] Bind error: %s\n", zmq_strerror(zmq_errno()));
        return 1;
    }
    printf("[SERVER-C] Listening on port %d\n", port);
    fflush(stdout);

    static char recv_buf[MAX_BUF];

    while (1) {
        int nbytes = zmq_recv(socket, recv_buf, MAX_BUF - 1, 0);
        if (nbytes < 0) continue;

        // parse msgpack
        mpack_tree_t tree;
        mpack_tree_init_data(&tree, recv_buf, nbytes);
        mpack_tree_parse(&tree);
        mpack_node_t root = mpack_tree_root(&tree);

        char type[64], username[256], channel_name[256];
        get_str(root, "type",         type,         sizeof(type));
        get_str(root, "username",     username,     sizeof(username));
        get_str(root, "channel_name", channel_name, sizeof(channel_name));
        double ts = get_double(root, "timestamp");

        mpack_tree_destroy(&tree);

        printf("[SERVER-C] RECV | type=%-10s | from=%-15s | ts=%.3f\n",
               type, username, ts);
        fflush(stdout);

        size_t resp_size = 0;
        char  *resp_buf  = NULL;

        if      (strcmp(type, "login")   == 0) resp_buf = handle_login(username, ts, &resp_size);
        else if (strcmp(type, "channel") == 0) resp_buf = handle_create_channel(channel_name, username, ts, &resp_size);
        else if (strcmp(type, "list")    == 0) resp_buf = handle_list_channels(&resp_size);
        else {
            char err_msg[128];
            snprintf(err_msg, sizeof(err_msg), "Unknown type: %s", type);
            resp_buf = make_resp("error", err_msg, NULL, 0, &resp_size);
        }

        printf("[SERVER-C] SEND | %zu bytes\n", resp_size);
        fflush(stdout);

        zmq_send(socket, resp_buf, resp_size, 0);
        free(resp_buf);
    }

    zmq_close(socket);
    zmq_ctx_destroy(ctx);
    sqlite3_close(db);
    return 0;
}
