#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <pthread.h>
#include <unistd.h>
#include <zmq.h>
#include <sqlite3.h>
#include "mpack.h"

#define DB_PATH  "/data/server.db"
#define MAX_BUF  65536

static sqlite3 *db;
static void    *pub_socket;
static void    *zmq_ctx;

/* relógio lógico */
static long long logical_clock = 0;
static pthread_mutex_t clock_mu = PTHREAD_MUTEX_INITIALIZER;

static long long tick_send(void) {
    pthread_mutex_lock(&clock_mu);
    long long v = ++logical_clock;
    pthread_mutex_unlock(&clock_mu);
    return v;
}
static void tick_recv(long long r) {
    pthread_mutex_lock(&clock_mu);
    if (r > logical_clock) logical_clock = r;
    pthread_mutex_unlock(&clock_mu);
}

/* relógio físico */
static double time_offset = 0.0;
static int server_rank = 0;
static char server_name[64] = "server-c";
static char ref_host[64]    = "reference";
static char ref_port[16]    = "5559";

static double now_ts(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec/1e9 + time_offset;
}

static void get_str(mpack_node_t root, const char *key, char *buf, size_t size) {
    buf[0] = '\0';
    mpack_node_t node = mpack_node_map_cstr(root, key);
    if (mpack_node_type(node) == mpack_type_str)
        mpack_node_copy_utf8_cstr(node, buf, size);
}
static double get_double(mpack_node_t root, const char *key) {
    mpack_node_t n = mpack_node_map_cstr(root, key);
    if (mpack_node_type(n) != mpack_type_missing) return mpack_node_double(n);
    return now_ts();
}
static long long get_int(mpack_node_t root, const char *key) {
    mpack_node_t n = mpack_node_map_cstr(root, key);
    if (mpack_node_type(n) != mpack_type_missing) return (long long)mpack_node_i64(n);
    return 0;
}

static void init_db(void) {
    mkdir("/data", 0755);
    if (sqlite3_open(DB_PATH, &db) != SQLITE_OK) { fprintf(stderr,"Cannot open DB\n"); exit(1); }
    sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);"
        "CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);"
        "CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);"
        "CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, username TEXT NOT NULL, message TEXT NOT NULL, timestamp REAL NOT NULL, clock INTEGER NOT NULL DEFAULT 0);",
        NULL, NULL, NULL);
}

/* chama o serviço de referência */
static void call_reference(const char *type_str, long long msg_count) {
    void *sock = zmq_socket(zmq_ctx, ZMQ_REQ);
    int timeout = 5000;
    zmq_setsockopt(sock, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    char addr[128]; snprintf(addr, sizeof(addr), "tcp://%s:%s", ref_host, ref_port);
    zmq_connect(sock, addr);

    char *buf = NULL; size_t sz = 0;
    mpack_writer_t w;
    mpack_writer_init_growable(&w, &buf, &sz);
    int fields = (msg_count >= 0) ? 5 : 4;
    mpack_start_map(&w, fields);
    mpack_write_cstr(&w, "type");      mpack_write_cstr(&w, type_str);
    mpack_write_cstr(&w, "name");      mpack_write_cstr(&w, server_name);
    mpack_write_cstr(&w, "clock");     mpack_write_i64(&w, tick_send());
    mpack_write_cstr(&w, "timestamp"); mpack_write_double(&w, now_ts());
    if (msg_count >= 0) { mpack_write_cstr(&w, "msg_count"); mpack_write_i64(&w, msg_count); }
    mpack_finish_map(&w); mpack_writer_destroy(&w);

    zmq_send(sock, buf, sz, 0);
    free(buf);

    static char resp_buf[MAX_BUF];
    int n = zmq_recv(sock, resp_buf, MAX_BUF-1, 0);
    if (n > 0) {
        mpack_tree_t tree;
        mpack_tree_init_data(&tree, resp_buf, n);
        mpack_tree_parse(&tree);
        mpack_node_t root = mpack_tree_root(&tree);
        long long rclk = get_int(root, "clock");
        tick_recv(rclk);
        int rank_val = (int)get_int(root, "rank");
        if (rank_val > 0) server_rank = rank_val;
        double ref_time = get_double(root, "time");
        if (ref_time > 1e9) {
            struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
            time_offset = ref_time - ((double)ts.tv_sec + (double)ts.tv_nsec/1e9);
        }
        double ref_ts = get_double(root, "timestamp");
        if (ref_ts > 1e9 && strcmp(type_str,"register")==0) {
            struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
            time_offset = ref_ts - ((double)ts.tv_sec + (double)ts.tv_nsec/1e9);
            printf("[%s] Registered | rank=%d | offset=%.3fs\n", server_name, server_rank, time_offset);
        }
        if (strcmp(type_str,"heartbeat")==0)
            printf("[%s] HEARTBEAT sent | rank=%d | clock=%lld | offset=%.3fs\n", server_name, server_rank, logical_clock, time_offset);
        mpack_tree_destroy(&tree);
    }
    zmq_close(sock);
}

static char *make_resp(const char *status, const char *message, char **data, int data_len,
                        long long clk, size_t *out_size) {
    char *buf = NULL;
    mpack_writer_t writer;
    mpack_writer_init_growable(&writer, &buf, out_size);
    int fields = (data != NULL) ? 5 : 4;
    mpack_start_map(&writer, fields);
    mpack_write_cstr(&writer, "status");    mpack_write_cstr(&writer, status);
    mpack_write_cstr(&writer, "message");   mpack_write_cstr(&writer, message);
    mpack_write_cstr(&writer, "clock");     mpack_write_i64(&writer, clk);
    mpack_write_cstr(&writer, "timestamp"); mpack_write_double(&writer, now_ts());
    if (data != NULL) {
        mpack_write_cstr(&writer, "data");
        mpack_start_array(&writer, data_len);
        for (int i=0; i<data_len; i++) mpack_write_cstr(&writer, data[i]);
        mpack_finish_array(&writer);
    }
    mpack_finish_map(&writer);
    mpack_writer_destroy(&writer);
    return buf;
}

static char *handle_login(const char *username, size_t *out) {
    if (!username[0]) return make_resp("error","Username cannot be empty",NULL,0,tick_send(),out);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO users (username,created_at) VALUES (?,?)",-1,&s,NULL);
    sqlite3_bind_text(s,1,username,-1,SQLITE_STATIC); sqlite3_bind_double(s,2,now_ts());
    sqlite3_step(s); sqlite3_finalize(s);
    sqlite3_prepare_v2(db,"INSERT INTO logins (username,timestamp) VALUES (?,?)",-1,&s,NULL);
    sqlite3_bind_text(s,1,username,-1,SQLITE_STATIC); sqlite3_bind_double(s,2,now_ts());
    sqlite3_step(s); sqlite3_finalize(s);
    char msg[256]; snprintf(msg,sizeof(msg),"Welcome, %s!",username);
    return make_resp("ok",msg,NULL,0,tick_send(),out);
}

static char *handle_create_channel(const char *name, const char *by, size_t *out) {
    if (!name[0]) return make_resp("error","Channel name cannot be empty",NULL,0,tick_send(),out);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,"INSERT INTO channels (name,created_by,created_at) VALUES (?,?,?)",-1,&s,NULL);
    sqlite3_bind_text(s,1,name,-1,SQLITE_STATIC); sqlite3_bind_text(s,2,by,-1,SQLITE_STATIC);
    sqlite3_bind_double(s,3,now_ts());
    int rc = sqlite3_step(s); sqlite3_finalize(s);
    if (rc==SQLITE_CONSTRAINT) {
        char msg[128]; snprintf(msg,sizeof(msg),"Channel '%s' already exists",name);
        return make_resp("error",msg,NULL,0,tick_send(),out);
    }
    char msg[128]; snprintf(msg,sizeof(msg),"Channel '%s' created!",name);
    return make_resp("ok",msg,NULL,0,tick_send(),out);
}

static char *handle_list(size_t *out) {
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,"SELECT name FROM channels ORDER BY created_at",-1,&s,NULL);
    char *names[256]; int count=0;
    while(sqlite3_step(s)==SQLITE_ROW && count<256)
        names[count++] = strdup((const char*)sqlite3_column_text(s,0));
    sqlite3_finalize(s);
    char *resp = make_resp("ok","OK",names,count,tick_send(),out);
    for(int i=0;i<count;i++) free(names[i]);
    return resp;
}

static char *handle_publish(const char *ch, const char *user, const char *msg,
                             double ts, long long recv_clk, size_t *out) {
    if (!ch[0]||!msg[0]) return make_resp("error","Channel and message required",NULL,0,tick_send(),out);
    sqlite3_stmt *chk;
    sqlite3_prepare_v2(db,"SELECT name FROM channels WHERE name=?",-1,&chk,NULL);
    sqlite3_bind_text(chk,1,ch,-1,SQLITE_STATIC);
    int exists=(sqlite3_step(chk)==SQLITE_ROW); sqlite3_finalize(chk);
    if(!exists) {
        char em[128]; snprintf(em,sizeof(em),"Channel '%s' does not exist",ch);
        return make_resp("error",em,NULL,0,tick_send(),out);
    }
    sqlite3_stmt *ins;
    sqlite3_prepare_v2(db,"INSERT INTO messages (channel,username,message,timestamp,clock) VALUES (?,?,?,?,?)",-1,&ins,NULL);
    sqlite3_bind_text(ins,1,ch,-1,SQLITE_STATIC); sqlite3_bind_text(ins,2,user,-1,SQLITE_STATIC);
    sqlite3_bind_text(ins,3,msg,-1,SQLITE_STATIC); sqlite3_bind_double(ins,4,ts);
    sqlite3_bind_int64(ins,5,recv_clk);
    sqlite3_step(ins); sqlite3_finalize(ins);

    long long pub_clk = tick_send();
    char *pb=NULL; size_t pb_sz=0;
    mpack_writer_t pw; mpack_writer_init_growable(&pw,&pb,&pb_sz);
    mpack_start_map(&pw,6);
    mpack_write_cstr(&pw,"channel");   mpack_write_cstr(&pw,ch);
    mpack_write_cstr(&pw,"username");  mpack_write_cstr(&pw,user);
    mpack_write_cstr(&pw,"message");   mpack_write_cstr(&pw,msg);
    mpack_write_cstr(&pw,"timestamp"); mpack_write_double(&pw,ts);
    mpack_write_cstr(&pw,"received");  mpack_write_double(&pw,now_ts());
    mpack_write_cstr(&pw,"clock");     mpack_write_i64(&pw,pub_clk);
    mpack_finish_map(&pw); mpack_writer_destroy(&pw);
    zmq_send(pub_socket,ch,strlen(ch),ZMQ_SNDMORE);
    zmq_send(pub_socket,pb,pb_sz,0);
    free(pb);
    printf("[%s] PUB  | channel=%-15s | from=%-12s | clock=%lld | %s\n",server_name,ch,user,pub_clk,msg);
    fflush(stdout);
    return make_resp("ok","Published!",NULL,0,tick_send(),out);
}

int main(void) {
    const char *pe = getenv("PORT"); int port = pe ? atoi(pe) : 5553;
    const char *ph = getenv("PROXY_HOST"); if(!ph) ph="proxy";
    const char *xp = getenv("XSUB_PORT"); if(!xp) xp="5557";
    const char *sn = getenv("SERVER_NAME"); if(sn) strncpy(server_name,sn,sizeof(server_name)-1);
    const char *rh = getenv("REF_HOST"); if(rh) strncpy(ref_host,rh,sizeof(ref_host)-1);
    const char *rp = getenv("REF_PORT"); if(rp) strncpy(ref_port,rp,sizeof(ref_port)-1);

    init_db();
    zmq_ctx = zmq_ctx_new();
    sleep(2);
    call_reference("register", -1);

    void *rep = zmq_socket(zmq_ctx,ZMQ_REP);
    char addr[64]; snprintf(addr,sizeof(addr),"tcp://*:%d",port);
    zmq_bind(rep,addr);

    pub_socket = zmq_socket(zmq_ctx,ZMQ_PUB);
    char pub_addr[128]; snprintf(pub_addr,sizeof(pub_addr),"tcp://%s:%s",ph,xp);
    zmq_connect(pub_socket,pub_addr);
    struct timespec ts0={0,500000000}; nanosleep(&ts0,NULL);

    printf("[%s] Listening on port %d | rank=%d\n",server_name,port,server_rank); fflush(stdout);

    static char recv_buf[MAX_BUF];
    long long msg_count=0;
    while(1) {
        int nb=zmq_recv(rep,recv_buf,MAX_BUF-1,0); if(nb<0) continue;
        mpack_tree_t tree;
        mpack_tree_init_data(&tree,recv_buf,nb);
        mpack_tree_parse(&tree);
        mpack_node_t root=mpack_tree_root(&tree);

        char type[64],username[256],channel[256],message[1024];
        get_str(root,"type",type,sizeof(type));
        get_str(root,"username",username,sizeof(username));
        get_str(root,"channel_name",channel,sizeof(channel));
        get_str(root,"message",message,sizeof(message));
        double recv_ts=get_double(root,"timestamp");
        long long recv_clk=get_int(root,"clock");
        mpack_tree_destroy(&tree);

        tick_recv(recv_clk); msg_count++;
        printf("[%s] RECV | type=%-10s | from=%-12s | clock=%lld | lc=%lld\n",
               server_name,type,username,recv_clk,logical_clock); fflush(stdout);

        size_t rsz=0; char *rbuf=NULL;
        if      (!strcmp(type,"login"))   rbuf=handle_login(username,&rsz);
        else if (!strcmp(type,"channel")) rbuf=handle_create_channel(channel,username,&rsz);
        else if (!strcmp(type,"list"))    rbuf=handle_list(&rsz);
        else if (!strcmp(type,"publish")) rbuf=handle_publish(channel,username,message,recv_ts,recv_clk,&rsz);
        else { char em[128]; snprintf(em,sizeof(em),"Unknown: %s",type); rbuf=make_resp("error",em,NULL,0,tick_send(),&rsz); }

        zmq_send(rep,rbuf,rsz,0); free(rbuf);

        if(msg_count%10==0) call_reference("heartbeat",msg_count);
    }
    return 0;
}
