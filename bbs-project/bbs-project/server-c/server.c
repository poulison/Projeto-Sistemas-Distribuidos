// server-c.c — Parte 5: Replicação via PUB/SUB
// Adicionado: thread de replicação que assina TODOS os tópicos do proxy
//             e salva mensagens recebidas de outros servidores com INSERT OR IGNORE

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
#define SNAME_MAX 64

static sqlite3   *db;
static void      *pub_socket;
static char       server_name[SNAME_MAX];
static char       ref_host[64], ref_port[16];
static char       proxy_host[64], xsub_port[16], xpub_port[16];
static char       s2s_port_str[16];
static int        server_rank = 0;
static long long  logical_clock = 0;
static double     time_offset   = 0.0;
static char       coordinator[SNAME_MAX] = "";
static pthread_mutex_t clock_mu   = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t offset_mu  = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t coord_mu   = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t db_mu      = PTHREAD_MUTEX_INITIALIZER;

typedef struct { char name[SNAME_MAX]; int rank; } ServerInfo;
static ServerInfo known_servers[32];
static int        known_count = 0;
static pthread_mutex_t servers_mu = PTHREAD_MUTEX_INITIALIZER;
static void *ctx_global = NULL;

static double now_ts(void) {
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    double t = (double)ts.tv_sec + (double)ts.tv_nsec/1e9;
    pthread_mutex_lock(&offset_mu); t += time_offset; pthread_mutex_unlock(&offset_mu);
    return t;
}
static long long tick_send(void) {
    pthread_mutex_lock(&clock_mu); logical_clock++;
    long long lc=logical_clock; pthread_mutex_unlock(&clock_mu); return lc;
}
static void tick_recv(long long r) {
    pthread_mutex_lock(&clock_mu);
    if(r>logical_clock) logical_clock=r;
    pthread_mutex_unlock(&clock_mu);
}

static void get_str(mpack_node_t root, const char *key, char *buf, size_t size) {
    buf[0]='\0';
    mpack_node_t n=mpack_node_map_cstr(root,key);
    if(mpack_node_type(n)==mpack_type_str) mpack_node_copy_utf8_cstr(n,buf,size);
}
static double get_double_node(mpack_node_t root, const char *key) {
    mpack_node_t n=mpack_node_map_cstr(root,key);
    if(mpack_node_type(n)!=mpack_type_missing) return mpack_node_double(n);
    return 0.0;
}
static long long get_ll(mpack_node_t root, const char *key) {
    mpack_node_t n=mpack_node_map_cstr(root,key);
    if(mpack_node_type(n)!=mpack_type_missing) return (long long)mpack_node_i64(n);
    return 0;
}

// ── ID único para deduplicação ────────────────────────────────────────────────
static void make_msg_id(const char *channel, const char *username,
                        const char *message, double ts, char *out, size_t out_size) {
    // hash simples: primeiros 16 chars de sha-like combinando os campos
    unsigned long h = 5381;
    for(const char *p=channel; *p; p++) h = ((h<<5)+h) ^ (unsigned char)*p;
    h ^= (unsigned long)(ts * 1000);
    for(const char *p=username; *p; p++) h = ((h<<5)+h) ^ (unsigned char)*p;
    for(const char *p=message; *p; p++) h = ((h<<5)+h) ^ (unsigned char)*p;
    snprintf(out, out_size, "%016lx", h);
}

static void init_db(void) {
    mkdir("/data", 0755);
    sqlite3_open(DB_PATH, &db);
    sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, created_at REAL NOT NULL);"
        "CREATE TABLE IF NOT EXISTS logins (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, timestamp REAL NOT NULL);"
        "CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at REAL NOT NULL);"
        "CREATE TABLE IF NOT EXISTS messages ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  msg_id TEXT UNIQUE NOT NULL,"
        "  channel TEXT NOT NULL, username TEXT NOT NULL,"
        "  message TEXT NOT NULL, timestamp REAL NOT NULL,"
        "  clock INTEGER NOT NULL DEFAULT 0,"
        "  origin TEXT NOT NULL DEFAULT 'local');",
        NULL, NULL, NULL);
}

// ── replicação: salva mensagem recebida via SUB ───────────────────────────────
static void replicate_message(const char *channel, const char *username,
                              const char *message, double ts, long long clk, const char *origin) {
    if(!channel[0] || !message[0]) return;
    char msg_id[32];
    make_msg_id(channel, username, message, ts, msg_id, sizeof(msg_id));

    pthread_mutex_lock(&db_mu);
    // garante que canal existe
    sqlite3_stmt *sc;
    sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO channels (name,created_by,created_at) VALUES(?,?,?)",-1,&sc,NULL);
    sqlite3_bind_text(sc,1,channel,-1,SQLITE_STATIC); sqlite3_bind_text(sc,2,username,-1,SQLITE_STATIC); sqlite3_bind_double(sc,3,ts);
    sqlite3_step(sc); sqlite3_finalize(sc);

    sqlite3_stmt *sm;
    sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO messages (msg_id,channel,username,message,timestamp,clock,origin) VALUES(?,?,?,?,?,?,?)",-1,&sm,NULL);
    sqlite3_bind_text(sm,1,msg_id,-1,SQLITE_STATIC); sqlite3_bind_text(sm,2,channel,-1,SQLITE_STATIC);
    sqlite3_bind_text(sm,3,username,-1,SQLITE_STATIC); sqlite3_bind_text(sm,4,message,-1,SQLITE_STATIC);
    sqlite3_bind_double(sm,5,ts); sqlite3_bind_int64(sm,6,clk); sqlite3_bind_text(sm,7,origin,-1,SQLITE_STATIC);
    int rc=sqlite3_step(sm); sqlite3_finalize(sm);
    pthread_mutex_unlock(&db_mu);

    if(rc==SQLITE_DONE) {
        printf("[%s] REPL | channel=%-15s | from=%-12s | origin=%s\n",server_name,channel,username,origin); fflush(stdout);
    }
}

// ── thread de replicação ──────────────────────────────────────────────────────
static void *replication_thread(void *arg) {
    void *ctx=zmq_ctx_new(); void *sub=zmq_socket(ctx,ZMQ_SUB);
    char addr[128]; snprintf(addr,sizeof(addr),"tcp://%s:%s",proxy_host,xpub_port);
    zmq_connect(sub,addr);
    sleep(1);
    zmq_setsockopt(sub,ZMQ_SUBSCRIBE,"",0); // inscreve em TUDO
    printf("[%s] REPL SUB | subscribed to all topics on proxy\n",server_name); fflush(stdout);

    static char topic_buf[256], data_buf[MAX_BUF];
    while(1){
        int t=zmq_recv(sub,topic_buf,sizeof(topic_buf)-1,0); if(t<0) continue; topic_buf[t]='\0';
        int d=zmq_recv(sub,data_buf,MAX_BUF-1,0); if(d<0) continue;
        if(strcmp(topic_buf,"servers")==0) continue; // ignora eleição

        mpack_tree_t tree; mpack_tree_init_data(&tree,data_buf,d); mpack_tree_parse(&tree);
        mpack_node_t root=mpack_tree_root(&tree);
        char channel[256]="",username[256]="",message[1024]="",origin[SNAME_MAX]="local";
        get_str(root,"channel",channel,sizeof(channel));
        get_str(root,"username",username,sizeof(username));
        get_str(root,"message",message,sizeof(message));
        get_str(root,"origin",origin,sizeof(origin));
        double ts=get_double_node(root,"timestamp");
        long long clk=get_ll(root,"clock");
        tick_recv(clk);
        mpack_tree_destroy(&tree);
        replicate_message(channel,username,message,ts,clk,origin);
    }
    return NULL;
}

// ── helpers de comunicação ───────────────────────────────────────────────────
static int zmq_req_rep(void *ctx, const char *addr, const char *send_buf, size_t send_size,
                       char *recv_buf, int *recv_size, int timeout_ms) {
    void *sock=zmq_socket(ctx,ZMQ_REQ);
    zmq_setsockopt(sock,ZMQ_RCVTIMEO,&timeout_ms,sizeof(timeout_ms));
    int linger=0; zmq_setsockopt(sock,ZMQ_LINGER,&linger,sizeof(linger));
    zmq_connect(sock,addr);
    int r=zmq_send(sock,send_buf,send_size,0);
    if(r<0){zmq_close(sock);return -1;}
    *recv_size=zmq_recv(sock,recv_buf,MAX_BUF-1,0);
    zmq_close(sock); return *recv_size>0?0:-1;
}

static char *build_req(const char *type, long long clk, size_t *out_size) {
    char *buf=NULL; mpack_writer_t w; mpack_writer_init_growable(&w,&buf,out_size);
    mpack_start_map(&w,4);
    mpack_write_cstr(&w,"type");      mpack_write_cstr(&w,type);
    mpack_write_cstr(&w,"name");      mpack_write_cstr(&w,server_name);
    mpack_write_cstr(&w,"clock");     mpack_write_i64(&w,clk);
    mpack_write_cstr(&w,"timestamp"); mpack_write_double(&w,now_ts());
    mpack_finish_map(&w); mpack_writer_destroy(&w); return buf;
}

static int call_ref(const char *type, char *recv_buf, int *recv_size) {
    size_t sz=0; char *msg=build_req(type,tick_send(),&sz);
    char addr[128]; snprintf(addr,sizeof(addr),"tcp://%s:%s",ref_host,ref_port);
    int r=zmq_req_rep(ctx_global,addr,msg,sz,recv_buf,recv_size,5000);
    free(msg); return r;
}

static int s2s_port_of(const char *name) {
    if(strcmp(name,"server-python")==0) return 5560;
    if(strcmp(name,"server-go")==0)     return 5561;
    if(strcmp(name,"server-csharp")==0) return 5562;
    if(strcmp(name,"server-c")==0)      return 5563;
    if(strcmp(name,"server-lua")==0)    return 5564;
    return 5560;
}

static int call_s2s(const char *srv_name, const char *type, char *recv_buf, int *recv_size) {
    size_t sz=0;
    char *buf=NULL; mpack_writer_t w; mpack_writer_init_growable(&w,&buf,&sz);
    mpack_start_map(&w,4);
    mpack_write_cstr(&w,"type");      mpack_write_cstr(&w,type);
    mpack_write_cstr(&w,"name");      mpack_write_cstr(&w,server_name);
    mpack_write_cstr(&w,"rank");      mpack_write_i64(&w,server_rank);
    mpack_write_cstr(&w,"clock");     mpack_write_i64(&w,tick_send());
    mpack_finish_map(&w); mpack_writer_destroy(&w);
    char addr[128]; snprintf(addr,sizeof(addr),"tcp://%s:%d",srv_name,s2s_port_of(srv_name));
    int r=zmq_req_rep(ctx_global,addr,buf,sz,recv_buf,recv_size,3000);
    free(buf); return r;
}

static void connect_to_reference(void) {
    char rbuf[MAX_BUF]; int rsz=0;
    if(call_ref("register",rbuf,&rsz)<0) return;
    mpack_tree_t t; mpack_tree_init_data(&t,rbuf,rsz); mpack_tree_parse(&t);
    mpack_node_t root=mpack_tree_root(&t);
    tick_recv(get_ll(root,"clock"));
    mpack_node_t rn=mpack_node_map_cstr(root,"rank");
    if(mpack_node_type(rn)!=mpack_type_missing) server_rank=(int)mpack_node_i64(rn);
    mpack_tree_destroy(&t);
    printf("[%s] Registered | rank=%d\n",server_name,server_rank); fflush(stdout);
}

static void get_server_list(void) {
    char rbuf[MAX_BUF]; int rsz=0;
    if(call_ref("list",rbuf,&rsz)<0) return;
    mpack_tree_t t; mpack_tree_init_data(&t,rbuf,rsz); mpack_tree_parse(&t);
    mpack_node_t root=mpack_tree_root(&t);
    tick_recv(get_ll(root,"clock"));
    mpack_node_t sn=mpack_node_map_cstr(root,"servers");
    pthread_mutex_lock(&servers_mu); known_count=0;
    if(mpack_node_type(sn)==mpack_type_array){
        size_t len=mpack_node_array_length(sn);
        for(size_t i=0;i<len&&known_count<32;i++){
            mpack_node_t item=mpack_node_array_at(sn,i);
            mpack_node_t nn=mpack_node_map_cstr(item,"name");
            mpack_node_t rn2=mpack_node_map_cstr(item,"rank");
            if(mpack_node_type(nn)==mpack_type_str){
                mpack_node_copy_utf8_cstr(nn,known_servers[known_count].name,SNAME_MAX);
                known_servers[known_count].rank=(int)mpack_node_i64(rn2);
                known_count++;
            }
        }
    }
    pthread_mutex_unlock(&servers_mu);
    mpack_tree_destroy(&t);
}

static void send_heartbeat(void) {
    char rbuf[MAX_BUF]; int rsz=0;
    call_ref("heartbeat",rbuf,&rsz);
    printf("[%s] HEARTBEAT sent | rank=%d | clock=%lld\n",server_name,server_rank,logical_clock); fflush(stdout);
}

static void start_election(void); // forward declaration

static void sync_with_coordinator(void) {
    char coord[SNAME_MAX];
    pthread_mutex_lock(&coord_mu); strncpy(coord,coordinator,SNAME_MAX); pthread_mutex_unlock(&coord_mu);
    if(coord[0]=='\0'||strcmp(coord,server_name)==0) return;
    char rbuf[MAX_BUF]; int rsz=0;
    if(call_s2s(coord,"get_time",rbuf,&rsz)<0){
        printf("[%s] Coordinator '%s' unreachable — starting election\n",server_name,coord); fflush(stdout);
        start_election();
        return;
    }
    mpack_tree_t t; mpack_tree_init_data(&t,rbuf,rsz); mpack_tree_parse(&t);
    mpack_node_t root=mpack_tree_root(&t);
    tick_recv(get_ll(root,"clock"));
    double ref_time=get_double_node(root,"time");
    if(ref_time>0){
        struct timespec ts; clock_gettime(CLOCK_REALTIME,&ts);
        double real_now=(double)ts.tv_sec+(double)ts.tv_nsec/1e9;
        pthread_mutex_lock(&offset_mu); time_offset=ref_time-real_now; pthread_mutex_unlock(&offset_mu);
        printf("[%s] CLOCK SYNC | coord=%s | ref_time=%.3f | offset=%.6f\n",server_name,coord,ref_time,time_offset); fflush(stdout);
    }
    mpack_tree_destroy(&t);
}

static void announce_coordinator(void) {
    long long clk=tick_send();
    char *buf=NULL; size_t sz=0; mpack_writer_t w;
    mpack_writer_init_growable(&w,&buf,&sz);
    mpack_start_map(&w,3);
    mpack_write_cstr(&w,"coordinator"); mpack_write_cstr(&w,server_name);
    mpack_write_cstr(&w,"clock");       mpack_write_i64(&w,clk);
    mpack_write_cstr(&w,"timestamp");   mpack_write_double(&w,now_ts());
    mpack_finish_map(&w); mpack_writer_destroy(&w);
    zmq_send(pub_socket,"servers",7,ZMQ_SNDMORE);
    zmq_send(pub_socket,buf,sz,0); free(buf);
    printf("[%s] ELECTED as coordinator | clock=%lld\n",server_name,clk); fflush(stdout);
}

static void start_election(void) {
    printf("[%s] Starting election | rank=%d\n",server_name,server_rank); fflush(stdout);
    get_server_list();

    typedef struct{char name[SNAME_MAX];int rank;} Cand;
    Cand candidates[33]; int nc=0;
    strncpy(candidates[nc].name,server_name,SNAME_MAX); candidates[nc].rank=server_rank; nc++;

    pthread_mutex_lock(&servers_mu);
    for(int i=0;i<known_count;i++){
        if(strcmp(known_servers[i].name,server_name)==0) continue;
        char rbuf[MAX_BUF]; int rsz=0;
        if(call_s2s(known_servers[i].name,"election",rbuf,&rsz)==0){
            mpack_tree_t t; mpack_tree_init_data(&t,rbuf,rsz); mpack_tree_parse(&t);
            int rk=(int)get_ll(mpack_tree_root(&t),"rank"); mpack_tree_destroy(&t);
            strncpy(candidates[nc].name,known_servers[i].name,SNAME_MAX); candidates[nc].rank=rk; nc++;
        }
    }
    pthread_mutex_unlock(&servers_mu);

    int winner_idx=0;
    for(int i=1;i<nc;i++) if(candidates[i].rank<candidates[winner_idx].rank) winner_idx=i;
    pthread_mutex_lock(&coord_mu); strncpy(coordinator,candidates[winner_idx].name,SNAME_MAX); pthread_mutex_unlock(&coord_mu);
    if(strcmp(candidates[winner_idx].name,server_name)==0) announce_coordinator();
    printf("[%s] Election result: coordinator='%s'\n",server_name,coordinator); fflush(stdout);
}

static void *s2s_server_thread(void *arg) {
    void *ctx=zmq_ctx_new(); void *sock=zmq_socket(ctx,ZMQ_REP);
    char addr[64]; snprintf(addr,sizeof(addr),"tcp://*:%s",s2s_port_str);
    zmq_bind(sock,addr);
    static char rbuf[MAX_BUF];
    while(1){
        int nb=zmq_recv(sock,rbuf,MAX_BUF-1,0); if(nb<0) continue;
        mpack_tree_t t; mpack_tree_init_data(&t,rbuf,nb); mpack_tree_parse(&t);
        mpack_node_t root=mpack_tree_root(&t);
        char type[64]="",from[SNAME_MAX]="";
        get_str(root,"type",type,sizeof(type)); get_str(root,"name",from,sizeof(from));
        tick_recv(get_ll(root,"clock")); mpack_tree_destroy(&t);

        char *resp=NULL; size_t rsz=0; mpack_writer_t w;
        if(strcmp(type,"get_time")==0){
            mpack_writer_init_growable(&w,&resp,&rsz); mpack_start_map(&w,4);
            mpack_write_cstr(&w,"status");    mpack_write_cstr(&w,"ok");
            mpack_write_cstr(&w,"time");      mpack_write_double(&w,now_ts());
            mpack_write_cstr(&w,"clock");     mpack_write_i64(&w,tick_send());
            mpack_write_cstr(&w,"timestamp"); mpack_write_double(&w,now_ts());
            mpack_finish_map(&w); mpack_writer_destroy(&w);
        } else {
            mpack_writer_init_growable(&w,&resp,&rsz); mpack_start_map(&w,4);
            mpack_write_cstr(&w,"status");    mpack_write_cstr(&w,"ok");
            mpack_write_cstr(&w,"rank");      mpack_write_i64(&w,server_rank);
            mpack_write_cstr(&w,"clock");     mpack_write_i64(&w,tick_send());
            mpack_write_cstr(&w,"timestamp"); mpack_write_double(&w,now_ts());
            mpack_finish_map(&w); mpack_writer_destroy(&w);
        }
        zmq_send(sock,resp,rsz,0); free(resp);
    }
    return NULL;
}

static void *servers_sub_thread(void *arg) {
    void *ctx=zmq_ctx_new(); void *sub=zmq_socket(ctx,ZMQ_SUB);
    char addr[128]; snprintf(addr,sizeof(addr),"tcp://%s:%s",proxy_host,xpub_port);
    zmq_connect(sub,addr); usleep(500000);
    zmq_setsockopt(sub,ZMQ_SUBSCRIBE,"servers",7);
    static char tbuf[256],dbuf[MAX_BUF];
    while(1){
        int t=zmq_recv(sub,tbuf,sizeof(tbuf)-1,0); if(t<0) continue; tbuf[t]='\0';
        int d=zmq_recv(sub,dbuf,MAX_BUF-1,0); if(d<0) continue;
        mpack_tree_t tree; mpack_tree_init_data(&tree,dbuf,d); mpack_tree_parse(&tree);
        mpack_node_t root=mpack_tree_root(&tree);
        tick_recv(get_ll(root,"clock"));
        char new_coord[SNAME_MAX]=""; get_str(root,"coordinator",new_coord,sizeof(new_coord));
        if(new_coord[0]){pthread_mutex_lock(&coord_mu);strncpy(coordinator,new_coord,SNAME_MAX);pthread_mutex_unlock(&coord_mu);}
        mpack_tree_destroy(&tree);
    }
    return NULL;
}

// ── make_resp / handlers clientes ────────────────────────────────────────────
static char *make_resp(const char *status, const char *message, char **data, int data_len, size_t *out_size) {
    char *buf=NULL; mpack_writer_t w; mpack_writer_init_growable(&w,&buf,out_size);
    int fields=(data?5:4);
    mpack_start_map(&w,fields);
    mpack_write_cstr(&w,"status");    mpack_write_cstr(&w,status);
    mpack_write_cstr(&w,"message");   mpack_write_cstr(&w,message);
    mpack_write_cstr(&w,"clock");     mpack_write_i64(&w,tick_send());
    mpack_write_cstr(&w,"timestamp"); mpack_write_double(&w,now_ts());
    if(data){ mpack_write_cstr(&w,"data"); mpack_start_array(&w,data_len);
        for(int i=0;i<data_len;i++) mpack_write_cstr(&w,data[i]); mpack_finish_array(&w); }
    mpack_finish_map(&w); mpack_writer_destroy(&w); return buf;
}

static char *handle_login(const char *username, size_t *out) {
    if(!username[0]) return make_resp("error","Username cannot be empty",NULL,0,out);
    pthread_mutex_lock(&db_mu);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO users (username,created_at) VALUES(?,?)",-1,&s,NULL);
    sqlite3_bind_text(s,1,username,-1,SQLITE_STATIC); sqlite3_bind_double(s,2,now_ts()); sqlite3_step(s); sqlite3_finalize(s);
    sqlite3_prepare_v2(db,"INSERT INTO logins (username,timestamp) VALUES(?,?)",-1,&s,NULL);
    sqlite3_bind_text(s,1,username,-1,SQLITE_STATIC); sqlite3_bind_double(s,2,now_ts()); sqlite3_step(s); sqlite3_finalize(s);
    pthread_mutex_unlock(&db_mu);
    char msg[256]; snprintf(msg,sizeof(msg),"Welcome, %s!",username);
    return make_resp("ok",msg,NULL,0,out);
}
static char *handle_create_channel(const char *name, const char *by, size_t *out) {
    if(!name[0]) return make_resp("error","Channel name cannot be empty",NULL,0,out);
    pthread_mutex_lock(&db_mu);
    sqlite3_stmt *s;
    sqlite3_prepare_v2(db,"INSERT INTO channels (name,created_by,created_at) VALUES(?,?,?)",-1,&s,NULL);
    sqlite3_bind_text(s,1,name,-1,SQLITE_STATIC); sqlite3_bind_text(s,2,by,-1,SQLITE_STATIC); sqlite3_bind_double(s,3,now_ts());
    int rc=sqlite3_step(s); sqlite3_finalize(s);
    pthread_mutex_unlock(&db_mu);
    if(rc==SQLITE_CONSTRAINT){ char m[128]; snprintf(m,sizeof(m),"Channel '%s' already exists",name); return make_resp("error",m,NULL,0,out); }
    char m[128]; snprintf(m,sizeof(m),"Channel '%s' created!",name); return make_resp("ok",m,NULL,0,out);
}
static char *handle_list_channels(size_t *out) {
    pthread_mutex_lock(&db_mu);
    sqlite3_stmt *s; sqlite3_prepare_v2(db,"SELECT name FROM channels ORDER BY created_at",-1,&s,NULL);
    char *names[256]; int count=0;
    while(sqlite3_step(s)==SQLITE_ROW&&count<256) names[count++]=strdup((const char*)sqlite3_column_text(s,0));
    sqlite3_finalize(s); pthread_mutex_unlock(&db_mu);
    char *resp=make_resp("ok","OK",names,count,out);
    for(int i=0;i<count;i++) free(names[i]); return resp;
}
static char *handle_publish(const char *channel, const char *username, const char *message, long long clk_in, size_t *out) {
    if(!channel[0]||!message[0]) return make_resp("error","Channel and message required",NULL,0,out);
    pthread_mutex_lock(&db_mu);
    sqlite3_stmt *chk; sqlite3_prepare_v2(db,"SELECT name FROM channels WHERE name=?",-1,&chk,NULL);
    sqlite3_bind_text(chk,1,channel,-1,SQLITE_STATIC);
    int exists=(sqlite3_step(chk)==SQLITE_ROW); sqlite3_finalize(chk); pthread_mutex_unlock(&db_mu);
    if(!exists){ char m[128]; snprintf(m,sizeof(m),"Channel '%s' does not exist",channel); return make_resp("error",m,NULL,0,out); }

    long long clk=tick_send(); double ts=now_ts();
    char msg_id[32]; make_msg_id(channel,username,message,ts,msg_id,sizeof(msg_id));

    pthread_mutex_lock(&db_mu);
    sqlite3_stmt *ins;
    sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO messages (msg_id,channel,username,message,timestamp,clock,origin) VALUES(?,?,?,?,?,?,?)",-1,&ins,NULL);
    sqlite3_bind_text(ins,1,msg_id,-1,SQLITE_STATIC); sqlite3_bind_text(ins,2,channel,-1,SQLITE_STATIC);
    sqlite3_bind_text(ins,3,username,-1,SQLITE_STATIC); sqlite3_bind_text(ins,4,message,-1,SQLITE_STATIC);
    sqlite3_bind_double(ins,5,ts); sqlite3_bind_int64(ins,6,clk); sqlite3_bind_text(ins,7,server_name,-1,SQLITE_STATIC);
    sqlite3_step(ins); sqlite3_finalize(ins); pthread_mutex_unlock(&db_mu);

    char *pbuf=NULL; size_t psz=0; mpack_writer_t w;
    mpack_writer_init_growable(&w,&pbuf,&psz); mpack_start_map(&w,7);
    mpack_write_cstr(&w,"channel");   mpack_write_cstr(&w,channel);
    mpack_write_cstr(&w,"username");  mpack_write_cstr(&w,username);
    mpack_write_cstr(&w,"message");   mpack_write_cstr(&w,message);
    mpack_write_cstr(&w,"timestamp"); mpack_write_double(&w,ts);
    mpack_write_cstr(&w,"received");  mpack_write_double(&w,ts);
    mpack_write_cstr(&w,"clock");     mpack_write_i64(&w,clk);
    mpack_write_cstr(&w,"origin");    mpack_write_cstr(&w,server_name);
    mpack_finish_map(&w); mpack_writer_destroy(&w);
    zmq_send(pub_socket,channel,strlen(channel),ZMQ_SNDMORE);
    zmq_send(pub_socket,pbuf,psz,0); free(pbuf);
    printf("[%s] PUB  | channel=%-15s | from=%-12s | clock=%lld\n",server_name,channel,username,clk); fflush(stdout);
    return make_resp("ok","Published!",NULL,0,out);
}

int main(void) {
    const char *sn=getenv("SERVER_NAME"); snprintf(server_name,SNAME_MAX,"%s",sn?sn:"server-c");
    const char *pe=getenv("PORT"); int port=pe?atoi(pe):5553;
    const char *sp=getenv("S2S_PORT"); snprintf(s2s_port_str,16,"%s",sp?sp:"5563");
    const char *ph=getenv("PROXY_HOST"); snprintf(proxy_host,64,"%s",ph?ph:"proxy");
    const char *xs=getenv("XSUB_PORT"); snprintf(xsub_port,16,"%s",xs?xs:"5557");
    const char *xp=getenv("XPUB_PORT"); snprintf(xpub_port,16,"%s",xp?xp:"5558");
    const char *rh=getenv("REF_HOST"); snprintf(ref_host,64,"%s",rh?rh:"reference");
    const char *rp=getenv("REF_PORT"); snprintf(ref_port,16,"%s",rp?rp:"5559");

    init_db(); ctx_global=zmq_ctx_new();
    pub_socket=zmq_socket(ctx_global,ZMQ_PUB);
    char pub_addr[128]; snprintf(pub_addr,sizeof(pub_addr),"tcp://%s:%s",proxy_host,xsub_port);
    zmq_connect(pub_socket,pub_addr); sleep(1);

    sleep(2); connect_to_reference(); get_server_list();

    pthread_t t1,t2,t3;
    pthread_create(&t1,NULL,s2s_server_thread,NULL); pthread_detach(t1);
    pthread_create(&t2,NULL,servers_sub_thread,NULL); pthread_detach(t2);
    pthread_create(&t3,NULL,replication_thread,NULL); pthread_detach(t3);
    sleep(1); start_election();

    void *rep_socket=zmq_socket(ctx_global,ZMQ_REP);
    char rep_addr[64]; snprintf(rep_addr,sizeof(rep_addr),"tcp://*:%d",port);
    zmq_bind(rep_socket,rep_addr);
    printf("[%s] Listening on port %d | rank=%d\n",server_name,port,server_rank); fflush(stdout);

    static char recv_buf[MAX_BUF]; long msg_count=0;
    while(1){
        int nb=zmq_recv(rep_socket,recv_buf,MAX_BUF-1,0); if(nb<0) continue;
        mpack_tree_t tree; mpack_tree_init_data(&tree,recv_buf,nb); mpack_tree_parse(&tree);
        mpack_node_t root=mpack_tree_root(&tree);
        char type[64]="",username[256]="",channel_name[256]="",message[1024]="";
        get_str(root,"type",type,sizeof(type)); get_str(root,"username",username,sizeof(username));
        get_str(root,"channel_name",channel_name,sizeof(channel_name)); get_str(root,"message",message,sizeof(message));
        long long clk_in=get_ll(root,"clock"); mpack_tree_destroy(&tree);
        tick_recv(clk_in); msg_count++;
        printf("[%s] RECV | type=%-10s | from=%-12s | clock=%lld | lc=%lld\n",server_name,type,username,clk_in,logical_clock); fflush(stdout);
        size_t rsz=0; char *rbuf=NULL;
        if     (strcmp(type,"login"  )==0) rbuf=handle_login(username,&rsz);
        else if(strcmp(type,"channel")==0) rbuf=handle_create_channel(channel_name,username,&rsz);
        else if(strcmp(type,"list"   )==0) rbuf=handle_list_channels(&rsz);
        else if(strcmp(type,"publish")==0) rbuf=handle_publish(channel_name,username,message,clk_in,&rsz);
        else { char em[128]; snprintf(em,sizeof(em),"Unknown: %s",type); rbuf=make_resp("error",em,NULL,0,&rsz); }
        zmq_send(rep_socket,rbuf,rsz,0); free(rbuf);
        if(msg_count%15==0){ send_heartbeat(); sync_with_coordinator(); }
    }
    return 0;
}