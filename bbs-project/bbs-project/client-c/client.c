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

static const char *words[] = {"ola","mundo","sistema","distribuido","mensagem","canal",
    "teste","clinguagem","zmq","pubsub","broker","topico","servidor","rede",NULL};

static double now_ts(void) {
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec/1e9;
}

static char *random_msg(char *buf, size_t size) {
    int n=3+rand()%5; buf[0]='\0';
    int wc=0; while(words[wc]) wc++;
    for(int i=0;i<n;i++) {
        if(i>0) strncat(buf," ",size-strlen(buf)-1);
        strncat(buf,words[rand()%wc],size-strlen(buf)-1);
    }
    return buf;
}

static char *build_msg(const char *type, const char *username, const char *channel,
                        const char *message, long long clk, size_t *out_size) {
    char *buf=NULL;
    mpack_writer_t w; mpack_writer_init_growable(&w,&buf,out_size);
    int fields=4;
    if(channel&&channel[0]) fields++;
    if(message&&message[0]) fields++;
    mpack_start_map(&w,fields);
    mpack_write_cstr(&w,"type");      mpack_write_cstr(&w,type);
    mpack_write_cstr(&w,"username");  mpack_write_cstr(&w,username);
    mpack_write_cstr(&w,"timestamp"); mpack_write_double(&w,now_ts());
    mpack_write_cstr(&w,"clock");     mpack_write_i64(&w,clk);
    if(channel&&channel[0]) { mpack_write_cstr(&w,"channel_name"); mpack_write_cstr(&w,channel); }
    if(message&&message[0]) { mpack_write_cstr(&w,"message");      mpack_write_cstr(&w,message); }
    mpack_finish_map(&w); mpack_writer_destroy(&w);
    return buf;
}

static int send_recv(const char *type, const char *channel, const char *message,
                     char out_ch[][64], int *out_count) {
    long long clk = tick_send();
    size_t sz=0;
    char *sbuf = build_msg(type,bot_name,channel,message,clk,&sz);
    printf("[%s] SEND | type=%-10s | clock=%lld | ts=%.3f\n",bot_name,type,clk,now_ts()); fflush(stdout);
    zmq_send(req_socket,sbuf,sz,0); free(sbuf);

    static char rbuf[MAX_BUF];
    int nb=zmq_recv(req_socket,rbuf,MAX_BUF-1,0); if(nb<0) return 0;

    mpack_tree_t tree; mpack_tree_init_data(&tree,rbuf,nb); mpack_tree_parse(&tree);
    mpack_node_t root=mpack_tree_root(&tree);

    char status[32]="",msg[256]="";
    mpack_node_t sn=mpack_node_map_cstr(root,"status");
    if(mpack_node_type(sn)==mpack_type_str) mpack_node_copy_utf8_cstr(sn,status,sizeof(status));
    mpack_node_t mn=mpack_node_map_cstr(root,"message");
    if(mpack_node_type(mn)==mpack_type_str) mpack_node_copy_utf8_cstr(mn,msg,sizeof(msg));
    long long rclk=0;
    mpack_node_t cn=mpack_node_map_cstr(root,"clock");
    if(mpack_node_type(cn)!=mpack_type_missing) rclk=(long long)mpack_node_i64(cn);
    tick_recv(rclk);

    printf("[%s] RECV | status=%-8s | clock=%lld | msg=%s\n",bot_name,status,rclk,msg); fflush(stdout);

    if(out_ch&&out_count) {
        *out_count=0;
        mpack_node_t dn=mpack_node_map_cstr(root,"data");
        if(mpack_node_type(dn)==mpack_type_array) {
            size_t len=mpack_node_array_length(dn);
            printf("[%s] Channels: [",bot_name);
            for(size_t i=0;i<len&&*out_count<32;i++) {
                mpack_node_t item=mpack_node_array_at(dn,i);
                mpack_node_copy_utf8_cstr(item,out_ch[*out_count],64);
                printf("%s%s",out_ch[*out_count],i+1<len?", ":""); (*out_count)++;
            }
            printf("]\n"); fflush(stdout);
        }
    }
    mpack_tree_destroy(&tree);
    return strcmp(status,"ok")==0?1:0;
}

static void *subscriber_fn(void *arg) {
    void *ctx=zmq_ctx_new(); void *sub=zmq_socket(ctx,ZMQ_SUB);
    char addr[128]; snprintf(addr,sizeof(addr),"tcp://%s:%s",proxy_host,xpub_port);
    zmq_connect(sub,addr); usleep(500000);
    for(int i=0;i<sub_count;i++) {
        zmq_setsockopt(sub,ZMQ_SUBSCRIBE,sub_channels[i],strlen(sub_channels[i]));
        printf("[%s] SUB  | subscribed to '%s'\n",bot_name,sub_channels[i]); fflush(stdout);
    }
    static char topic[256],data[MAX_BUF];
    while(1) {
        int t=zmq_recv(sub,topic,sizeof(topic)-1,0); if(t<0) continue; topic[t]='\0';
        int d=zmq_recv(sub,data,MAX_BUF-1,0); if(d<0) continue;
        mpack_tree_t tree; mpack_tree_init_data(&tree,data,d); mpack_tree_parse(&tree);
        mpack_node_t root=mpack_tree_root(&tree);
        char ch[64]="",user[64]="",msg[512]="";
        mpack_node_t cn=mpack_node_map_cstr(root,"channel");
        if(mpack_node_type(cn)==mpack_type_str) mpack_node_copy_utf8_cstr(cn,ch,sizeof(ch));
        mpack_node_t un=mpack_node_map_cstr(root,"username");
        if(mpack_node_type(un)==mpack_type_str) mpack_node_copy_utf8_cstr(un,user,sizeof(user));
        mpack_node_t mn=mpack_node_map_cstr(root,"message");
        if(mpack_node_type(mn)==mpack_type_str) mpack_node_copy_utf8_cstr(mn,msg,sizeof(msg));
        double ts_send=mpack_node_double(mpack_node_map_cstr(root,"timestamp"));
        long long rclk=0;
        mpack_node_t clkn=mpack_node_map_cstr(root,"clock");
        if(mpack_node_type(clkn)!=mpack_type_missing) rclk=(long long)mpack_node_i64(clkn);
        tick_recv(rclk);
        mpack_tree_destroy(&tree);
        printf("[%s] MSG  | channel=%-12s | from=%-12s | clock=%lld | sent=%.3f | recv=%.3f | %s\n",
               bot_name,ch,user,rclk,ts_send,now_ts(),msg); fflush(stdout);
    }
    return NULL;
}

int main(void) {
    srand((unsigned)time(NULL)^(unsigned)getpid());
    bot_name   = getenv("BOT_NAME");    if(!bot_name)   bot_name   = "bot-c-1";
    const char *host=getenv("SERVER_HOST"); if(!host) host="server-c";
    const char *port=getenv("SERVER_PORT"); if(!port) port="5553";
    proxy_host = getenv("PROXY_HOST"); if(!proxy_host) proxy_host="proxy";
    xpub_port  = getenv("XPUB_PORT");  if(!xpub_port)  xpub_port ="5558";

    sleep(4);
    void *ctx=zmq_ctx_new(); req_socket=zmq_socket(ctx,ZMQ_REQ);
    char addr[128]; snprintf(addr,sizeof(addr),"tcp://%s:%s",host,port);
    zmq_connect(req_socket,addr);
    printf("[%s] Connected to %s\n",bot_name,addr); fflush(stdout);

    while(!send_recv("login",NULL,NULL,NULL,NULL)) sleep(2);
    printf("[%s] Login successful!\n",bot_name); fflush(stdout);

    static char channels[32][64]; int ch_count=0;
    send_recv("list",NULL,NULL,channels,&ch_count);
    if(ch_count<5) {
        char nc[64]; snprintf(nc,sizeof(nc),"ch-c-%d",100+rand()%900);
        send_recv("channel",nc,NULL,NULL,NULL);
        send_recv("list",NULL,NULL,channels,&ch_count);
    }

    sub_count=ch_count<3?ch_count:3;
    for(int i=0;i<sub_count;i++) strncpy(sub_channels[i],channels[i],64);

    pthread_t tid; pthread_create(&tid,NULL,subscriber_fn,NULL);
    sleep(2);

    printf("[%s] Starting publish loop\n",bot_name); fflush(stdout);
    char mbuf[256];
    while(1) {
        int idx=rand()%ch_count;
        for(int i=0;i<10;i++) {
            send_recv("publish",channels[idx],random_msg(mbuf,sizeof(mbuf)),NULL,NULL);
            sleep(1);
        }
        send_recv("list",NULL,NULL,channels,&ch_count);
    }
    return 0;
}
