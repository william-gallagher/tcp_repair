// Some basic code to exercise TCP repair
//
// Create 3 threads.
// Threads 1 and 2 alternate operation by sending chunks of a file to thread 3.
// Threads 1 and 2 pass the connection state between them. Each time they
// operate, they create a new socket and use TCP repair mode to update the
// socket state.
//
// Thread 3, the receiver has no knowledge that the other side of the connection
// is being swapped back and forth between the two threads...


#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <unistd.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdbool.h>

#define IP_ADR_STR "127.0.0.1"
#define PORT 7777
#define FILE_SZ 100000


typedef struct {
    bool initialized;
    bool done;
    int repair_cnt;
    int sent_bytes;

    // TCP connection state
    uint32_t local_ip;
    uint32_t remote_ip;
    uint16_t local_port;
    uint16_t remote_port;
    uint32_t local_seq;
    uint32_t remote_seq;
    uint32_t timestamp;
    uint8_t win_scale;
    int mss_clamp;
    struct tcp_repair_window win;
    struct tcp_info info;
} conn_data_t;

conn_data_t conn_data;

#define print_error(operation) {                             \
    char err_buf[100] = {0};                                 \
    strerror_r(errno, err_buf, sizeof(err_buf));             \
    printf("ERROR in %s: [%s] in function %s at line %d\n",  \
           operation,                                        \
           err_buf,                                          \
          __func__,                                          \
          __LINE__);                                         \
};

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>

#define LOCKFILE "/tmp/lock_file"

bool set_lock(int fd)
{
    int rc = flock(fd, LOCK_EX);
    if (rc != 0) {
        print_error("flock()");
        return false;
    }
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    printf("setting lock: PID [%u], fd = %d, %lu\n", getpid(), fd, ts.tv_sec);
    return true;
}

bool release_lock(int fd)
{
    int rc = flock(fd, LOCK_UN);
    if (rc != 0) {
        print_error("flock()");
        return false;
    }

    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    printf("releasing lock: PID [%u], fd = %d, %lu\n", getpid(), fd, ts.tv_sec);
    return true;
}


bool save_conn_state(int recv_fd, int lock_fd)
{
    // small delay to make sure no traffic is going back and forth on the connection
    usleep(20000);

    // Put the socket into repair mode
    int opt = 1;
    int rc = setsockopt(recv_fd, SOL_TCP, TCP_REPAIR, &opt, sizeof(opt));
    if (rc < 0) {
        print_error("setsockopt()");
        return false;
    }

    socklen_t optlen = sizeof(conn_data.info);
    rc = getsockopt(recv_fd, SOL_TCP, TCP_INFO, &conn_data.info, &optlen);
    if (rc < 0) {
        print_error("getsockopt()");
        return false;
    }

    opt = TCP_RECV_QUEUE;
    rc = setsockopt(recv_fd, SOL_TCP, TCP_REPAIR_QUEUE, &opt, sizeof(opt));
    if (rc < 0) {
        print_error("setsockopt()");
        return false;
    }

    optlen = sizeof(&conn_data.remote_seq);
    rc = getsockopt(recv_fd, SOL_TCP, TCP_QUEUE_SEQ, &conn_data.remote_seq, &optlen);
    if (rc < 0) {
        print_error("getsockopt()");
        return false;
    }

    opt = TCP_SEND_QUEUE;
    rc = setsockopt(recv_fd, SOL_TCP, TCP_REPAIR_QUEUE, &opt, sizeof(opt));
    if (rc < 0) {
        print_error("setsockopt()");
        return false;
    }

    optlen = sizeof(&conn_data.local_seq);
    rc = getsockopt(recv_fd, SOL_TCP, TCP_QUEUE_SEQ, &conn_data.local_seq, &optlen);
    if (rc < 0) {
        print_error("getsockopt()");
        return false;
    }

    optlen = sizeof(conn_data.win);
    rc = getsockopt(recv_fd, SOL_TCP, TCP_REPAIR_WINDOW, &conn_data.win, &optlen);
    if (rc < 0) {
        print_error("getsockopt()");
        return false;
    }

    // Extract the tcp timestamp
    optlen = sizeof(conn_data.timestamp);
    rc = getsockopt(recv_fd, SOL_TCP, TCP_TIMESTAMP, &conn_data.timestamp, &optlen);
    if (rc < 0) {
        print_error("getsockopt()");
        return false;
    }

    // Extract the mss clamp value
    optlen = sizeof(conn_data.mss_clamp);
    rc = getsockopt(recv_fd, SOL_TCP, TCP_MAXSEG, &conn_data.mss_clamp, &optlen);
    if (rc < 0) {
        print_error("getsockopt()");
        return false;
    }

    close(recv_fd);

    // Go back to the beginning of the file
    rc = lseek(lock_fd, 0, SEEK_SET);
    if (rc == -1) {
        print_error("lseek()");
        return false;
    } else if (rc != 0) {
        return false;
    }

    rc = write(lock_fd, &conn_data, sizeof(conn_data));
    if (rc == -1) {
        print_error("write()");
        return false;
    }

    return true;
}

int restore_conn_state(int lock_fd)
{
    // rewind to the beginning of the file
    int rc = lseek(lock_fd, 0, SEEK_SET);
    if (rc == -1) {
        print_error("lseek()");
        return false;
    } else if (rc != 0) {
        return false;
    }

    rc = read(lock_fd, &conn_data, sizeof(conn_data));
    printf("read %d bytes from the lock file\n", rc);
    if (rc == -1) {
        print_error("read()");
        return false;
    }


    // Create a new socket....
    int new_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (new_fd < 0) {
        print_error("socket()");
        return -1;
    }

    int opt = 1;
    rc = setsockopt(new_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    if (rc < 0) {
        print_error("setsockopt()");
        return -1;
    }

    // Put the new socket into REPAIR mode
    opt = 1;
    rc = setsockopt(new_fd, SOL_TCP, TCP_REPAIR, &opt, sizeof(opt));
    if (rc < 0) {
        print_error("setsockopt()");
        return -1;
    }

    // Restore remote sequence number
    opt = TCP_RECV_QUEUE;
    rc = setsockopt(new_fd, SOL_TCP, TCP_REPAIR_QUEUE, &opt, sizeof(opt));
    if (rc < 0) {
        print_error("setsockopt()");
        return -1;
    }

    rc = setsockopt(new_fd, SOL_TCP, TCP_QUEUE_SEQ, &conn_data.remote_seq, sizeof(conn_data.remote_seq));
    if (rc < 0) {
        print_error("setsockopt()");
        return -1;
    }

    // Restore the local sequence number
    opt = TCP_SEND_QUEUE;
    rc = setsockopt(new_fd, SOL_TCP, TCP_REPAIR_QUEUE, &opt, sizeof(opt));
    if (rc < 0) {
        print_error("setsockopt()");
        return -1;
    }

    rc = setsockopt(new_fd, SOL_TCP, TCP_QUEUE_SEQ, &conn_data.local_seq, sizeof(conn_data.local_seq));
    if (rc < 0) {
        print_error("setsockopt()");
        return -1;
    }

    // Restore the local IP address and TCP port
    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof(bind_addr));
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = inet_addr(IP_ADR_STR);
    bind_addr.sin_port = htobe16(PORT);
    rc = bind(new_fd, (struct sockaddr *) &bind_addr, sizeof(bind_addr));
    if (rc < 0) {
        print_error("bind()");
        return -1;
    }

    struct sockaddr_in s_in;
    memset(&s_in, 0, sizeof(s_in));
    s_in.sin_addr.s_addr = htobe32(conn_data.remote_ip);
    s_in.sin_family = AF_INET;
    s_in.sin_port = htobe16(conn_data.remote_port);
    printf("using %u as the port, using %u as the ip\n", conn_data.remote_port, conn_data.remote_ip);
    rc = connect(new_fd, (struct sockaddr *) &s_in, sizeof(s_in));
    if (rc < 0) {
        print_error("connect()");
        return -1;
    }

    // restore the TCP options
    struct tcp_repair_opt repair_opts[4] = {0};
    int i = 0;
    if (conn_data.info.tcpi_options | TCPI_OPT_SACK) {
        repair_opts[i].opt_code = TCPOPT_SACK_PERMITTED;
        i++;
    }

    if (conn_data.info.tcpi_options | TCPI_OPT_TIMESTAMPS) {
        repair_opts[i].opt_code = TCPOPT_TIMESTAMP;
        i++;
    }

    repair_opts[i].opt_code = TCPOPT_WINDOW;
    repair_opts[i].opt_val = conn_data.info.tcpi_snd_wscale + (conn_data.info.tcpi_rcv_wscale << 16);
    i++;

    repair_opts[i].opt_code = TCPOPT_MAXSEG;
    repair_opts[i].opt_val = conn_data.mss_clamp;

    rc = setsockopt(new_fd, SOL_TCP, TCP_REPAIR_OPTIONS, &repair_opts, sizeof(repair_opts));
    if (rc < 0) {
        print_error("setsockopt()");
        return -1;
    }

    // Restore the timestamp
    rc = setsockopt(new_fd, SOL_TCP, TCP_TIMESTAMP, &conn_data.timestamp, sizeof(conn_data.timestamp));
    if (rc < 0) {
        print_error("setsockopt()");
        return -1;
    }

    // Restore the window
    rc = setsockopt(new_fd, SOL_TCP, TCP_REPAIR_WINDOW, &conn_data.win, sizeof(conn_data.win));
    if (rc < 0) {
        print_error("setsockopt()");
        return -1;
    }

    // Take the socket out of REPAIR mode
    opt = 0;
    rc = setsockopt(new_fd, SOL_TCP, TCP_REPAIR, &opt, sizeof(opt));
    if (rc < 0) {
        print_error("setsockopt()");
        return -1;
    }

    printf("\nCreated new socket and restored the connection state!\n");
    printf("\tPID: %u\n", getpid());
    printf("\tTotal bytes sent so far: %d\n", conn_data.sent_bytes);
    printf("\tRemote Seq Number: %u\n", conn_data.remote_seq);
    printf("\tRemote Advertised Window: %u\n", conn_data.win.snd_wnd);
    printf("\tLocal Seq Number: %u\n", conn_data.local_seq);
    printf("\tLocal Advertised Window: %u\n", conn_data.win.rcv_wnd);
    conn_data.repair_cnt++;
    return new_fd;
}

int create_sock_listen(uint16_t port)
{
    int sock_listen = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_listen < 0) {
        print_error("socket()");
        return -1;
    }

    int opt = 1;
    int rc = setsockopt(sock_listen, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    if (rc < 0) {
        print_error("setsockopt()");
        return -1;
    }

    struct sockaddr_in bind_addr;
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = INADDR_ANY;
    bind_addr.sin_port = htobe16(port);
    rc = bind(sock_listen, (struct sockaddr *) &bind_addr, sizeof(bind_addr));
    if (rc < 0) {
        print_error("bind()");
        return -1;
    }

    rc = listen(sock_listen, 1);
    if (rc < 0) {
        print_error("listen()");
        return -1;
    }

    conn_data.local_port = port;

    return sock_listen;
}

int accept_conn(int sock_listen)
{
    struct sockaddr_in recv_addr;
    memset(&recv_addr, 0, sizeof(recv_addr));
    socklen_t recv_addr_len = sizeof(recv_addr);
    int sock_new = accept(sock_listen, (struct sockaddr *) &recv_addr, &recv_addr_len);
    if (sock_new < 0){
        print_error("accept()");
        return -1;
    }

    conn_data.remote_ip = be32toh(recv_addr.sin_addr.s_addr);
    conn_data.remote_port = be16toh(recv_addr.sin_port);

    int opt = 1;
    int rc = setsockopt(sock_new, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    if (rc < 0) {
        print_error("setsockopt()");
        return -1;
    }

    close(sock_listen);
    return sock_new;
}

//Add checks for recv Qs
void * send_func(void *args)
{
    char *src_file = (char *) args;
    int sock_listen;
    int sock_send;


    // a little testing with flock
    int r = open(LOCKFILE, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (r == -1) {
        if (errno != EEXIST) {
            print_error("open");
            pthread_exit(NULL);
        }
    }

    int r2 = open(LOCKFILE, O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (r2 == -1) {
        print_error("regular open");
        pthread_exit(NULL);
    }

    printf("PID [%u], fd = %d\n", getpid(), r2);


        FILE *read_fd = fopen(src_file, "r");
        if (!read_fd) {
            printf("debug 3\n");
        }


    set_lock(r2);
    off_t fsize = lseek(r2, 0, SEEK_END);
    if (fsize == 0) {

        sock_listen = create_sock_listen(PORT);
        if (sock_listen < 0) {
            pthread_exit(NULL);
        }

        sock_send = accept_conn(sock_listen);
        if (sock_send < 0) {
            printf("debug 1\n");
            pthread_exit(NULL);
        }

        if (!save_conn_state(sock_send, r2)) {
            printf("debug 2\n");
            pthread_exit(NULL);
        }


        conn_data.initialized = true;
    } else {
        printf("conn data already intied\n");
    }
    release_lock(r2);

    while (1) {
        set_lock(r2);
        if (conn_data.done) {
            pthread_exit(NULL);
        }

        int sock_send = restore_conn_state(r2);
        if (sock_send < 0) {
            release_lock(r2);
            pthread_exit(NULL);
        }
        char send_buf[100];

        // seek to the current offset of the file
        fseek(read_fd, conn_data.sent_bytes, SEEK_SET);


        // Send between 0 and 49 chunks of data
        int send_chunks = rand() % 50;

        for (int i = 0; i < send_chunks; i++) {
            size_t bytes = fread(send_buf, 1, sizeof(send_buf), read_fd);
            write(sock_send, send_buf, bytes);
            printf("writing %lu bytes to socket\n", bytes);
            conn_data.sent_bytes += bytes;

            if (feof(read_fd)) {
                fclose(read_fd);
                close(sock_send);
                conn_data.done = true;
                release_lock(r2);
                pthread_exit(NULL);
            }
        }

        save_conn_state(sock_send, r2);
        release_lock(r2);
        // Quick sleep to allow other thread to grab the lock...
        usleep(10);
    }
}

int main()
{
    char *src_file = "source.txt";
    send_func(src_file);

    
    //pthread_t send;
    //int rc = pthread_create(&send, NULL, send_func, src_file);
    //if (rc != 0) {
    //    print_error("pthread_create");
    //    return 0;
    //}

    //rc = pthread_join(send, NULL);
    //if (rc != 0) {
    //    print_error("pthread_join(): send thread");
    //    return 0;
    //}

    return 0;
}
