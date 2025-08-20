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


// TCP client side and file receiver
void * recv_func(void *args)
{
    char *dst_file = (char *) args;

    // create the socket
    int recv_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (recv_sock < 0) {
        print_error("socket()");
        pthread_exit(NULL);
    }

    // allow reuse
    int opt = 1;
    int rc = setsockopt(recv_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    if (rc < 0) {
        print_error("setsockopt()");
        pthread_exit(NULL);
    }

    // Connect out
    struct sockaddr_in s_in;
    memset(&s_in, 0, sizeof(s_in));
    s_in.sin_addr.s_addr = inet_addr(IP_ADR_STR);
    s_in.sin_family = AF_INET;
    s_in.sin_port = htobe16(PORT);

    // Spin until connect is successful
    while (1) {
        rc = connect(recv_sock, (struct sockaddr *) &s_in, sizeof(s_in));
        if (rc < 0) {
            if (errno != ECONNREFUSED) {
                print_error("connect()");
                pthread_exit(NULL);
            }
        } else {
            // Connected Successfully
            break;
        }
    }

    FILE *write_fp = fopen(dst_file, "w");
    if (!write_fp) {
        print_error("fopen()")
        pthread_exit(NULL);
    }

    char recv_buf[100];

    // Read off socket and write to file
    while (1) {
        ssize_t bytes = read(recv_sock, recv_buf, sizeof(recv_buf));
        if (bytes < 0) {
            print_error("read()");
            pthread_exit(NULL);
        } else if (bytes == 0) {
            break;
        } else {
            fwrite(recv_buf, 1, bytes, write_fp);
        }
    }

    fclose(write_fp);
    close(recv_sock);
    pthread_exit(NULL);
}
int main()
{
    char *dst_file = "destination.txt";

    pthread_t recv;
    int rc = pthread_create(&recv, NULL, recv_func, dst_file);
    if (rc != 0) {
        print_error("pthread_create");
        return 0;
    }

    rc = pthread_join(recv, NULL);
    if (rc != 0) {
        print_error("pthread_join(): recv thread");
        return 0;
    }

    return 0;
}
