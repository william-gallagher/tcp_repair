// Thread 3, the receiver has no knowledge that the other side of the connection
// is being swapped back and forth between the two threads...


#include "tcp_repair.h"
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/tcp.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>

// TCP client side and file receiver
void recv_func(const char *dst_file)
{
    // create the socket
    int recv_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (recv_sock < 0) {
        print_error("socket()");
        return;
    }

    // allow reuse
    int opt = 1;
    int rc = setsockopt(recv_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    if (rc < 0) {
        print_error("setsockopt()");
        return;
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
                return;
            }
        } else {
            // Connected Successfully
            break;
        }
    }

    FILE *write_fp = fopen(dst_file, "w");
    if (!write_fp) {
        print_error("fopen()")
        return;
    }

    char recv_buf[100];

    // Read off socket and write to file
    while (1) {
        ssize_t bytes = read(recv_sock, recv_buf, sizeof(recv_buf));
        if (bytes < 0) {
            print_error("read()");
            return;
        } else if (bytes == 0) {
            break;
        } else {
            fwrite(recv_buf, 1, bytes, write_fp);
        }
    }

    fclose(write_fp);
    close(recv_sock);
    return;
}

int main()
{
    char *dst_file = "destination.txt";
    recv_func(dst_file);
    return 0;
}
