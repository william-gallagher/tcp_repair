#ifndef TCP_REPAIR_H
#define TCP_REPAIR_H

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

#endif // TCP_REPAIR_H
