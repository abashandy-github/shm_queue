/*********************************************************
 *
 * ISC License  (ISC)
 * 
 * Copyright (c) 2019-2021 Ahmed Bashandy
 * 
 * Permission to use, copy, modify, and/or distribute this software and/or text
 * in this file for any purpose with or without fee is hereby granted, provided
 * that the above copyright notice and this permission notice appear in all
 * copies.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(s) DISCLAIM ALL 
 * WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE 
 * AUTHOR(s) BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR 
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM 
 * LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, 
 * NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN      
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE. 
 */
#include <errno.h>
#include <locale.h>
#include <signal.h>
#include <stdio.h>
#include <stdarg.h> /* for va_start, va_end, va_arg,..., etc */
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <sys/time.h> /* for  timersub*/
#include <time.h>
#include <syslog.h>
#include <stdint.h>
#include <stddef.h> /* for offsetof() */
#include <assert.h>

#include <sys/mman.h>
#include <sys/stat.h>    /* For mode constants */
#include <fcntl.h>       /* For fcntl() and  O_* constants */
#include <unistd.h>      /* for unlink() */
#include <errno.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>
#include <sys/types.h>   /* AF_UNIX socket */
#include <sys/socket.h>  /* AF_UNIX socket */
/*#include <sys/un.h>*/  /* AF_UNIX socket but does NOT have "UNIX_PATH_MAX"*/
#include <linux/un.h>    /* AF_UNIX socket and UNIX_PATH_MAX */
#include <sys/eventfd.h>  /* for eventfd() */
#include <poll.h> /* For poll() */
#include <sys/epoll.h> /* For epoll() */
#include <pthread.h>   /* For all pthread_.. and PTHREAD_... */



#ifndef TIMESPEC_TO_TIMEVAL
/* should be in sys/time.h on BSD & Linux libcs */
#define TIMESPEC_TO_TIMEVAL(tv, ts)                                            \
	do {                                                                   \
		(tv)->tv_sec = (ts)->tv_sec;                                   \
		(tv)->tv_usec = (ts)->tv_nsec / 1000;                          \
	} while (0)
#endif
#ifndef TIMEVAL_TO_TIMESPEC
/* should be in sys/time.h on BSD & Linux libcs */
#define TIMEVAL_TO_TIMESPEC(tv, ts)                                            \
	do {                                                                   \
		(ts)->tv_sec = (tv)->tv_sec;                                   \
		(ts)->tv_nsec = (tv)->tv_usec * 1000;                          \
	} while (0)
#endif


/* colors */
#define COLOR_NORMAL   "\x1B[0m"
#define COLOR_RED   "\x1B[31m"
#define COLOR_GREEN   "\x1B[32m"
#define COLOR_YELLOW   "\x1B[33m"
#define COLOR_BLUE   "\x1B[34m"
#define COLOR_MAGENTA   "\x1B[35m"
#define COLOR_CYAN   "\x1B[36m"
#define COLOR_WIHTE   "\x1B[37m"
#define COLOR_RESET "\x1B[0m"


/* default and max data item size */
#define DEFAULT_QUEUE_NAME "shm_queue"
#define DEFAULT_NUM_OBJS (1 << 20) /* 1M objects to send */
#define DEFAULT_OBJ_SIZE (sizeof(obj_t) + 30)
#define DEFAULT_NUM_OBJS_PER_BATCH (32)/* # packets to write/read in one shot */
#define DEFAULT_SLOW_FACTOR (0) /* Looping to slow sender or receiver down */
#define DEFAULT_NUM_RECEIVERS (1) /* default number of receivers */
#define DEFAULT_OBJ_SIZE (sizeof(obj_t) + 30)

#define MAX_NUM_RECEIVERS (4)
#define MIN_OBJ_SIZE (sizeof(obj_t))
#define MAX_OBJ_SIZE (1024)
#define MAX_NUM_OBJS (1 << 31)     /* 2 Billion objects to transmit */
#define MAX_NUM_OBJS_PER_BATCH (8192) /* At most 8k objects in a single write()*/

/*
 * TCP communications
 */
#define DEFAULT_TCP_PORT (65000)
#define MIN_TCP_PORT (10000)

/*
 * send a single pulse to receiver
 */
#define PULSE_PEER(__pulse, __peer_fd, __use_eventfd)                   \
do {                                                                    \
  ssize_t rc;                                                           \
  uint64_t eventfd_write = (uint64_t)(__pulse);;                        \
  rc = write(__peer_fd, &eventfd_write,                                 \
             __use_eventfd ? sizeof(eventfd_write) : sizeof(uint8_t));  \
  if (((__use_eventfd) ? rc !=  sizeof(uint64_t) : rc !=  sizeof(uint8_t))) { \
    PRINT_ERR("Cannot write message %lu size %zu to peer_wakeup fd %d\n" \
              "\tSo far sent/received %d objects %u packets\n"          \
              "\tpacket=(%s): %d %s\n",                                 \
              __use_eventfd ? eventfd_write : (uint64_t)(__pulse),      \
              __use_eventfd ? sizeof(uint64_t) : sizeof(uint8_t),       \
              __peer_fd,                                                \
              num_sent_received_objs,                                   \
              num_packets,                                              \
              print_packet(packet),                                     \
              errno, strerror(errno));                                  \
    ret_val = EXIT_FAILURE;                                             \
    goto out;                                                           \
  } else {                                                              \
    PRINT_DEBUG("SUCCESSFULLY pulsed %lu size %zu to peer_wakeup fd %d.\n " \
                "\tSo far sent %d objects %u packets %u batchs "        \
                "curr_batch_size %d\n",                                 \
                __use_eventfd ? eventfd_write : (uint64_t)(__pulse),    \
                __use_eventfd ? sizeof(uint64_t) : sizeof(uint8_t),     \
                __peer_fd,                                              \
                num_sent_received_objs,                                 \
                num_packets,                                            \
                curr_batch_size);                                       \
  }                                                                     \
} while (false)



/*
 * Objects to be sent
 */
typedef struct obj_t_ {
  uint32_t counter; /* To catch any loss */
  uint8_t data[]; /* so that we can size it anyway we want */
} obj_t;








/* Macro to print error */
#define PRINT_ERR(str, param...)                                        \
  do {                                                                  \
    print_error("%s %d: " str, __FUNCTION__, __LINE__, ##param);        \
  } while (false);
#define PRINT_INFO(str, param...)                                       \
  do {                                                                  \
    print_debug("%s %d: " str, __FUNCTION__, __LINE__, ##param);        \
  } while (false);


#ifdef DEBUGENABLE
#define PRINT_DEBUG(str, param...)                                      \
  do {                                                                  \
    print_debug("%s %d: " str, __FUNCTION__, __LINE__, ##param);        \
  } while (false);
#else
#define PRINT_DEBUG(str, param...)
 #endif


/* Print error in read color */
__attribute__ ((format (printf, 1, 2), unused))
static void print_error(char *string, ...)
{
  va_list args;
  va_start(args, string);
  fprintf(stdout, COLOR_RED);
  vfprintf(stdout, string, args);
  fprintf(stdout, COLOR_RESET);
  va_end(args);
}

/* Print error in info or debug in normal color */
__attribute__ ((format (printf, 1, 2), unused))
static void print_debug(char *string, ...)
{
  va_list args;
  va_start(args, string);
  fprintf(stdout, COLOR_RESET);
  vfprintf(stdout, string, args);
  va_end(args);
}

static  bool  __attribute__ ((unused))
is_power_of_two(uint32_t num)
{
  uint32_t num_ones = 0, i;

  if (num & 0x1) {
    return (false);
  }
  /* We treat zero as NOT power of two? */
  if (!num) {
    return (false);
  }

  for (i = 0; i < sizeof(num) << 3; i++) {
    num = num >> 1; /* Shift right */
    num_ones += (num & 0x1);
  }
  return  (!(num_ones > 1));
}


/*
 * Prototypes
 */
int
send_receive_eventfd(bool is_transmitter,
                     bool is_use_eventfd,
                     int *sender_wakekup_fd_p,
                     int *receiver_wakekup_fd_p,
                     int *epoll_fd_p,
                     int *sock_fd_p,
                     int *server_accept_fd,                     
                     char *sock_path,
                     uint32_t num_receivers);
