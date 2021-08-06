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

 * 
 * the objective is to measure how much time to send a certain number of equal
 * size objects accross a TCP
 * We use it co compare with with the shared memory queue in shm_queue.c

 * How to build
 * =============
 * - Regular
 *   rm tcp_queue; gcc -o tcp_queue tcp_queue.c -lrt -pthread -Wall -Wextra
 * - With symbols
 *   rm tcp_queue; gcc -g -O0 -o tcp_queue tcp_queue.c -lrt -pthread -Wall -Wextra
 * How to run
 * ==========
 * - In one window do the server
 *     ./tcp_queue -t -o 20 -b 88 -n 1000000000 -p 65001
 * - In another window run the client
 *    ./tcp_queue -o 20 -b 88 -n 1000000000 -p 65001
 *
 * For the options, do
 *    ./tcp_queue -h
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
#include <assert.h>

#include <sys/mman.h>
#include <sys/stat.h>    /* For mode constants */
#include <fcntl.h>       /* For O_* constants */
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
#define DEFAULT_NUM_OBJS (1 << 20) /* 1M objects to send */
#define DEFAULT_OBJ_SIZE (sizeof(obj_t) + 30)
#define DEFAULT_SERVER_PORT (65000)
#define DEFAULT_NUM_OBJS_PER_BATCH (32)/* # packets to write or read in one shot */

#define MAX_NUM_RECEIVERS (1)
#define MIN_OBJ_SIZE (sizeof(obj_t))
#define MAX_OBJ_SIZE (1024)
#define MAX_NUM_OBJS (1 << 31)     /* 2 Billion objects to transmit */
#define MAX_NUM_OBJS_PER_BATCH (8192) /* At most 8k objects in a single write()*/

/*
 * Objects to be sent
 */
typedef struct obj_t_ {
  uint32_t counter; /* To catch any loss */
  uint8_t data[]; /* so that we can size it anyway we want */
} obj_t;

static bool is_transmitter = false;

/*
 * objects to bbe sent
 */
/*
 * memoryfor the sender to to send objects and receiver to receive objects
 */
static obj_t *obj;
static uint32_t obj_size = DEFAULT_OBJ_SIZE;
static uint32_t num_objs = DEFAULT_NUM_OBJS;
static uint16_t server_port = DEFAULT_SERVER_PORT;

/*
 * socket to accept receivers 
 */
static int server_accept_fd = -1;

static uint32_t packet_size = 0;
/* # packets for transmitter to write or receiver to read 
 * Because we are using TCP, if this value is greater than zero, then we will
 * - Allocate a single buffer the size of num_objs_per_batch*packet_size
 * - Copy as much objects in it as we can
 * - Transmitter Makes a single "write()" call to write it
 * - Recevier will continue to call reach until it reads this size
 */
static uint32_t num_objs_per_batch = DEFAULT_NUM_OBJS_PER_BATCH; 
/* the sender and receiver socket */
static int sock_fd;


/*
 * timestamps when connecting and after finishning
 */
struct timespec start_ts;
struct timeval start_tv;
struct timespec end_ts;
struct timeval end_tv;
struct timeval time_diff;


/*
 * counters
 */
uint32_t num_packets = 0; /* Number packets sent/received */
uint32_t num_sent_received_objs = 0; /* Number objects sent/received */


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



/*
 * print stats
 */
static void print_stats(char *string)
{
  PRINT_INFO("\n%s %s Statistics:\n"
             "\tTotal Time: %lu.%06lu\n"
             "\tnum_objs_per_batch = %u\n"
             "\tnum_packets = %u\n"
             "\tPacket size = %u\n"
             "\tobjs/packet = %u\n"
             "\tobj size = %u\n"
             "\tnum_sent_received_objs = %u\n",
             is_transmitter ? "Sender" : "Receiver",
             string,
             time_diff.tv_sec, time_diff.tv_usec,
             num_objs_per_batch,
             num_packets,
             packet_size,
             packet_size/obj_size,
             obj_size,
             num_sent_received_objs);
}

/* CTRL^C handler */
static void
signal_handler(int s __attribute__ ((unused))) 
{
  PRINT_DEBUG("Caught signal %d\n",s);


  
  PRINT_DEBUG( "Going close socket %d\n",
               sock_fd);
  
  close(sock_fd);
  close(server_accept_fd);
  print_stats("Signal Caught");
  exit(EXIT_SUCCESS); 
}

static void
print_usage (const char *progname)
{
  fprintf (stdout,
           "usage: %s [options] \n"
           "\t-t Transmitter mode instead of the default receiver mode\n"
           "\t-p <TCP-port>: user another port instead of default'%u'\n" 
           "\t-n <num_objs> # of objects to send/receive. Default %u\n"
           "\t-b <Number of objects to send/receive per batch>, default %u\n"
           "\t-o <object size>, default %u\n",
           progname,
           DEFAULT_SERVER_PORT,
           DEFAULT_NUM_OBJS,
           DEFAULT_NUM_OBJS_PER_BATCH,
           (uint32_t)DEFAULT_OBJ_SIZE);
}


int
main (int    argc,
      char **argv)
{
  uint32_t i;
  int opt;
  struct sockaddr_in sockaddr;
  socklen_t remote_len;
  uint8_t *packet;
  int ret_val = EXIT_SUCCESS;

  /* Setup handler for CTRL^C */
  struct sigaction sigIntHandler;
  sigIntHandler.sa_handler = signal_handler;
  sigemptyset(&sigIntHandler.sa_mask);
  sigIntHandler.sa_flags = 0;  
  if (sigaction(SIGINT, &sigIntHandler, NULL) != 0) {
    PRINT_ERR("Cannot setup signal handler: %d %s\n",
              errno, strerror(errno));
    ret_val = EXIT_FAILURE;
    goto out;
  }

  while ((opt = getopt (argc, argv, "aetp:r:n:q:b:o:")) != -1) {
    switch (opt)
      {
      case 'n':
        if (1 != sscanf(optarg, "%u", &num_objs)) {
          PRINT_ERR("\nCannot read number of objects %s. \n", optarg);
          exit (EXIT_FAILURE);
        }
        if (num_objs >= (uint32_t)MAX_NUM_OBJS || !num_objs) {
          PRINT_ERR("\nInvalid number of objects %s. "
                    "Must be between 1 and %u\n", optarg, MAX_NUM_OBJS);
          exit (EXIT_FAILURE);
        }
        break;
      case 't':
        is_transmitter = true;
        break;
      case 'p':
        if (1 != sscanf(optarg, "%u", (uint32_t *)(&server_port))) {
          PRINT_ERR("\nCannot read TCP port %s. \n", optarg);
          exit (EXIT_FAILURE);
        }
        if (server_port< 10000 ) {
          PRINT_ERR("\nInvalid server port %s. "
                    "Must be less than 1000\n", optarg);
          exit (EXIT_FAILURE);
        }
        break;
        
      case 'b':
        if (1 != sscanf(optarg, "%u", &num_objs_per_batch)) {
          PRINT_ERR("\nCannot read batch size %s. \n", optarg);
          exit (EXIT_FAILURE);
        }
        if (num_objs_per_batch < 1 || num_objs_per_batch > MAX_NUM_OBJS_PER_BATCH) {
          PRINT_ERR("\nInvalid num_objs_per_batch %s. "
                    "Must be between 0 and %u\n", optarg, MAX_NUM_OBJS_PER_BATCH);
          exit (EXIT_FAILURE);
        }
        break;
      case 'o': 
        if (1 != sscanf(optarg, "%u", &obj_size)) {
          PRINT_ERR("\nCannot read batch size %s. \n", optarg);
          exit (EXIT_FAILURE);
        }
        if (obj_size < MIN_OBJ_SIZE  || obj_size > MAX_OBJ_SIZE) {
          PRINT_ERR("\nInvalid obj_size %s. "
                    "Must be between %u and %u\n", optarg,
                    (uint32_t)MIN_OBJ_SIZE, (uint32_t)MAX_OBJ_SIZE);
          exit (EXIT_FAILURE);
        }
        break;
      default: /* '?' */
        print_usage(argv[0]);
        return EXIT_FAILURE;
      }
  }
  
  if (optind < argc) {
    PRINT_ERR("\nInvalid number of arguments optind=%d argc=%d.\n ",
              optind, argc);
    print_usage(argv[0]);
    exit (EXIT_FAILURE);
  }


  if (num_objs_per_batch > MAX_NUM_OBJS_PER_BATCH) {
    PRINT_ERR("\nBatch size %u cannot exceed  %u\n",
              num_objs_per_batch, MAX_NUM_OBJS_PER_BATCH);
    exit (EXIT_FAILURE);
  }

  /*
   * allocate memory for objects to be sent by sender and received by receiver
   */
  obj = malloc(obj_size);
  if (!obj) {
    PRINT_ERR("UUUnable to allocate %u bytes for object\n", obj_size);
    ret_val = EXIT_FAILURE;
    goto out;
  }
  memset(obj, 0, obj_size);
  /*
   * Allocate a single packet of the batch size is NOT zero
   * Otherwise we will be sending one object at a time
   */
  packet_size = num_objs_per_batch * obj_size;
  /* Number of objects to pack in a packet */
  packet = malloc(packet_size);
  if (!packet) {
    PRINT_ERR("UUnable to allocate %u bytes for object\n", packet_size);
    ret_val = EXIT_FAILURE;
    goto out;
  }

  /* Create the TCP socket. */
  sock_fd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sock_fd < 0) {
    PRINT_ERR("socket() failed: %d %s\n",
              errno, strerror(errno));
    ret_val = EXIT_FAILURE;
    goto out;
  }

  /* Construct local address structure */
  memset(&sockaddr, 0, sizeof(sockaddr));   /* Zero out structure */
  sockaddr.sin_family = AF_INET;                /* Internet address family */
  sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); /* Any incoming interface */
  sockaddr.sin_port = htons(server_port);      /* Local port */    
  remote_len = sizeof(sockaddr);

  if (is_transmitter) {
    
    /* Bind to the local address */
    if (bind(sock_fd, (struct sockaddr *) &sockaddr, sizeof(sockaddr)) < 0) {
      PRINT_ERR("Cannot bind socket %d Port %d (%d) : %d %s\n",
                sock_fd, server_port, sockaddr.sin_port,
                errno, strerror(errno));
      ret_val = EXIT_FAILURE;
      goto out;
    }
    /*
     * Put socket in listen mode
     */
    if (listen(sock_fd, MAX_NUM_RECEIVERS) == -1) {
      PRINT_ERR("listen() sock_fd %d backlong=%u "
                "failed : %d %s\n",
                sock_fd, MAX_NUM_RECEIVERS, errno, strerror(errno));
      ret_val = EXIT_FAILURE;
      goto out;
    }
    if ((server_accept_fd = accept(sock_fd,
                                      (struct sockaddr *)&sockaddr,
                                      &remote_len)) == -1) {
      PRINT_ERR("accept() sock_fd %d remote_len=%u "
                "failed : %d %s\n",
                sock_fd, remote_len, errno, strerror(errno));
      ret_val = EXIT_FAILURE;
      goto out;
    } else {
      PRINT_INFO("Success accept() sock_fd %d accept_fd %u remote_len=%u\n",
                sock_fd, remote_len, server_accept_fd);
    }

    /* get time stamp when stazrting to send */
    clock_gettime(CLOCK_MONOTONIC, &start_ts);

    /*
     * Loop until all objects are sent
     */
    while(num_sent_received_objs < num_objs) {
      /*
       * Copy objects into the packet
       */
      uint8_t *temp_packet = packet;
      for (i = 0; i < num_objs_per_batch; i++) {
        memcpy(temp_packet, obj, obj_size);
        obj->counter++;
        temp_packet += obj_size;
      }
      /* Send the apcket */
      ssize_t j = send(server_accept_fd, packet, packet_size, 0);
      if (j != (ssize_t)packet_size) {
        PRINT_ERR("Sent INVALID size %d packet_size=%u num_objs_per_batch=%u \n"
                  "\tnum_sent_received_objs=%u num_packets=%u: %d %s\n", (int32_t)j,
                  packet_size, num_objs_per_batch,
                  num_sent_received_objs, num_packets,
                  errno, strerror(errno));
        ret_val = EXIT_FAILURE;
        goto out;
      }
      num_sent_received_objs += num_objs_per_batch;
      num_packets++;
    }
    /* get time stamp when finish sending */
    clock_gettime(CLOCK_MONOTONIC, &end_ts);

    PRINT_DEBUG("So far sent %u packets, %u objects\n",
                num_packets, num_sent_received_objs);
  }

  if (!is_transmitter)  {
    uint32_t counter = 0;
    if (connect(sock_fd,
                (struct sockaddr *)&sockaddr,
                remote_len)){
      PRINT_ERR("FAILED connect() sock_fd %d remote_len=%u: %d %s\n",
                sock_fd, remote_len, errno, strerror(errno));
      ret_val = EXIT_FAILURE;
      goto out;
    }

    /* get time stamp when start receiving */
    clock_gettime(CLOCK_MONOTONIC, &start_ts);
    
    while(num_sent_received_objs < num_objs) {
      ssize_t j = recv(sock_fd, packet, packet_size, MSG_WAITALL);
      if (j < 0) {
        PRINT_ERR("cannot receiver size %d packet_size=%u num_objs_per_batch=%u \n"
                  "\tnum_sent_received_objs=%u num_packets=%u: %d %s\n", (int32_t)j,
                  packet_size, num_objs_per_batch,
                  num_sent_received_objs, num_packets,
                  errno, strerror(errno));
        break;
      }
      if (j != (ssize_t)packet_size) {
        PRINT_INFO("Receiver unexpcted size %d packet_size=%u num_objs_per_batch=%u \n"
                  "\tnum_sent_received_objs=%u num_packets=%u\n", (int32_t)j,
                  packet_size, num_objs_per_batch,
                  num_sent_received_objs, num_packets);
        num_sent_received_objs += j/obj_size;
        num_packets++;
        break;
      } else {
        num_sent_received_objs += num_objs_per_batch;
        num_packets++;
      }
      uint8_t *temp_packet = packet;
      for (i = 0; i < num_objs_per_batch; i++) {
        memcpy(obj, temp_packet, obj_size);
        temp_packet += obj_size;
        if (counter != obj->counter) {
          PRINT_DEBUG("Received invalid counter %u expecting %u\n",
                      obj->counter, counter);
          ret_val = EXIT_FAILURE;
          goto out;
        }
        counter++;
      }
      PRINT_DEBUG("So far received %u packets, %u objects\n",
                  num_packets, num_sent_received_objs);
    }
    /* get time stamp when finish receiving */
    clock_gettime(CLOCK_MONOTONIC, &end_ts);
  }
  /*
   * convert timers to timval then use timersub to subtract them
   */
  TIMESPEC_TO_TIMEVAL(&start_tv, &start_ts);
  TIMESPEC_TO_TIMEVAL(&end_tv, &end_ts);
  timersub(&end_tv, &start_tv, &time_diff);

  
 out:
  print_stats("Exitting");

  close(sock_fd);
  close(server_accept_fd);

  exit(ret_val);
}
