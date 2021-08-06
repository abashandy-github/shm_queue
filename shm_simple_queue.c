/*********************************************************
 *
 * ISC License  (ISC)
 * 
 * Copyright (c) 2021 Ahmed Bashandy
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

 * What is this
 * This is a VERY simple shared memory queue that uses either eventfd or the
 * AF_UNIX socket for signalling
 * 1 Sender creates two eventfds : receiver_eventfd and sender_eventfd
 * 2 Sender also creates a single shared memory window
 * 3 Receiver starts 
 *  - Connects to sender on AF_UNIX socket
 *  - receives both sernder_eventfd and receiver_eventfd
 *  - while loop over epoll_wait() on receiver_eventfd or the AF_UNIX socket
 *    which  it used to connect to the sender 
 * 4 When receiver connects to sender , sender
 *   - Fills up the shared memory window with a single batch of objects
 *   - write(receiver_eventfd) or the AF_UNIX socket
 *   - epoll_wait on sender_eventfd or the AF_UNIX 
 * 5 Receiver comes out of epoll_wait() on receiver_eventfd or the AF_UNIX
 *   socket because of the sender's pulse
 * - A pulse is a write to the recevier_eventfd or the AF_UNIX socket
 * - Reads all objects in the shared memory window
 * - write(sender_eventfd)
 * - Goes back to the beginning of the while loop on epoll_wait() on
 *   receiver_eventfd 
 * 6 Sender comes out of the epoll_wait on sender_eventfd Or the AF_UNIX socket
 *   due to the pulse from  the receiver
 *   - Goes to step 4
 * 
 * The nice thing about that is 
 * - VERY SIMPLE
 * - No mutexes
 * - Very small amount of memory, which is the shared memory window
 *   If we expect objects to be of the size 20-50-100 bytes, then probably 16k
 *   or 32k shared memory window would be more than sufficient
 * Disadvantage
 * - Writing by the sender and reading by the receiver are SERIALIZED
 *   I.e. the sender has to wait for the receiver to finish and vice versa
 * - Quite slow. For example, when compared with a TCP connection between
 *   sendewr and receiver, it takes about 3 times the time for the same packet
 *   size, object size, and number of objects

 * How to build
 * ============
 * Use
 *    gcc -o shm_simple_queue shm_simple_queue.c shm_common.c -lrt -pthread -Wall -Wextra
 * OR (if you want symbols)
 *   gcc -g -O0 -o shm_simple_queue shm_simple_queue.c shm_common.c -lrt -pthread -Wall -Wextra
 *
 * - to log a message for each sent and received packet, use -DLOG_PER_PACKET
 *      gcc -DLOG_PER_PACKET -o shm_simple_queue shm_simple_queue.c shm_common.c -lrt -pthread -Wall -Wextra
 * OR (if you want symbols)
 *      gcc -DLOG_PER_PACKET -g -O0 -o shm_simple_queue shm_simple_queue.c shm_common.c -lrt -pthread -Wall -Wextra
 *
 * How to use
 * ============
 * - In one window start the sehnder using "-t" option. You can change default
 *   arguments. For example
 *     ./shm_simple_queue  -t -n 1000000000 -b 2000 -o 50
 * - In another window, start the receiver using  
 *       ./shm_simple_queue -o 50
 *
 * - If you want to use AF_UNIX instead of eventfd for signaling, add the
 *    option "-u" to the command line on both sender and receiver
 *
 * This will cause the sender to send 1 billion objects, each of size 50, in
 * a batch of 2000 objects each.  
 * Hence we will create a single packet shared memory window each of size
 * 2000x50 = 10,000 bytes plus the sizeof(packet_t), which is 16. So the
 * total shared memory window size will be 10,016 bytes
 * 
 */

#include "shm_common.h"



/*
 * Other constants
 */
#define MIN_OBJ_SIZE (sizeof(obj_t))


/* Default values for the samred memory queue*/
#define DEFAULT_PACKET_SIZE  (2048) /* Size of a single buffer inthe ring */
#define DEFAULT_QUEUE_LEN (256) /* Number of buffers in the circular ring  */

/* Max values */
#define MAX_PACKET_SIZE  (1048576 + sizeof(packet_t)) 
#define MAX_NUM_OBJS (1 << 31) /* 2 Billion objects to transmit */
#define MAX_QUEUE_LEN (256) /* Limited to 256 because of a bug */
#define MAX_QUEUE_NAME_LEN 256
#define MAX_BATCH_SIZE (MAX_PACKET_SIZE/MIN_OBJ_SIZE)
#define MAX_NUM_RECEIVERS (4)

/*
 * Default named socket (AF_UNIX) path
 */
#define DEFAULT_SOCK_PATH "/tmp/test_named_socket_path"

/*
 * Single packet
 */
typedef struct packet_t_ {
  uint32_t obj_size; /* the size in bytes of each object */
  uint32_t data_size; /* the entire size in bytes of the payload */
  uint32_t sequence; /* Sequence nmumber of  that packet */
  uint32_t num_objs; /* Number of objects packed in that packet */
  uint8_t data[];
} packet_t;


static char *
print_packet(packet_t *packet)
{
  static char buf[1025];
  memset(buf, 0, sizeof(buf));
  snprintf(buf, 1024, "obj_size=%u seq=%u num_objs=%u data_size=%u packet=%p",
           packet->obj_size,
           packet->sequence,
           packet->num_objs,
           packet->data_size,
           packet);
  return (buf);
}


/*
 * whether to use eventfd vs the AF_UNIX socket for signalling
 */
static bool is_use_eventfd = true;

/*
 * the packet that is uses to carry obkjects
 */
static packet_t *packet;

/*
 * timestamps when starcting and after finishning
 */
struct timespec start_ts;
struct timeval start_tv;
struct timespec end_ts;
struct timeval end_tv;
struct timeval time_diff;

/*
 * objects to be sent
 */
static uint32_t packet_size = 0;
static obj_t *obj;
uint32_t obj_size = DEFAULT_OBJ_SIZE;
static uint32_t num_objs = DEFAULT_NUM_OBJS;
static uint32_t num_objs_per_batch = DEFAULT_NUM_OBJS_PER_BATCH;

/* Need these to be public so that the signal handler can use them */
static char *sock_path = DEFAULT_SOCK_PATH;
static int server_accept_fd[MAX_NUM_RECEIVERS];
static int sock_fd;
static int epoll_fd;


/*
 * the name of the shared memory window
 */
static char queue_name[MAX_QUEUE_NAME_LEN + 1];

/* When sender finishes packeingobjects, the sender will epoll on this FD until
   the receiver dequeues all objects from the shared memory queue and writes to
   this eventfd */
static int sender_wakeup_eventfd_fd = -1;
/* Receiver will wait on this eventfd until the sender packs a batch of
   objects in the shared memory window */
static int receiver_wakeup_eventfd_fd = -1;


static bool is_transmitter = false;
static uint32_t num_receivers = DEFAULT_NUM_RECEIVERS;

/*
 * Vehicle to send
 */
static uint32_t packet_size; /* Entire packet size, including header */
static uint32_t data_size; /* Maximum payload size */
static uint32_t window_size; /* Total size of shared memory window 
                       For our case, it is also the packet size*/

/*
 * slow factor
 */
static uint32_t slow_factor = DEFAULT_SLOW_FACTOR;



/*
 * counters
 */
static uint32_t num_packets = 0; /* Number packets sent/received */
static uint32_t num_sent_received_objs = 0; /* Number objects sent/received */

             
/*
 * print stats
 */
static void print_stats(char *string)
{
  PRINT_INFO("\n%s %s Statistics:\n"
             "\tSingalling using %s\n"
             "\tTotal Time: %lu.%06lu\n"
             "\tMax objects per batch = %u\n"
             "\tMax batch_size in bytes = %u\n"
             "\tnum_packets = %u\n"
             "\tMax Packet payload size = %u\n"
             "\tobjs/packet = %u\n"
             "\tobj size = %u\n"
             "\tnum_sent_received_objs = %u\n"
             "\tSlow factor = %u\n",
             is_transmitter ? "Sender" : "Receiver",
             string,
             is_use_eventfd ? "eventfd" : "AF_UNIX socket",
             time_diff.tv_sec, time_diff.tv_usec,
             num_objs_per_batch,
             num_objs_per_batch * obj_size ,
             num_packets,
             packet->data_size,
             packet->data_size/packet->obj_size,
             packet->obj_size,
             num_sent_received_objs,
             slow_factor);
}



static void
close_fds(void)
{
  for (int i = 0; i < MAX_NUM_RECEIVERS; i++) {
    close(server_accept_fd[i]);
  }
    close(sock_fd);
  close(sender_wakeup_eventfd_fd);
  close(receiver_wakeup_eventfd_fd);
}

/* CTRL^C handler */
static void
signal_handler(int s __attribute__ ((unused))) 
{
  /* get time stamp after getting the signal because we will exit */
  clock_gettime(CLOCK_MONOTONIC, &end_ts);
  /*
   * convert timers to timval then use timersub to subtract them
   */
  TIMESPEC_TO_TIMEVAL(&start_tv, &start_ts);
  TIMESPEC_TO_TIMEVAL(&end_tv, &end_ts);
  timersub(&end_tv, &start_tv, &time_diff);
  
  PRINT_DEBUG("Caught signal %d\n",s);

  PRINT_DEBUG( "Going close socket %d %d %d %d\n",
               sock_fd);
  print_stats("Got Signal");  
  close_fds();
  exit(EXIT_SUCCESS); 
}


#define READ_PULSE(__use_eventfd)                       \
do {                                                    \
  uint64_t fd_read;                                                     \
  rc = (int)read(event.data.fd, &fd_read,                               \
                 __use_eventfd ? sizeof(uint64_t) : sizeof(uint8_t));    \
  if (((__use_eventfd) ?                                                \
       (ssize_t)rc !=  sizeof(uint64_t) :                               \
       (ssize_t)rc !=  sizeof(uint8_t))) {                              \
    PRINT_ERR("read %zu bytes failed for epoll_fd %d %s_wakeup_fd %d  "\
              "(%s socket) packet=(%s): %d %s\n",                       \
              __use_eventfd ? sizeof(uint64_t) : sizeof(uint8_t),       \
              epoll_fd,                                                 \
              is_transmitter ? "sender" : "receiver",                    \
              event.data.fd,                                            \
              __use_eventfd ? "eventfd" : "AF_UNIX",                    \
              print_packet(packet), errno, strerror(errno));            \
    ret_val = EXIT_FAILURE;                                             \
    goto out;                                                           \
  }                                                                     \
 } while (false);

static void
print_usage (const char *progname)
{
  fprintf (stdout,
           "usage: %s [options] \n"
           "\t-t Transmitter mode instead of the default receiver mode\n"
           "\t-u Use AF_UNIX socket for singalling instead of default eventfd\n"
           "\t-n <num_objs> # of objects to send/receive. Default %u\n"
           "\t-b <Number of objects to send/receive per batch>, default %u\n"
           "\t-o <object size>, default %u\n"
           "\t-s <slow_factor> default %u\n"
           "\t-q <queue_name>. Default '%s'\n", 
           progname,
           DEFAULT_NUM_OBJS,
           DEFAULT_NUM_OBJS_PER_BATCH,
           (uint32_t)DEFAULT_OBJ_SIZE,
           DEFAULT_SLOW_FACTOR,
           DEFAULT_QUEUE_NAME);
}


int
main (int    argc,
      char **argv)
{
  uint32_t i;
  int opt;
  int ret_val = EXIT_SUCCESS;
  int shm_fd;
  struct epoll_event event;
  int rc;

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

  /* Init queueu name */
  memset(queue_name, 0, sizeof(queue_name));
  strcpy(queue_name, DEFAULT_QUEUE_NAME);
  
  while ((opt = getopt (argc, argv, "utr:n:q:b:o:s:S:q:")) != -1) {
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
      case 'u':
        is_use_eventfd = false;
        break;
      case 'S':
        sock_path = optarg;
        if (strlen(sock_path) >= UNIX_PATH_MAX) {
          PRINT_ERR("\nSocket path '%s' len = %u. MUST be less than %u\n",
                    sock_path,
                    (uint32_t)strlen(sock_path), UNIX_PATH_MAX)
          exit (EXIT_FAILURE);
        }                    
        break;

      case 'q':
        if (strlen(optarg) > MAX_QUEUE_NAME_LEN || !strlen(optarg)) {
          PRINT_ERR("\nInvalid queue name '%s'. "
                    "Must be between 0 and %u\n", optarg, MAX_QUEUE_NAME_LEN);
          exit (EXIT_FAILURE);
        }
        /*
         * Only contain letters
         */
        for (i = 0; i < strlen(optarg); i++) {
          if (optarg[i] < 'a' || optarg[i] > 'Z') {
            PRINT_ERR("\nInvalid %uth char '%c' in queue name '%s'\n", i,
                      optarg[i], optarg);        
            exit (EXIT_FAILURE);
          }
        }
        strcpy(queue_name, optarg);
        break;
      case 's':
        if (1 != sscanf(optarg, "%u", (uint32_t *)(&slow_factor))) {
          PRINT_ERR("\nCannot read Slow factor %s. \n", optarg);
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
                    "Must be between 1 and %u\n", optarg,
                    MAX_NUM_OBJS_PER_BATCH);
          exit (EXIT_FAILURE);
        }
        break;
      case 'o': 
        if (1 != sscanf(optarg, "%u", &obj_size)) {
          PRINT_ERR("\nCannot read object size %s. \n", optarg);
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


  packet_size = num_objs_per_batch * obj_size + sizeof(packet_t);
  if (packet_size > MAX_PACKET_SIZE) {
    PRINT_ERR("packet_size %u cannot exceed  %u\n",
              (uint32_t)packet_size, (uint32_t)MAX_PACKET_SIZE);
    exit (EXIT_FAILURE);
  }

  /* For this particular implemenation, the window size is just one packet */
  window_size = packet_size;



  
  /*
   * Create (for sender) or open (for receiver) shared memory window
   */
   if (is_transmitter) {
    /*
     * delete the shared memory queue filename
     */
    
    if (shm_unlink(queue_name))      {
      if (errno != ENOENT) {
        PRINT_ERR("CANNOT unlink '%s': %d %s\n",
                  queue_name, errno, strerror(errno));
        exit(EXIT_FAILURE);
      } else {
        PRINT_INFO("Queue %s does not exist\n", queue_name);;
      }
    } else {
      PRINT_INFO("Successfully delete Queue '%s' \n", queue_name);;
      }
    
    /*
     * Create, size, and map the shared memory queue
     */
    shm_fd = shm_open(queue_name, O_RDWR | O_CREAT,
                      S_IRWXO | S_IRWXG | S_IRWXU);
    if (shm_fd < 0) {
      PRINT_ERR( "\nCannot open '%s': %d %s\n", queue_name,
                 errno, strerror(errno));
      
      return EXIT_FAILURE;
    }
    rc =  posix_fallocate(shm_fd, 0, window_size);
    if (rc) {
      PRINT_ERR("\nCannot size '%s' to %u bytes: %d %s", queue_name,
                window_size,
                rc, strerror(rc));
      return EXIT_FAILURE;
    }

    /*
     * Map the queue
     */
    packet = mmap(0,
                  window_size,
                 PROT_READ | PROT_WRITE,
                 MAP_SHARED, /* MUST have for file-backed window */
                 shm_fd, 0);
    if (!packet) {
      PRINT_ERR( "\nCannot map '%s' %u bytes shm_fd %d: %d %s", queue_name,
                 window_size, shm_fd,
                 errno, strerror(errno));
      return EXIT_FAILURE;
    }
    /*
     * Init the queue
     */
    memset(packet, 0, window_size);
    packet->obj_size = obj_size;
    packet->data_size = (packet_size - sizeof(packet_t));
    
    PRINT_DEBUG("Successfully created queue at %p with %u elements each size %u "
                "total size %u\n",
                packet, packet_size,
                window_size);
  } else {
    /*
     * We are receiver. Hence we only open an EXISTING window and just map it
     * Sender should have already done the creation and init
     */
    shm_fd = shm_open(queue_name, O_RDWR,
                      S_IRWXO | S_IRWXG | S_IRWXU);
    if (shm_fd < 0) {
      PRINT_ERR( "\nCannot open '%s': %d %s\n", queue_name,
                 errno, strerror(errno));
      return EXIT_FAILURE;
    }
    packet = mmap(0,
                  sizeof(packet_t),
                 PROT_READ | PROT_WRITE,
                 MAP_SHARED, /* MUST have for file-backed window */
                 shm_fd, 0);
    if (!packet) {
      PRINT_ERR( "\nCannot map '%s' %zu bytes shm_fd %d: %d %s", queue_name,
                 sizeof(packet_t), shm_fd,
                 errno, strerror(errno));
      return EXIT_FAILURE;
    }
    if (packet->obj_size != obj_size) {
      PRINT_ERR( "Invalid objsize %u expecting %u  '%s' %u bytes shm_fd %d: %d %s\n",
                 packet->obj_size, obj_size,
                 queue_name,
                 window_size, shm_fd,
                 errno, strerror(errno));
      return EXIT_FAILURE;
    }
    /*
     * get queue parameters
     */
    obj_size = packet->obj_size;
    data_size = packet->data_size;
    packet_size = data_size + sizeof(*packet);
    window_size = data_size + sizeof(*packet);

    /*
     * Unmap it then remap it with the "window_size" that we got
     */
    if (munmap(packet, sizeof(packet_t)) < 0) {
      PRINT_ERR("Cannot unmap %p size %zu shm_fd %d name '%s': %d %s\n",
                packet, sizeof(packet_t), shm_fd,
                queue_name,
                errno, strerror(errno));
      return EXIT_FAILURE;
    }
      
    packet = mmap(0,
                 window_size,
                 PROT_READ | PROT_WRITE,
                 MAP_SHARED, /* MUST have for file-backed window */
                 shm_fd, 0);
    if (!packet) {
      PRINT_ERR( "\nCannot map '%s' %u bytes shm_fd %d: %d %s", queue_name,
                 window_size, shm_fd,
                 errno, strerror(errno));
      return EXIT_FAILURE;
    }
    
  }
 

  /*
   * Init the array of receiver file descriptors
   */
  for (i = 0; i < MAX_NUM_RECEIVERS; i++) {
    server_accept_fd[i] = -1;
  }

  /* Create the AF_UNIX socket. */
  sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (sock_fd < 0) {
    PRINT_ERR("socket() failed: %d %s\n",
              errno, strerror(errno));
    exit(EXIT_FAILURE);
  }

  /*  epoll on the sender_wakeup_eventfd_fd and receiver_wakeup eventfd */
  epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    PRINT_ERR("epoll_create() failed : %d %s\n",
              errno, strerror(errno));
    exit(EXIT_FAILURE);
  } else {
    PRINT_DEBUG("Successfully called epoll_create() epoll_fd=%d \n",
                epoll_fd);
  }


  /*
   * allocate memory for objects to be sent by sender and received by receiver
   */
  obj = malloc(obj_size);
  if (!obj) {
    PRINT_ERR("UUUnable to allocate %u bytes for object\n", obj_size);
    exit(EXIT_FAILURE);
  }
  memset(obj, 0xee, obj_size);
  memset(obj->data, 0xee, (obj_size - offsetof(obj_t, data)));
  obj->counter = 0;


  num_objs_per_batch = packet->data_size/obj_size;

  /*
   * Connect the server to the destination, creae eventfds and epoll_fd, exchange them, addand
   * get back all thes file descriptors
   */

  if (send_receive_eventfd(is_transmitter,
                           is_use_eventfd,
                           &sender_wakeup_eventfd_fd,
                           &receiver_wakeup_eventfd_fd,
                           &epoll_fd,
                           &sock_fd,
                           server_accept_fd,
                           sock_path,
                           num_receivers) < 0) {
    ret_val = EXIT_FAILURE;
    goto out;
  }

  /*
   * If I am a transmitter, let me go through the transmission loop
   */
  if (is_transmitter) {
    /* get time stamp when starting to send */
    clock_gettime(CLOCK_MONOTONIC, &start_ts);

    uint32_t sequence = 0;
    while (num_objs > num_sent_received_objs) {
      /* adjust number of objects to pack according to remaining ones */
      uint32_t curr_batch_size = num_objs_per_batch;
      if (curr_batch_size > num_objs - num_sent_received_objs) {
        curr_batch_size = num_objs - num_sent_received_objs;
      }
      /* store sequence */
      packet->sequence = sequence;
      uint8_t *data = packet->data;
      for (i = 0; i < curr_batch_size; i++) {
        memcpy(data, obj, obj_size);
        data = (uint8_t *)((uintptr_t)data + (uintptr_t) obj_size);
        obj->counter++;
      }
      /* Store number of obgjects in that packet */
      packet->num_objs = curr_batch_size;
      /* Inc other stats */
      num_packets++; /* NMumber of packets sent so far */
      /* Inc sequence number of packet */
      sequence++;
      
      
      
      /* Slow down if required */
      for (i = 0; i < slow_factor; i++) {
        uint32_t __attribute__ ((unused)) a;
        uint32_t b = 5000, c = 129;
        a = b *c;
      }
      
      /* increment number of sent objects */
      num_sent_received_objs += curr_batch_size;
      
      /* Now pulse the receiver */
      if (is_use_eventfd) {
        PULSE_PEER(num_receivers, receiver_wakeup_eventfd_fd, is_use_eventfd);
      } else {
        for (i = 0; i < num_receivers; i++) {
          PULSE_PEER(num_receivers, server_accept_fd[i], is_use_eventfd);
        }
      }

      /*
       * Let's wait until receiver(s) read what we have sent
       */
      if ((rc = epoll_wait(epoll_fd, &event, 1, -1)) < 0) {
        PRINT_ERR("epoll_wait failed for epoll_fd %d sender_evenfd %d "
                  "packet=(%s): %d %s\n",
                  epoll_fd,  sender_wakeup_eventfd_fd,
                  print_packet(packet), errno, strerror(errno));
        ret_val = EXIT_FAILURE;
        goto out;
      }
      /* since we may be using one of the server_accept_fd[i] or
       * sender_wakeup_eventfd_fd and we do not know which one, we will just
       * read from the file descriptor returned in the "event" argment used in
       * epoll_wait() 
       */
      READ_PULSE(is_use_eventfd);
#ifdef LOG_PER_PACKET
      PRINT_INFO("Sent %u packets and %u objects\n",
                 packet->sequence, num_sent_received_objs);
#endif
    }
    /* get time stamp after completing all sending */
    clock_gettime(CLOCK_MONOTONIC, &end_ts);
  }
        
  /*
   * If receiver, loop until sender exits
   * NOte that we have already setup the epoll_fd and added sock_fd to it
   */
  if (!is_transmitter)  {
    uint32_t counter = 0;
    uint32_t sequence = 0;
    
    /* get time stamp when start receiving */
    clock_gettime(CLOCK_MONOTONIC, &start_ts);
    

    while ((rc = epoll_wait(epoll_fd, &event, 1, -1)) >= 0) {
      /*
       * If we got EPOLLRDHUP, then the sender has exited. So we will exit
       */
      if (event.events & EPOLLRDHUP) {
        PRINT_INFO("Sender exited\n");
        ret_val = EXIT_SUCCESS;
        break;
      }
      READ_PULSE(is_use_eventfd);
#ifdef LOG_PER_PACKET
      PRINT_INFO("Sent %u packets and %u objects\n",
                 packet->sequence, num_sent_received_objs);
#endif
      if (packet->sequence != sequence) {
        PRINT_ERR("Received invalid packet %p sequence %u expecting %u\n",
                  packet,
                  packet->sequence, sequence);
        ret_val = EXIT_FAILURE;
        goto out;
      }
      uint8_t *data = packet->data;
      for (i = 0; i < packet->num_objs; i++) {
        memcpy(obj, data, obj_size);
        if (counter != obj->counter) {
          PRINT_ERR("Received invalid counter %u expecting %u at data %p "
                    "at %uth %zu ahead obj packet=(%s)\n",
                    obj->counter, counter, data, i,
                    (uintptr_t)data - (uintptr_t)packet,
                    print_packet(packet));
          ret_val = EXIT_FAILURE;
          goto out;
        }
        data = (uint8_t *)((uintptr_t)data + (uintptr_t)obj_size);
        counter++;
      }
      PRINT_DEBUG("So far received %u packets, %u objects\n",
                  num_packets, num_sent_received_objs);
      /* Slow down if required */
      for (i = 0; i < slow_factor; i++) {
        uint32_t __attribute__ ((unused)) a;
        uint32_t b = 5000, c = 129;
        a = b *c;
      }
      /* increment number of sent objects */
      num_sent_received_objs += num_objs_per_batch;

      /* Inc other stats */
      num_packets++; /* NMumber of packets sent so far */
      sequence++;
      
      /* Now pulse the receiver */
      PULSE_PEER(1, is_use_eventfd ? sender_wakeup_eventfd_fd : sock_fd,
                 is_use_eventfd);
      
    }
    /* get time stamp when finish receiving */
    clock_gettime(CLOCK_MONOTONIC, &end_ts);
  }

 out:
  /*
   * convert timers to timval then use timersub to subtract them
   */
  TIMESPEC_TO_TIMEVAL(&start_tv, &start_ts);
  TIMESPEC_TO_TIMEVAL(&end_tv, &end_ts);
  timersub(&end_tv, &start_tv, &time_diff);
  print_stats("Exitting");

  close_fds();

  exit(ret_val);
}
