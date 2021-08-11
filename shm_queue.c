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
 * THE SOFTWARE AND TEXT IS PROVIDED "AS IS" AND THE AUTHOR(s) DISCLAIM ALL
 * WARRANTIES WITH REGARD TO THIS SOFTWARE AND TEXT INCLUDING ALL IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR(s)
 * BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY
 * DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
 * AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * What is this
 * ============
 * Point to multipoint shared memory queue comparison with 2 other IPC
 * mechanisms 
 * - AF_UNIX socket
 * - TCP socket
 * Parameters
 *   queue_name: default value "shm_queue"
 *   packet_size: Size of a single buffer in the circular ring. default value is
 *             1024 bytes 
 *   queue_len: number of buffers in the circular buffer. Default is 128
 *   num_receivers: default is 1
 *   batch_size: 
 *   - Sender: The number of packets to send before advancincg the tail of the
 *     queue AND incrementing the number of queued packets and signaling the
 *     receiver 
       (Receiver is signaled only if queue was empty when this batch started)
 *   - REceiver: Number of packets to dequeue before stopping and doing two
 *     things
 *     (1) advancing "head" and decrementing number of queued packets
 *     (2) if sender reached high water mark , check if queued packets went
 *         below low water mark. If so, pulse sender
 *     (3) If there are still packets queued in the quque, pulse itself so that
 *         it continueues dequeuing another batch later
 *         This step emulates the case wqhere the receiver has to do other
 *         things other than dequeueing packets and processing objects. 
 *         Ie.e defer dequeueing packets and processing incoming objects
 *   num_objs: Number of objects to transmit. Default is 2^20
 *   obj_size: The size of each data item to be sent by transmitter and
 *             received by receiver. default is 16
 * The queue is just a single shared memory file consisting of "num_objs" slots
 * each of size "obj_size"

 * After building, Execute
 *    <binary_name> -h
 * to understand the different options and their default values
 *
 * How to build
 *-------------
 * Use
 *    gcc -o shm_queue shm_queue.c -lrt -pthread -Wall -Wextra
 *
 * How to use
 * ----------
 * - Start the transmitter by doing
 *     <binary_name> -t
 *   The transmitter will first delete any existing shared memory queue and
 *   then will create a new one
 * - Wait until the transmitter dislays the info about the shared memory queue
 * - Start one or more receivers by doing
 *     <binary_name>
 * - Transmission and testing will start when all receivers have started and
 *   connected to the transmitter
 *
 * How to use for multiple receivers,
 * ------------------------
 * - On the sender use "-r <n> -u" to specify "n" receiver and the the use of
 *   "AF_UNIX" socket for singalling instead of eventfd
 * - On the receivers
 *   - On each receiver use "-r <n> -u" to specify "n" receiver and the use of
 *     "AF_UNIX" socket for singalling instead of eventfd wheren "n" on the
 *     receiver MUST be the same value as used oin the sender
 *   - Specify a DISTINCT receiver ID using -i <id> for each receiver where the
 *     value of "<id>" MUST be strictly less than "<n>"
 *
 * Example of multiple receiver uses
 * ----------------------------------
 * Sender:
 *  ./shm_queue -t -n 1000000000 -p 50000 -b 2 -o 50 -a -r 3 -u
 * -r 3 This is expecting exactly 3 receivers
 * -n 1000000000 : 1 billion objects
 * -p 50000; Each packet is 50,000 bytes
 * -b 2: A batch of 2 packets is sent at a time
 * -a : (optional) Use non-standard adaptive mutex
 * -u: Use AF_UNIX socket for signaling. Mandatory for multi-receiver
 *
 * Receiver 0:
 *   ./shm_queue -n 1000000000 -p 50000 -b 2 -o 50 -r3 -i0 -u
 * Receiver 0:
 *   ./shm_queue -n 1000000000 -p 50000 -b 2 -o 50 -r3 -i1 -u
 * Receiver 2:
 *   ./shm_queue -n 1000000000 -p 50000 -b 2 -o 50 -r3 -i2 -u
 *
 *
 * How SINGLE RECEIVER works
 * =========================
 * - The sender creates the shared memory queue
 * - THe sender maintains one array of file descriptors
 *   - server_accept_fd: THe file descriptors returned by "accept()"
 *     This is the socket used to transmit the eventfd file descriptor to the
 *     receiver 
 * - The transmitter creates two eventfd() filedescriptors
 *   - sender_wakeup_eventfd_fd: fd used by the last receiver to
 *     dequeue high water buffers to signal the   transmitter when the
 *     transmitter blocks  because the window is full  
 *   - receiver_wakeup_eventfd_fd: fd used by transmitter to signal receivers
 *     when the transmitter sees that the queue is empty when it starts
 *     queueing a batch of packets
 * - Both file descriptors are sent to each receiver when the receiver connects
 *   using an SCM_RIGHTS ancillary message
 * - The receiver does the following
 *   - Connects to the server AF_UNIX socket
 *   - Waits until it receives the two eventfd file descriptors over an
 *     SCM_RIGHTS ancillary message
 *   - If we are comparing with socket other that AF_UNIX
 *     - Closes the AF_unix SOCKET
 *     - waits 1 ssecond to make sure that the transmitter is waiting on 
 *       the IPC socket 
 *     - connects to the IPC socket
 *   - Otherwise (we are comparing against AF_UNIX socket
 *     It justuses the AF_UNIX socket that was used to receive the SCM_RIGHTS
 *     ancillary message containing the eventfd file descriptors
 * The server does the following
 * - Creates the two eventfd file descriptors
 * - Creates the AF_UNIX to send the eventfd file descriptors
 * - Loops for the number of receivers
 *   - waits on AF_UNIX with accept() 
 *   - when a receiver connects,
 *   - sends the two eventfd FDs using SCM_RIGHTS messages
 *   - If we are3 comapring something else other that AF_UNIX
 *     - Waity on the IPC FD using accept
 *     - When the receiver connects, store the FD returned by accept in the
 *       array fd_receiver_ipc_array
 *   - else 
 *      - store the FD returned by accept() on the AF_UNIX socket in the array
 *        fd_receiver_ipc_array 
 * - Start the test
 * 
 * How the transmitter sends objects to the SINGLE receiver
 * ==================================================================
 * queue any number of packets as follows
 * 1. lock(queue->mutex)
 * 2. If queue is empty
 *    2.1 pulse_receiver = true;
 * 3. else
 *    3.1 pulse receiver = false
 * 4. curr_batch_size =
 *      min(queue_len - number_queued_packets, remaining_objects/obj_per_packet)
 * 5. Unlock(eueu->mutex>
 * 6. queue one batch  as follows
 *    for (i = 0; i < batch_size; i++) {
 *       packet objects into a single packet
 *       add the packet at position (queue->tail % queue->queue_len
 *     endfor
 * 7. if (pulse_receiver) {
 *      write(receiver_wakeup_eventfd_fd)
 * 8. lock(queue->mutex)
 * 9. queue->tail = (queue->tail + curr_batch_size) % queue->queue_len
 * 10.  queue->num_queue_packet += curr_batch_size
 * 11.  if (queue->num_queue_packet == queue->queue_len)
 *        queue->high_water_mark_reached = true;
 * 12. Unlock(queue->mutex)
 * 13. If high waiter mark reached
 * 14    Wait for pulse from receiver
 * 14. Loop back to step 1
 *    - 
 *        
 * How the SINGLE receiver receives objects from the queue
 * ================================================
 * 1. Receiver epoll on receiver_wakeup_eventfd_fd
 * 2. lock(queue->mutex)
 * 3. if high_water_mark_reached == false
 *    3.1 If queue is full
 *        (I.e. sender is waiting until queue reaches low water mark)
 *        3.1.1 high_water_mark_reached = true;
 *    3.2 else
 *        4.1.1 high_water_mark_reached = false;
 *    3.3 endif
 * 4. endif 
 * 4.5 curr_batch_size = min(batch_size, queue->num_queued_packets)
 * 5. unlock(queue->mutex)
 * 6. Dequeue one batch  as follows
 *    for (i = 0; i < min(batch_size, queue->num_queued_packets); i++) {
 *       buffer = queue->packets[(queue->tail + i) % queue->queue_len]
 *       unpack one object at a time
 *       check the "obj->counter" field to make sure nothing lost
 *       do some processing to emulate receiver doing some work on objects
 * 7. endfor   
 * 8. lock(queue->mutex)
 * 9. queue->head = (queue->head + curr_batch_size)
 *                   % queue->queue_len
 * 10. queue->num_queued_packets -= curr_batch_size
 * 11. unlock(queue->mutex) 
 * 12. If (queue->num_queued_packets == 0) {
 *      high_water_mark_reached = false
 *      (of course we are out of high water mark)
 *      goto line 1
 *       (no more items in the queue, wait on receiver_wakeup_eventfd_fd,
 *        which will come when sender queues one or more packets)
 * 13. else
 *       Still some items in the queue. So we have to pulse ourselves
 *       got back to step 1
 *       
 *       * If we have reached the high water mark, see if we went lower than the
 *         low water mark. If so, pulse the transmitter
 *       if (high_water_mark_reached) {
 *          if (queue->num_queued_packet < low_water_mark) 
 *             high_water_mark_reached = false; * no longer in high water mark state
 *             write(sender_wakeup_eventfd_fd)  * wakeup the transmitter *
 *          endif
 *       endif
 * 14. endif
 *
 *
 * How Multiple Receivers works
 *-=============================
 * - Similar to the single receiver cases
 * - More details: TBD
 * 
 *
 * NOTE:
 * The same idea can be applied by assuming that the queue is a single
 * contnuous buffers and both head and tail move in increments of bytes
 * instead of increments of packets
 *
 * NOTE: regarding using eventfd with multiple receivers
 *- As it has been tested before, it is possible for multiple processes to wait
 *  on the same eventfd file descriptor using epoll_wait() and have a single
 *  wite() to that eventfd wake up all these pprocess out of epoll_wait()
 * - But I noticed that When I use eventfd with multiple receivers, I can see
 *   that a pulse gets lost and we fall into a deadlock after ew hundred
 *   thousands to few million objects. Hence I wll NOt use it
 * - This may very well be a bug in my code or the fact that a single write
 *   to an eventfd cannot be used reliably to wakeup multiple waiters on that
 *   same eventfd using epoll_wait()
 * - HEnce I am disallowing the use of eventfd for signaling from sender to
 *   recievers when there are more than one receiver
 * - Instead, I am using the AF_UNIX socket that was used to send the
 *   SCM_RIGHTS message for signaling by individually sending a single byte to
 *   each receiver by the sender or from a receiver to the sender
 * - I even disallowed using eventfd with multiple receivers for a receiver to
 *   pulse itself when the user uses the "-d" option because I saw some
 *   problems
 * - Bottom line, it seems sharing the same eventfd file descriptors among
 *   multiple processes (and possibly multiple threads within the same process)
 *   is either unreliable OR needs some different code
 * - this is not the issue that we are tackling in this code so we will not
 *   look at it as this point in time
 *


 *****************************************************************/

#include <errno.h>
#include <locale.h>
#include <signal.h>
#include <stdio.h>
#include <stddef.h>  /* for offsetof */
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
#include <sys/socket.h>  /* AF_UNIX and other sockets */
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



/* Default values for the samred memory queue*/
#define DEFAULT_NUM_RECEIVERS (1)
#define DEFAULT_PACKET_SIZE (8192) /* Size of a single buffer in the ring */
#define DEFAULT_NUM_OBJS (1 << 20) /* 1M objects to send */
#define DEFAULT_QUEUE_LEN (256) /* Number of buffers in the circular ring  */
#define DEFAULT_QUEUE_NAME "shm_queue"
#define DEFAULT_BATCH_SIZE (32)/* # packets to write or read before sender
                                  writes to or receiver waits on eventfd,
                                  respectively */
#define DEFAULT_QUEUE_LOW_WATER (DEFAULT_QUEUE_LEN >> 1) /* 1/2 the queue */
#define DEFAULT_SLOW_FACTOR (0) /* Looping to slow sender or receiver down */

/* Max values */
#define MAX_PACKET_SIZE  (65536)
#define MAX_NUM_OBJS (UINT32_MAX) /* 4 Billion objects to transmit */
#define MAX_QUEUE_LEN (8192)  /* Maximum number of packets in the queue */
#define MAX_QUEUE_NAME_LEN 256
#define MAX_BATCH_SIZE (MAX_QUEUE_LEN) /* a single batch can be the entire queue*/

/*
 * Default named socket (AF_UNIX) path
 */
#define DEFAULT_SOCK_PATH "/tmp/test_named_socket_path"

/*
 * max and default values for the transmitter-receiver communciation
 */
#define MAX_NUM_RECEIVERS (64)


/*
 * Objects to be sent
 */
typedef struct obj_t_ {
  uint32_t counter; /* To catch any loss */
  uint8_t data[]; /* so that we can size it anyway we want */
} obj_t;


/*
 * Single packet
 */
typedef struct packet_t_ {
  uint32_t sequence;
  uint32_t num_objs;
  uint8_t data[];
} packet_t;


/*
 * per receiver queue info
 */
typedef struct shm_receiver_queue_t_ {
  bool high_water_mark_reached;
  bool receiver_waiting_for_pulse_from_sender; /* true when receiver is waiting
                                                  for sender to wake it up*/
  uint32_t head; /* where receiver dequeues packets */
  uint32_t num_queued_packets; /* # packets still queue for this receiver */
  uint32_t receiver_id; /* to catch running more than one reciver with the same
                           ID */
} shm_receiver_queue_t;

/*
 * the queue
 */
typedef struct shm_queue_t_ {
  /* per queue info */
  uint32_t obj_size;
  uint32_t queue_len; /* total Number of packets in the queue */
  uint32_t window_size; /* total Number of byte of the shared mem window */
  uint32_t tail; /* where sender enqueues packets */
  uint32_t packet_size; /* packet size */
  uint32_t num_receivers; /* Number of receivers expected by the sender */
  pthread_mutex_t mutex; /* mutual exclusion between sender/receiver */
  bool is_adaptive_mutex;

  /* Per receiver info */
  shm_receiver_queue_t receiver_queue[MAX_NUM_RECEIVERS];;

  /* the packets carrying payload*/
  uint8_t packets[]; /* the actual list of packets */
} shm_queue_t;


/* default and max data item size */
#define DEFAULT_OBJ_SIZE (sizeof(obj_t) + 30)
#define MIN_OBJ_SIZE (sizeof(obj_t))
#define MAX_OBJ_SIZE (1024)

/*
 * objects to bbe sent
 */
uint32_t obj_size = DEFAULT_OBJ_SIZE;
static uint32_t num_objs = DEFAULT_NUM_OBJS;

/* Need these to be public so that the signal handler can use them */
static char *sock_path = DEFAULT_SOCK_PATH;
static int server_accept_fd[MAX_NUM_RECEIVERS];
static int sock_fd;
/* When the queue is full, the sender will epoll on this FD until the receiver
   dequeues enough packets to make the queue length less than
   queue_low_water_mark
*/
static int sender_wakeup_eventfd_fd = -1;
/* If the queue is empty, then the receiver will epoll on this FD until the
   sender queues one or more packets and writes to this FD and hence the
   receiver will wakeup to dequeues these packets
*/
static int receiver_wakeup_eventfd_fd = -1;

static bool is_transmitter = false;
static uint32_t num_receivers = DEFAULT_NUM_RECEIVERS;
static int eventfd_flags = EFD_NONBLOCK | EFD_SEMAPHORE;


/*
 * IN general, after each batch we will pulse ouirselves if there are still
 * packets in the queu (for receiver) 
 * this way we defer receiving to emulate a thread that has to
 * service other tasks instead of dwqeueing packets
 * However if the user wants us to only service packets, then the variable has
 * to be false
 */
bool receiver_defer_after_each_batch = false;


/*
 * Use eventfd to pulse receivers instead of individually pulsing the AF_UNIX
 * socket of each receiver
 */
bool is_use_eventfd_to_pulse_receivers = true;

/* Variables for the window */
static uint32_t queue_len = DEFAULT_QUEUE_LEN;
/* If the sender blocks because there are no empty packets in the queue, then
   the receier will pulse the sender when there is at least
   low_water_mark packets available */
static uint32_t low_water_mark = DEFAULT_QUEUE_LOW_WATER;
static uint32_t packet_size = DEFAULT_PACKET_SIZE;
static char queue_name[MAX_QUEUE_NAME_LEN + 1];
static uint32_t window_size; /* total Number of byte of the shared mem window   */
static uint32_t batch_size = DEFAULT_BATCH_SIZE; /* # packets for transmitter
                                                 to write or receiver to read
                                                 before stopping to write
                                                 or wait on eventfd,
                                                 respectively */

/*
 * the queue
 */
shm_queue_t *queue;
bool is_adaptive_mutex = false; /* whether to use adaptive mutex */

/*
 * memoryfor the sender to to send objects and receiver to receive objects
 */
obj_t *obj;


/*
 * counters
 */
uint32_t num_batchs = 0; /* Number of batches of packets sent/received */
uint32_t num_packets = 0; /* Number packets sent/received */
uint32_t num_sent_received_objs = 0; /* Number objects sent/received */
uint32_t num_receiver_wakeup_needed = 0; /* For sender : number of times sender
                                            pulsed recever */
uint32_t num_receiver_pulse_myself = 0; /* For receiver: number of times the
                                            receiver pulsed itself because
                                            after draining a batch, the
                                            receiver found that there are
                                            still more queued packets and
                                            hence it had to pulse itself to
                                            wakeup later and continue draining
                                            these packets */
uint32_t num_sender_wakeup_needed = 0; /* number of times sender waited for
                                          recevieer. Needed when queue reached
                                          high water mark and sender has to
                                          wait for receiver to dequeue some
                                          packets
                                          MUST be identical to the number of
                                          times the low water mark reached
                                          AFTER reaching the high water mark. 
                                          I.e. num_low_water_mark_reached */
uint32_t num_lost_objs = 0; /* number of lost objects at receiver */
uint32_t num_lost_packets = 0;
uint32_t num_pulse_myself = 0; /* # times receiver had to pulse itself because
                                    by the time it drained a full batch of
                                    packets there where still some packets in
                                    the queue and we know that the sender will
                                    ONLY pulse the receiver if, at the time the
                                    sender queues a batch, there was no
                                    packets in the queue */

/* Number of times recevier saw the high water mark flag set then the size of
   the queue goes below the low water mark AND we were the last receiver that
   had our high water mark then go below low water mark
   As a result of that, we will pulse the sender out of epoll_wait()*/
uint32_t num_last_receiver_reach_low_water_mark = 0;
/* Number of times recevier saw the high water mark flag set then the size of
   the queue goes below the low water mark NUT we were NOT the last receiver
   that had our high water mark then go below low water mark
   Hence we will NOT pulse the sender*/
uint32_t num_non_last_receiver_reach_low_water_mark = 0;

/* Number of times the sender attempted to send a batch but coundn't because
   the current batch size is zero because the queue of one of the receivers is
   full (or VERY UNLIKELY because all objects were sent) */
uint32_t num_sender_zero_curr_batch_size = 0;

/*
 * reveiver ID
 */
static uint32_t receiver_id = UINT32_MAX;


/*
 * slow factor
 */
uint32_t slow_factor = DEFAULT_SLOW_FACTOR;



/*
 * timestamps when starcting and after finishning
 */
struct timespec start_ts;
struct timeval start_tv;
struct timespec end_ts;
struct timeval end_tv;
struct timeval time_diff;

/* Macro to print error */
#define PRINT_ERR(str, param...)                                        \
  do {                                                                  \
    print_error("%s %d: " str, __FUNCTION__, __LINE__, ##param);        \
  } while (false);
#define PRINT_INFO(str, param...)                                       \
  do {                                                                  \
    print_debug("%s %d: " str, __FUNCTION__, __LINE__, ##param);        \
  } while (false);

#ifdef DEBUG_ENABLE
#define PRINT_DEBUG(str, param...)                                      \
  do {                                                                  \
    print_debug("%s %d: " str, __FUNCTION__, __LINE__, ##param);        \
  } while (false);
#else
#define PRINT_DEBUG(str, param...)
#endif

/*
 * Macros to loc/unlock queue mutex
 */
#define QUEUE_LOCK                                  \
  do {                                              \
    int rc = pthread_mutex_lock(&queue->mutex);     \
    if (rc) {                                       \
      PRINT_ERR( "\nCannot lock mutex: %d %s",      \
                 rc, strerror(rc));                 \
      return EXIT_FAILURE;                          \
  }                                                 \
} while(false)
#define QUEUE_UNLOCK                                       \
  do {                                                     \
    int rc = pthread_mutex_unlock(&queue->mutex);          \
    if (rc) {                                              \
      PRINT_ERR( "\nCannot UNlock mutex: %d %s",           \
                 rc, strerror(rc));                        \
      return EXIT_FAILURE;                                 \
    }                                                      \
} while(false)

/*
 * Mmacros to advance the head and tail pointers and adjust the number of
 * queued packets accordnigly
 * REMEMBER
 * - Advancing the head means a packet has been dequeued and the number of
 *   packets in the queue is decremented
 * - Advancing the tail means a packet has been enqueued and the number of
 *   packets in the queue is incremented
 * V.I.
 * To use bitwise AND instead of long division to do "mod" opeation, we assume
 * that the queue length is a pwoer of 2
 */
/* Each receiver individually increments its own queue */
#define QUEUE_HEAD_INC(__my_batch_size, __ith)                          \
  do {                                                                  \
    assert(!(__my_batch_size > queue->receiver_queue[__ith].num_queued_packets)); \
    queue->receiver_queue[__ith].num_queued_packets -= __my_batch_size;       \
    /* Unlike the tail, we have multiple receivers so we need multiple heads*/ \
    queue->receiver_queue[__ith].head =                                       \
      (queue->receiver_queue[__ith].head + __my_batch_size) & (queue->queue_len - 1); \
  } while (false)
/* for tail_inc, sender increments queue count for ALL receivers in one shot */
#define QUEUE_TAIL_INC(__my_batch_size)                                 \
  do {                                                                  \
    uint32_t i;                                                         \
    /* One tail for all revceiver because we have 1 sender */           \
    queue->tail = (queue->tail + __my_batch_size) & (queue->queue_len - 1); \
    for (i = 0; i < num_receivers; i++) {                               \
      assert(!(receiver_queue[i].num_queued_packets + __my_batch_size >  \
               queue->queue_len));                                      \
      receiver_queue[i].num_queued_packets += __my_batch_size;         \
    }                                                                   \
  } while (false)

/* Print error in read color */
__attribute__ ((format (printf, 1, 2), unused))
static void print_error(char *string, ...)
{
  va_list args;
  va_start(args, string);
  /*fprintf(stderr, COLOR_RED);
  vfprintf(stderr, string, args);
  fprintf(stderr, COLOR_RESET);
  fprintf(stdout, COLOR_RED);*/
  /* We will print errors to stdout instead of stderr so that it gets
     correctly interleaved with regular debug messages when we redirect output
     to a file
  */
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
 * Close server side sockets and file descriptor
 */
static void  __attribute__ ((unused))
close_fds(void)
{
  uint32_t i;
  for (i = 0; i < num_receivers; i++) {
    if (server_accept_fd[i] > 0) {
      close(server_accept_fd[i]);
    } else {
      if (is_transmitter) {
        PRINT_ERR("Server socket %u NOT opened\n", i);
        } else {
        PRINT_DEBUG("Server socket %u NOT opened\n", i);
      }
    }
  }
    
  if (sender_wakeup_eventfd_fd > 0) {
    close(sender_wakeup_eventfd_fd);
  } else {
    PRINT_DEBUG("sender_wakeup eventfd() NOT opened\n");
  }
  if (receiver_wakeup_eventfd_fd > 0) {
    close(receiver_wakeup_eventfd_fd);
  } else {
    PRINT_DEBUG("receiver_wakeup eventfd() NOT opened\n");
  }
  if (sock_fd > 0) {
    close(sock_fd);
  } else {
    PRINT_ERR("sockets AF_UNIX %d NOT opened\n",
              sock_fd);
  }
}



static char *
print_packet(packet_t *packet)
{
  static char buf[1025];
  memset(buf, 0, sizeof(buf));
  snprintf(buf, 1024, "seq=%u num_objs=%u",
           packet->sequence,
           packet->num_objs);
  return (buf);
}

static char *
print_queue(shm_queue_t *queue)
{
  static char buf[2049];
  memset(buf, 0, sizeof(buf));
  snprintf(buf, 2048, "len=%u tail=%u obj=%u pack_size=%u window=%u",
           queue->queue_len,
           queue->tail,
           queue->obj_size,
           queue->packet_size,
           queue->window_size);
  for (uint32_t i = 0; i < num_receivers; i++) {
    snprintf(&buf[strlen(buf)], 2048 - strlen(buf),
             "\n\t  %u: queued=%u head=%u wait_pulse=%s HW=%s",
             i,
             queue->receiver_queue[i].num_queued_packets,
             queue->receiver_queue[i].head,
             queue->receiver_queue[i].receiver_waiting_for_pulse_from_sender
             ? "T" : "F",
             queue->receiver_queue[i].high_water_mark_reached ? "T" : "F");
  }
  return(buf);
}

static char *
print_queue_receiver(shm_queue_t *queue, uint32_t my_receiver_id)
{
  static char buf[2049];
  memset(buf, 0, sizeof(buf));
  snprintf(buf, 2048, "%u: len=%u tail=%u obj=%u pack_size=%u window=%u"
           "\n\t  queued=%u head=%u wait_pulse=%s HW=%s",
           my_receiver_id,
           queue->queue_len,
           queue->tail,
           queue->obj_size,
           queue->packet_size,
           queue->window_size,
           queue->receiver_queue[my_receiver_id].num_queued_packets,
           queue->receiver_queue[my_receiver_id].head,
           queue->receiver_queue[my_receiver_id].receiver_waiting_for_pulse_from_sender
           ? "T" : "F",
           queue->receiver_queue[my_receiver_id].high_water_mark_reached ? "T" : "F");
  return(buf);
}

/*
 * send a single pulse to receiver
 */
#define PULSE_RECEIVER(pulse, is_transmitter)                   \
  do {                                                          \
    uint64_t eventfd_write = pulse;                                     \
  if (write(receiver_wakeup_eventfd_fd, &eventfd_write, sizeof(eventfd_write))\
      !=  sizeof(eventfd_write)) {                                      \
    PRINT_ERR("Cannot write message %zu to receiver_wakeup fd %d\n"     \
              "\tSo far sent %d objects %u packets %u batchs\n "        \
              "\tcurr_batch size %d\n"                                  \
              "\tqueue=(%s): %d %s\n",                                  \
              eventfd_write,                                            \
              receiver_wakeup_eventfd_fd,                               \
              num_sent_received_objs,                                   \
              num_packets,                                              \
              num_batchs,                                               \
              curr_batch_size,                                          \
              print_queue(queue),                                       \
              errno, strerror(errno));                                  \
    exit(EXIT_FAILURE);                                                 \
  } else {                                                              \
    if (is_transmitter) {                                               \
      num_receiver_wakeup_needed++;                                     \
    } else {                                                            \
      num_receiver_pulse_myself++;                                      \
    }                                                                   \
    PRINT_DEBUG("SUCCESSFULLY pulsed %zu to receiver_wakeup fd %d.\n "  \
                "\tSo far receiver_wakeup_needed %u.\n"                 \
                "\tSo far sent %d objects %u packets %u batchs "        \
                "curr_batch_size %d\n"                                  \
                "\tqueue=(%s)\n",                                       \
                eventfd_write,                                          \
                num_receiver_wakeup_needed,                             \
                receiver_wakeup_eventfd_fd,                             \
                num_sent_received_objs,                                 \
                num_packets,                                            \
                num_batchs,                                             \
                curr_batch_size,                                        \
                print_queue(queue));                                    \
  }                                                                     \
 } while (false)



/*
 * send a pulse to the sender
 */
#define PULSE_SENDER                            \
  do {                                                                  \
    uint64_t eventfd_write = 1; /* ONLY ONE sender */                   \
    if (write(sender_wakeup_eventfd_fd, &eventfd_write, sizeof(eventfd_write)) \
        !=  sizeof(eventfd_write)) {                                    \
      PRINT_ERR("Cannot write message %zu to sender_wakeup fd %d\n "    \
                "\tSo far sender_wakeup_needed %u lost objects %u.\n"   \
                "\tSo received %d objects %u packets %u batchs\n"       \
                "\tqueue=(%s): %d %s\n",                                \
                eventfd_write,                                          \
                sender_wakeup_eventfd_fd,                               \
                num_sender_wakeup_needed,                               \
                num_lost_objs,                                          \
                num_sent_received_objs,                                 \
                num_packets,                                            \
                num_batchs,                                             \
                print_queue(queue),                                     \
                errno, strerror(errno));                                \
      exit(EXIT_FAILURE);                                               \
    } else {                                                            \
      PRINT_DEBUG("SUCCESSFULLY pulsed %zu to sender_wakeup fd %d.\n"   \
                  "\tSo far sender_wakeup_needed %u lost objs.\n"       \
                  "\tSo received %d objects %u packets %u batchs batch size %u\n" \
                  "\tqueue=(%s)\n",                                     \
                  eventfd_write,                                        \
                  num_sender_wakeup_needed,                             \
                  num_sent_received_objs,                               \
                  num_lost_objs,                                        \
                  num_packets,                                          \
                  num_batchs,                                           \
                  batch_size,                                           \
                  print_queue(queue));                                  \
    }                                                                   \
  } while (false)


#define READ_PULSE(__use_eventfd)                       \
do {                                                    \
  uint64_t fd_read;                                                     \
  rc = (int)read(event.data.fd, &fd_read,                               \
                 __use_eventfd ? sizeof(uint64_t) : sizeof(uint8_t));    \
  if (((__use_eventfd) ?                                                \
       (ssize_t)rc !=  sizeof(uint64_t) :                               \
       (ssize_t)rc !=  sizeof(uint8_t))) {                              \
    PRINT_ERR("CANNOT read from %s %d "                                 \
              "num_epoll_wait %u "                                      \
              "current packet SEQUENCE %u, "                            \
              "curr_batch_size %u head %u from %uth packet %p data %p \n" \
              "\tSo far sender_wakeup_needed %u lost objects %u "       \
              "lost packets %u.\n"                                      \
              "\tSo received %d objects %u packets %u batchs\n"         \
              "\tqueue=(%s): %d %s\n",                                  \
              is_use_eventfd_to_pulse_receivers ?                       \
              "receiver_wakeup_eventfd_fd" : "sock_fd",                 \
              is_use_eventfd_to_pulse_receivers ?                       \
              receiver_wakeup_eventfd_fd : sock_fd,                     \
              num_epoll_wait,                                           \
              sequence,                                                 \
              curr_batch_size, receiver_queue->head, i, packet, data,   \
              num_sender_wakeup_needed,                                 \
              num_lost_objs,                                            \
              num_lost_packets,                                         \
              num_sent_received_objs,                                   \
              num_packets,                                              \
              num_batchs,                                               \
              print_queue(queue),                                       \
              errno, strerror(errno));                                  \
    ret_value = EXIT_FAILURE;                                           \
    goto out;                                                           \
  }                                                                     \
 } while (false);

/*
 * print stats
 */
static void print_stats(void)
{
  PRINT_INFO("\n%s Statistics:\n"
             "\tTotal Time: %lu.%06lu\n"
             "\tAdaptive mutex = %s\n"
             "\tnum_receivers = %u\n"
             "\tnum_batchs = %u\n"
             "\tMax batch_size = %u\n"
             "\tMax batch_size in bytes = %u\n"
             "\tnum_packets = %u\n"
             "\tPacket size = %u\n"
             "\tobjs/packet = %u\n"
             "\tobj size = %u\n"
             "\tnum_sent_received_objs = %u\n"
             "\t%s = %u\n"
             "\tnum_sender_wakeup_needed = %u\n"
             "\tnum_lost_objs = %u\n"
             "\tnum_pulse_myself  = %u\n"
             "\tnum_last_receiver_reach_low_water_mark = %u\n"
             "\tnum_non_last_receiver_reach_low_water_mark = %u\n"
             "\tnum_sender_zero_curr_batch_size = %u\n"
             "\tSlow factor = %u\n"
             "%s %s"
             "\tqueue = %s\n",
             is_transmitter ? "Sender" : "Receiver",
             time_diff.tv_sec, time_diff.tv_usec,
             is_adaptive_mutex ? "true" : "false",
             num_receivers,
             num_batchs,
             batch_size,
             batch_size * queue->packet_size ,
             num_packets,
             queue->packet_size,
             queue->packet_size/queue->obj_size,
             queue->obj_size,
             num_sent_received_objs,
             is_transmitter ? "num_receiver_wakeup_needed" :
             "num_receiver_pulse_myself",
             num_receiver_wakeup_needed,
             num_sender_wakeup_needed,
             num_lost_objs,
             num_pulse_myself,
             num_last_receiver_reach_low_water_mark,
             num_non_last_receiver_reach_low_water_mark,
             num_sender_zero_curr_batch_size,
             slow_factor,
             is_transmitter ? "" : "\treceiver_defer_after_each_batch = ",
             is_transmitter ? "" :
             (receiver_defer_after_each_batch ? "true\n" : "false\n"),
             is_transmitter ? 
             print_queue(queue) : print_queue_receiver(queue, receiver_id));
}



static bool
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

  
/* CTRL^C handler */
static void
signal_handler(int s __attribute__ ((unused))) 
{
  PRINT_DEBUG("Caught signal %d\n",s);

  if (is_transmitter) {
    if (unlink(sock_path)) {
      if (errno != ENOENT) {
        PRINT_ERR("Cannot unlink sock_path %s: %d %s\n",
                  sock_path,
                  errno, strerror(errno));
        exit(EXIT_FAILURE);
      } else {
        PRINT_ERR("sock_path %s does NOT exist\n",
                  sock_path);
      }
    } else {
      PRINT_DEBUG("Successfully deleted sock_path '%s' \n",
                  sock_path);
    }
  }  

  
  PRINT_DEBUG( "Going close socket %d and %u accepted "
               "sockets, "
               "receiver socket  %d "
               "and sender_wakeup eventfd() %d "
               "receiver_wakeup eventfd() %d\n",
               sock_fd,
               num_receivers,
               sock_fd,
               sender_wakeup_eventfd_fd,
               receiver_wakeup_eventfd_fd);
  
  close_fds();
  print_stats();
  exit(EXIT_SUCCESS); 
}


  /*
   * bind an open unix socket to sockpath
   */
static void
bind_to_sock_path(int sock_fd)
{
  struct sockaddr_un sockaddr;
  sockaddr.sun_family = AF_UNIX;
  strcpy(sockaddr.sun_path, sock_path);
  socklen_t len = strlen(sockaddr.sun_path) + sizeof(sockaddr.sun_family);
  /*
   * delete the sockpath
   */
  if (unlink(sockaddr.sun_path)) {
    if (errno != ENOENT) {
      PRINT_ERR("Cannot unlink sock_path %s: %d %s\n",
                sockaddr.sun_path,
                errno, strerror(errno));
      exit(EXIT_FAILURE);
    } else {
      PRINT_DEBUG("sock_path %s does NOT exist\n",
                sock_path);
    }
  }
    
  /* bind to the sockpath */
  if (bind(sock_fd, (struct sockaddr *)&sockaddr, len) == -1) {
    PRINT_ERR("Cannot bind socker %d to sockpath %s length %d: %d %s\n",
              sock_fd, sockaddr.sun_path, len,
              errno, strerror(errno));
    exit(EXIT_FAILURE);
  } else {
    PRINT_DEBUG("Succesfully bound socker %d to sockpath '%s' length %d\n",
              sock_fd, sockaddr.sun_path, len);
  }
}

/*
 * Prepares msg and cmsg for the "num_fs" file descriptors to be either sent or
 * received
 * It does NOT copy into or extract from the msg or cmsg the file descriptors
 */

static void
prepare_msg(int *fds,
            struct msghdr *msg,
            struct cmsghdr **cmsg_p,
            uint32_t num_fds)
{
  static char iobuf[1];
  static struct iovec io = {
    .iov_base = iobuf,
    .iov_len = sizeof(iobuf)
  };
  struct cmsghdr *cmsg;
  static union {         /* Ancillary data buffer, wrapped in a union
                            in order to ensure it is suitably aligned */
    char buf[0];
    struct cmsghdr align;
  } *u;

  size_t size = CMSG_SPACE(sizeof(*fds)*num_fds);
  u = malloc(size);
  if (!u) {
    PRINT_ERR("Cannot allocate %zu bytes for %u fds\n",
              CMSG_SPACE(sizeof(*fds)*num_fds), num_fds);
    exit(EXIT_FAILURE);
  }

  /* Setup the application-specific payload of message
   * Remember that from the point of view of the kernel, this payload is
   * completely application specific and NOT related  to the control
   * (ancillary) SCM_RIGHTS message
   * However we still need this to be non-zero. I think this is to make
   * sendmsg() and recvmsg() work (but I am not sure)
   */
  memset(msg, 0, sizeof(*msg));
  msg->msg_iov = &io;
  msg->msg_iovlen = 1;

  /* Setup the control messaage (ancillary data) info inside "msg" */
  msg->msg_control = u->buf;
  msg->msg_controllen = size;

  /* populate the control message (ancillary data)  */
  cmsg = CMSG_FIRSTHDR(msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(sizeof(*fds) * num_fds);

  /* Return the populated value */
  *cmsg_p =  cmsg;
    
}


/*
 * receive the file descriptors
 */
static ssize_t
recev_fds(int sock, int *fds, uint32_t num_fds)
{
  struct msghdr msg;
  struct cmsghdr *cmsg = NULL;
  int *fdptr;

  /*
   * Prepare the structures
   */
  prepare_msg(fds,
              &msg,
              &cmsg,
              num_fds);
  
  PRINT_DEBUG("Going to wait to receive %u fds.\n", num_fds);
  ssize_t j = recvmsg(sock, &msg, 0); /* send the SCM_RIGHTS message */
  if (j < 0) {
    PRINT_ERR("FAILED recvmsg() to fd %d for %u fds "
              "msg.msg_iovlen=%u, msg.msg_controllen=%u "
              "cmsg->cmsg_level=%u "
              "cmsg->cmsg_type=%u "
              "cmsg->cmsg_len = %u"
              "\tmsg=  %p\n"
              "\tcmsg= %p\n"
              ": %d %s\n",
              sock, num_fds,
              (uint32_t)msg.msg_iovlen,
              (uint32_t)msg.msg_controllen,
              (uint32_t)cmsg->cmsg_level,
              (uint32_t)cmsg->cmsg_type,
              (uint32_t)cmsg->cmsg_len,
              &msg,
              cmsg,
              errno, strerror(errno));
  } else {
    fdptr = (int *) CMSG_DATA(cmsg);    /* get the location of the payload */ 
    memcpy(fds, fdptr, num_fds * sizeof(*fds)); /* Copy the fds from the msg */
    PRINT_DEBUG("SUCCESS recv_fds() recevied %zd bytes to fd %d for %u fds\n "
              "\tmsg.msg_iovlen=%zu\n "
              "\tmsg.msg_controllen=%zu \n"
              "\tcmsg->cmsg_level=%u\n"
              "\tcmsg->cmsg_type=%u \n"
              "\tcmsg->cmsg_len=%zu\n"
              "\tmsg=  %p\n"
              "\tcmsg= %p\n"
              "\tfdptr=%p\n",
              j,
              sock, num_fds,
              msg.msg_iovlen,
              msg.msg_controllen,
              cmsg->cmsg_level,
              cmsg->cmsg_type,
              cmsg->cmsg_len,
              &msg,
              cmsg,
              fdptr);
  }
  /* Free mem we allocated for the control message when calling prepare(msg) */
  free(msg.msg_control);
  return (j);
}

/*
 * send any array of file descriptors to the socket using SCM_RIGHTS buffer
 * over an AF_UNIX socket. 
 * Assume socket is already connected
 * Most of this came from "man -s3 cmsg"
 */
static ssize_t
send_fds(int sock, int *fds, uint32_t num_fds)
{
  struct msghdr msg;
  struct cmsghdr *cmsg = NULL;
  int *fdptr;

  /*
   * Prepare the structures
   */
  prepare_msg(fds,
              &msg,
              &cmsg,
              num_fds);

  /* Get a pointer to the ancillary data payload */
  fdptr = (int *) CMSG_DATA(cmsg);
  /* Copy the fds into the ancillary data payload  */
  memcpy(fdptr, fds, num_fds * sizeof(*fds));
  /* send the msg containing SCM_RIGHTS message */
  ssize_t j = sendmsg(sock, &msg, 0);
  if (j < 0) {
    PRINT_ERR("FAILED send_fds() to fd %d for %u fds "
              "msg.msg_iovlen=%u, msg.msg_controllen=%u "
              "cmsg->cmsg_level=%u "
              "cmsg->cmsg_type=%u "
              "cmsg->cmsg_len = %u"
              ": %d %s\n",
              sock, num_fds,
              (uint32_t)msg.msg_iovlen,
              (uint32_t)msg.msg_controllen,
              (uint32_t)cmsg->cmsg_level,
              (uint32_t)cmsg->cmsg_type,
              (uint32_t)cmsg->cmsg_len,
              errno, strerror(errno));
  } else {
    PRINT_DEBUG("SUCCESS send_fds() sent %zd bytes to fd %d for %u fds\n "
              "\tmsg.msg_iovlen=%zu\n "
              "\tmsg.msg_controllen=%zu \n"
              "\tcmsg->cmsg_level=%u\n"
              "\tcmsg->cmsg_type=%u \n"
              "\tcmsg->cmsg_len=%zu\n"
              "\tmsg=  %p\n"
              "\tcmsg= %p\n"
              "\tfdptr=%p\n",
              j,
              sock, num_fds,
              msg.msg_iovlen,
              msg.msg_controllen,
              cmsg->cmsg_level,
              cmsg->cmsg_type,
              cmsg->cmsg_len,
              &msg,
              cmsg,
              fdptr);
  }
  /* Free mem we allocated for the control message when calling prepare(msg) */
  free(msg.msg_control);
  return (j);
}


static void
print_usage (const char *progname)
{
  fprintf (stdout,
           "usage: %s [options] \n"
           "\t-a Use Adaptive Mutex (non-portable)\n"
           "\t-d receiver defer dequeing after each batch by pulsing itself (default '%s')\n"
           "\t-t Transmitter mode instead of the default receiver mode\n"
           "\t-s <slow_factor> default %u\n"
           "\t-S <sockpath>: user another socket path instead of default '%s'\n"
           "\t-p <packetsize>: user another packet size default %u\n" 
           "\t-r <num_receivers>: Specify number of receivers instead of default %u\n"
           "\t-n <num_objs> # of objects to send/receive. Default %u\n"
           "\t-m <shared memory queue file name>, default '%s'\n"
           "\t-q <queue len>. Default %u packets\n"
           "\t-l <low water mark> Default %u packets\n"
           "\t-b <batch size>, default %u\n"
           "\t-i <receiver_ID>. Only needed when more than 1 receiver\n"
           "\t-u Use AF_UNIX socket for singalling instead of default eventfd\n"
           "\t-o <object size>, default %u\n",
           progname,
           receiver_defer_after_each_batch ? "true" : "false",
           DEFAULT_SLOW_FACTOR,
           DEFAULT_SOCK_PATH,
           DEFAULT_PACKET_SIZE,
           DEFAULT_NUM_RECEIVERS,
           DEFAULT_NUM_OBJS,
           DEFAULT_QUEUE_NAME,
           DEFAULT_QUEUE_LEN,
           DEFAULT_QUEUE_LOW_WATER,
           DEFAULT_BATCH_SIZE,
           (uint32_t)DEFAULT_OBJ_SIZE);
}


int
main (int    argc,
      char **argv)
{
  uint32_t i, j;
  int opt;
  struct sockaddr_un sockaddr_remote;
  socklen_t remote_len;
  int rc;
  int shm_fd;
  int scm_fds[2];
  int epoll_fd;
  struct epoll_event event;
  uint32_t curr_batch_size = 0;
  int ret_value = EXIT_SUCCESS;
  /* Number of objects to pack in a packet */
  uint32_t objs_per_packet;
  /* packet info to readwrite */
  packet_t *packet;
  uint8_t *data;


  /* Init queueu name */
  memset(queue_name, 0, sizeof(queue_name));
  strcpy(queue_name, DEFAULT_QUEUE_NAME);
  
  while ((opt = getopt (argc, argv, "aetdup:r:n:q:b:o:l:S:s:i:")) != -1) {
    switch (opt)
      {
      case 'a':
        is_adaptive_mutex = true;
        break;
      case 'd':
        receiver_defer_after_each_batch = true;
        break;
      case 't':
        is_transmitter = true;
        break;
      case 's':
        if (1 != sscanf(optarg, "%u", (uint32_t *)(&slow_factor))) {
          PRINT_ERR("\nCannot read Slow factor %s. \n", optarg);
          exit (EXIT_FAILURE);
        }
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
      case 'p':
        if (1 != sscanf(optarg, "%u", &packet_size)) {
          PRINT_ERR("\nCannot read packet size %s. \n", optarg);
          exit (EXIT_FAILURE);
        }
        if (packet_size > (uint32_t)MAX_PACKET_SIZE || packet_size < 256) {
          PRINT_ERR("\nInvalid number of objects %s. "
                    "Must be between %u and %u\n", optarg, 256, MAX_PACKET_SIZE);
          exit (EXIT_FAILURE);
        }
        break;
      case 'n':
        if (1 != sscanf(optarg, "%u", &num_objs)) {
          PRINT_ERR("\nCannot read number of objects %s. \n", optarg);
          exit (EXIT_FAILURE);
        }
        if (num_objs > (uint32_t)MAX_NUM_OBJS || !num_objs) {
          PRINT_ERR("\nInvalid number of objects %s. "
                    "Must be between 1 and %u\n", optarg, MAX_NUM_OBJS);
          exit (EXIT_FAILURE);
        }
        break;
      case 'r':
        if (1 != sscanf(optarg, "%u", &num_receivers)) {
          PRINT_ERR("\nCannot read number of receivers %s. \n", optarg);
          exit (EXIT_FAILURE);
        }
        if (num_receivers > MAX_NUM_RECEIVERS || !num_receivers) {
          PRINT_ERR("\nInvalid number of receivers %s. "
                    "Must be between 1 and %u\n", optarg, MAX_NUM_RECEIVERS);
          exit (EXIT_FAILURE);
        }
        break;
      case 'i':
        if (1 != sscanf(optarg, "%u", &receiver_id)) {
          PRINT_ERR("\nCannot read receiver ID %s. \n", optarg);
          exit (EXIT_FAILURE);
        }
        if (receiver_id > MAX_NUM_RECEIVERS) {
          PRINT_ERR("\nInvalid number of receivers %s. "
                    "Must be between 0 and %u\n", optarg, MAX_NUM_RECEIVERS);
          exit (EXIT_FAILURE);
        }
        break;
      case 'q':
        if (1 != sscanf(optarg, "%u", &queue_len)) {
          PRINT_ERR("\nCannot read queue length %s. \n", optarg);
          exit (EXIT_FAILURE);
        }
        if (queue_len > MAX_QUEUE_LEN || !queue_len) {
          PRINT_ERR("\nInvalid queue len %s. "
                    "Must be between 1 and %u\n", optarg, MAX_QUEUE_LEN);
          exit (EXIT_FAILURE);
        }
        break;
      case 'l':
        if (1 != sscanf(optarg, "%u", ((uint32_t *)&low_water_mark))) {
          PRINT_ERR("\nCannot read low water mark %s. \n", optarg);
          exit (EXIT_FAILURE);
        }
        if (low_water_mark >= MAX_QUEUE_LEN || !low_water_mark) {
          PRINT_ERR("\nInvalid low water mark %s. "
                    "Must be between 1 and %u\n", optarg, MAX_QUEUE_LEN);
          exit (EXIT_FAILURE);
        }
        break;
      case 'e':
        eventfd_flags = EFD_NONBLOCK | EFD_SEMAPHORE;
        break;
      case 'm':
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
      case 'b':
        if (1 != sscanf(optarg, "%u", &batch_size)) {
          PRINT_ERR("\nCannot read batch size %s. \n", optarg);
          exit (EXIT_FAILURE);
        }
        if (batch_size < 1 || batch_size > MAX_BATCH_SIZE) {
          PRINT_ERR("\nInvalid batch_size %s. "
                    "Must be between 1 and %u\n", optarg, MAX_BATCH_SIZE);
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
      case 'u':
        is_use_eventfd_to_pulse_receivers = false;
        break;
      default: /* '?' */
        print_usage(argv[0]);
        return EXIT_FAILURE;
      }
  }
  
  if (optind < argc) {
    PRINT_ERR("\nInvalid number of arguments optind=%d argc=%d.\n",
              optind, argc);
    exit (EXIT_FAILURE);
  }

  if(num_receivers == 1) {
    receiver_id = 0;
  } else {
    /*
     * When I use eventfd with multiple receivers, I can see that a pulse gets
     * lost and we fall into a deadlock after ew hundred thousands to few
     * million objects. Hence I wll NOt use it
     * This may very well be a bug in my code or the fact that a single write
     * to an eventfd cannot be used reliably to wakeup multiple waiters on that
     * same eventfd using epoll_wait()
     */

    if (is_use_eventfd_to_pulse_receivers) {
      PRINT_ERR("eventfd CANNOT with multiple-receivers "
                "because it is NOT RELIABLE\n");
      exit (EXIT_FAILURE);
    }
    /*
     * something occurs when there is nore than one receiver
     * (may be a bug in this code)
     * So I will NOT use eventfd with multiple recievers
     */
    if (receiver_defer_after_each_batch) {
      PRINT_ERR("Receiver defer afer each batch does not currentluy work "
                "with multiple-receivers\n");
      exit (EXIT_FAILURE);
    }
  }
  if (!is_transmitter && !(receiver_id < num_receivers)) {
    PRINT_ERR("Receiver ID '%u' incorrect or NOT specififed. "
              "MUST strictly less than number of receivers %u\n",
              receiver_id, num_receivers);
    exit (EXIT_FAILURE);
  }

  if (!is_power_of_two(queue_len)) {
    PRINT_ERR("queue len is '%u' MUST be a power of two.\n",
              queue_len);
    exit (EXIT_FAILURE);
  }
  
  if (batch_size > queue_len) {
    PRINT_ERR("\nBatch size %u cannot exceed total queue length %u\n",
              batch_size, queue_len);
    exit (EXIT_FAILURE);
  }

  if (low_water_mark > queue_len-1 || !low_water_mark) {
    PRINT_ERR("\nLow water mark %u MUST be strictly less than total queue length %u\n",
              low_water_mark, queue_len/2);
    exit (EXIT_FAILURE);
  }

  if (obj_size > packet_size) {
    PRINT_ERR("\nobj_size %u CANNOT be larger than packet size %u\n",
              obj_size , packet_size)        
    exit (EXIT_FAILURE);
  }
  
  /* The entire shared window sizew in bytes */
  window_size = queue_len * packet_size + sizeof(shm_queue_t);


  
  /* Setup handler for CTRL^C */
  struct sigaction sigIntHandler;
  sigIntHandler.sa_handler = signal_handler;
  sigemptyset(&sigIntHandler.sa_mask);
  sigIntHandler.sa_flags = 0;  
  if (sigaction(SIGINT, &sigIntHandler, NULL) != 0) {
    PRINT_ERR("Cannot setup signal handler: %d %s\n",
              errno, strerror(errno));
    exit(EXIT_FAILURE);
  }


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
        PRINT_DEBUG("Queue %s does not exist\n", queue_name);;
      }
    } else {
      PRINT_DEBUG("Successfully delete Queue '%s' \n", queue_name);;
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
    queue = mmap(0,
                 window_size,
                 PROT_READ | PROT_WRITE,
                 MAP_SHARED, /* MUST have for file-backed window */
                 shm_fd, 0);
    if (!queue) {
      PRINT_ERR( "\nCannot map '%s' %u bytes shm_fd %d: %d %s", queue_name,
                 window_size, shm_fd,
                 errno, strerror(errno));
      return EXIT_FAILURE;
    }
    /*
     * Init the queue
     */
    memset(queue, 0, window_size);
    memset(queue->packets, 0xffffffff, window_size - offsetof(shm_queue_t, packets));
    queue->queue_len = queue_len;
    queue->packet_size = packet_size;
    queue->obj_size = obj_size;
    queue->window_size = window_size;
    queue->num_receivers =  num_receivers;
    queue->is_adaptive_mutex = is_adaptive_mutex;
    for (i = 0; i < num_receivers; i++) {
      queue->receiver_queue[i].receiver_id = UINT32_MAX;
    }

    /*
     * Init the mutex
     */
    pthread_mutexattr_t mattr;
    if ((rc = pthread_mutexattr_init(&mattr))) {
      PRINT_ERR("Cannot init mutex attribute: %d %s\n", rc, strerror(rc));
      return (EXIT_FAILURE);
    }
    if ((rc = pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED))) {
      PRINT_ERR("Cannot set mutex attribute: %d %s\n", rc, strerror(rc));
      return (EXIT_FAILURE);
    }
    if (is_adaptive_mutex) {
      pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_ADAPTIVE_NP);
    }
    if ((rc = pthread_mutex_init(&queue->mutex, &mattr))) {
      PRINT_ERR("Cannot init mutex : %d %s\n", rc, strerror(rc));
      /* Destroy the mutex attribute */
      return (EXIT_FAILURE);
    }
    pthread_mutexattr_destroy(&mattr);
    
    PRINT_DEBUG("Successfully created queue at %p with %u elements each size %u "
                "total size %u\n",
                queue, queue->queue_len, queue->packet_size,
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
    queue = mmap(0,
                 sizeof(*queue),
                 PROT_READ | PROT_WRITE,
                 MAP_SHARED, /* MUST have for file-backed window */
                 shm_fd, 0);
    if (!queue) {
      PRINT_ERR( "\nCannot map '%s' %zu bytes shm_fd %d: %d %s", queue_name,
                 sizeof(*queue), shm_fd,
                 errno, strerror(errno));
      return EXIT_FAILURE;
    }
    if (queue->obj_size != obj_size) {
      PRINT_ERR( "Invalid objsize %u expecting %u  '%s' %u bytes shm_fd %d: %d %s\n",
                 queue->obj_size, obj_size,
                 queue_name,
                 window_size, shm_fd,
                 errno, strerror(errno));
      return EXIT_FAILURE;
    }
    queue_len = queue->queue_len;
    packet_size = queue->packet_size;
    obj_size = queue->obj_size;
    window_size = queue->window_size;
    is_adaptive_mutex = queue->is_adaptive_mutex;
    /*
     * Unmap then remap using the size obtained from reading the queue header
     */
    if (munmap(queue, sizeof(*queue)) < 0) {
      PRINT_ERR("Cannot unmap %p size %zu shm_fd %d name '%s': %d %s\n",
                queue, sizeof(*queue), shm_fd,
                queue_name,
                errno, strerror(errno));
      return EXIT_FAILURE;
    }

    queue = mmap(0,
                 window_size,
                 PROT_READ | PROT_WRITE,
                 MAP_SHARED, /* MUST have for file-backed window */
                 shm_fd, 0);
    if (!queue) {
      PRINT_ERR( "\nCannot RE-map '%s' %u bytes shm_fd %d: %d %s", queue_name,
                 window_size, shm_fd,
                 errno, strerror(errno));
      return EXIT_FAILURE;
    }

    /*
     * Let's do some validation
     */
    if (num_receivers != queue->num_receivers) {
      PRINT_ERR( "\nUser asked for num_receivers %u but sender expects %u. "
                 "window '%s' shm_fd %d\n",
                 num_receivers, queue->num_receivers,
                  queue_name, shm_fd);
      return EXIT_FAILURE;
    }
    if (queue->receiver_queue[receiver_id].receiver_id != UINT32_MAX) {
      PRINT_ERR( "\nUser asked for receiver_id %u "
                 "but is already taken by id %u. "
                 "window '%s' shm_fd %d\n",
                 receiver_id, queue->receiver_queue[receiver_id].receiver_id,
                  queue_name, shm_fd);
      return EXIT_FAILURE;
    }

    /* Record our receiver ID */
    queue->receiver_queue[receiver_id].receiver_id = receiver_id;
  }

  /* Number of objects to pack/unpack into/from a packet */
  objs_per_packet = (queue->packet_size - (uint32_t)sizeof(packet_t))/obj_size;


  /* Print the shared memory queue parameters */
  PRINT_INFO("\n%s Shared memory queue:\n"
             "Name:                                         %s\n"
             "Queue Len:                                    %u\n"
             "Low water Mark:                               %u\n"
             "Adaptive mutex                                %s\n"
             "Single packet size:                           %u\n"
             "Single payload size:                          %lu\n"
             "Object size:                                  %u\n"
             "Objects per packet:                           %u\n"
             "Max single Batch Size (in packets):           %u\n"
             "Max batch size (in bytes):                    %u\n"
             "Number of objects in a max batch:             %u\n"
             "Total payload size in a max batch in bytes:   %u\n"
             "Total number of objects to %s           %u\n"
             "Total queue size in bytes:                    %u\n"
             "Total packet buffer in bytes:                 %lu\n"
             "Slow factor                                   %u\n"
             "%s               %s\n",
             is_transmitter ? "Transmitter" : "Receiver",
             queue_name,
             queue->queue_len,
             low_water_mark,
             is_adaptive_mutex ? "true" : "false",
             queue->packet_size,
             queue->packet_size - offsetof(packet_t, data),
             obj_size,
             objs_per_packet,
             batch_size,
             batch_size * queue->packet_size,
             batch_size * objs_per_packet,
             batch_size * objs_per_packet * queue->obj_size,
             is_transmitter ? "send:   " : "receive:", num_objs,
             window_size,
             window_size - offsetof(shm_queue_t, packets),
             slow_factor,
             is_transmitter ? "" : "receiver_defer_after_each_batch",
             is_transmitter ? "" : 
             (receiver_defer_after_each_batch ? "true\n" : "false\n"));
  
  
  
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
  memset(obj, 0xeeeeeeee, obj_size);
  memset(obj->data, 0xeeeeeeee, (obj_size - offsetof(obj_t, data)));
  obj->counter = 0;
 
  if (is_transmitter) {
    /*
     * Transmitter mode
     * This is the step where the tranismitter receivers a connection from a
     * receiver and send the eventfd that the transmitter uses to tell the
     * receiver that the transmitter has queued a packet
     */


    /* Bind it to the sockpath */
    bind_to_sock_path(sock_fd);

    /*
     * Put socket in listen mode
     */
    if (listen(sock_fd, MAX_NUM_RECEIVERS) == -1) {
      PRINT_ERR("listen() sock_fd %d backlong=%u "
                "failed : %d %s\n",
                sock_fd, MAX_NUM_RECEIVERS, errno, strerror(errno));
      exit(EXIT_FAILURE);
    }
    
    /*
     * prepare the eventfd file descriptors
     * Rememmber that we need TWO eventfd
     * - receiver_wakeup_eventfd_fd: this is the file descriptor that the transmitter
     *   will write() to when the transmitter queues a single batch of packets
     *   The receiver will ALWAYS wait on this file descriptor when there are
     *   no longer packets queued in the queue
     * - sender_wakeup_eventfd_fd: If the sender attempted to get a packet to
     *   pack objects inside it and finds tha tthe queue is full (because the
     *   receiver is too slow), then the sender will wait on this fd
     *   When the receiver attempts to dequeue a packet and finds that the
     *   queue iss full, then after dequeuing to the LOW WATER MARK, the
     *   receiver will "write()" to this FD so that the sender knws that the
     *   queue is available and it starts sending again
     * - We will NOT use the semaphore mode because we want to the server to do
     *   one read to wakeup receivers from poll a
     * - It has to be non-blocking to work with pol() or select()
     */
    sender_wakeup_eventfd_fd = eventfd(0,eventfd_flags);
    if (sender_wakeup_eventfd_fd == -1) {
      PRINT_ERR("sender_wakeup_eventfd_fd failed with flags 0x%x: %d %s\n",
                eventfd_flags,
                errno, strerror(errno));
      exit(EXIT_FAILURE);
    }
    receiver_wakeup_eventfd_fd = eventfd(0,eventfd_flags);
    if (receiver_wakeup_eventfd_fd == -1) {
      PRINT_ERR("receiver_wakeup_eventfd_fd failed with flags 0x%x: %d %s\n",
                eventfd_flags,
                errno, strerror(errno));
      exit(EXIT_FAILURE);
    }
    /* Now store therse FDs in the array so that they can be sent into an
       SCM_RIGHTS message
    */
    scm_fds[0] = sender_wakeup_eventfd_fd;
    scm_fds[1] = receiver_wakeup_eventfd_fd;    

    PRINT_DEBUG("Going to wait on sock_fd %d for %u receivers.\n",
                sock_fd, num_receivers);
    /*
     * Loop until all receivers connect
     * For each receiver connection, 
     * - send SCM_RIGHTS message containing the eventfd file descriptors
     */
    for (i = 0; i < num_receivers; i++) {
      remote_len = sizeof(sockaddr_remote);
      if ((server_accept_fd[i] = accept(sock_fd,
                                        (struct sockaddr *)&sockaddr_remote,
                                        &remote_len)) == -1) {
        PRINT_ERR("accept() receiver (%d) sock_fd %d remote_len=%u "
                  "failed : %d %s\n",
                  i, sock_fd, remote_len, errno, strerror(errno));
        exit(EXIT_FAILURE);
      }

      PRINT_INFO("Connected to receiver %d accept_fd %d  waiting for %u.\n",
                 i, server_accept_fd[i], num_receivers - (i + 1));
      /*
       * Send the SCM_RIGHTS message containing the eventfd
       */
      if (send_fds(server_accept_fd[i],
                   scm_fds, 2) < 1) {
        PRINT_ERR("send_fds() to receiver %d for eventfd %d %d failed : %d %s\n", i,
                  scm_fds[0], scm_fds[1],
                  errno, strerror(errno));
        exit(EXIT_FAILURE);
      }
    }
    PRINT_INFO("Connected to %u receivers..\n\n",num_receivers);

    /*
     * prepare the epoll for sender so that we can wait on it if we find that
     * the queue is full
     */
    event.events = EPOLLIN;
    event.data.fd = sender_wakeup_eventfd_fd;
    int j = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sender_wakeup_eventfd_fd, &event);
    if (j < 0) {
      PRINT_ERR("epoll_ctl failed for epoll_fd %d sender_wakeup_evenfd %d: %d %s\n",
                epoll_fd, sender_wakeup_eventfd_fd, errno, strerror(errno));
        exit(EXIT_FAILURE);
    } else {
      PRINT_DEBUG("Successfully called epoll_ctl() epoll_fd=%d "
                  "sender_wakeup_evenfd %d \n",
                  epoll_fd, sender_wakeup_eventfd_fd);
    }

    
    /* Sleep for 1 second after all receivers connect to make sure that all
     * receivers have called poll() or read to wait on the eventfd
     */
    /*PRINT_INFO("Waiting to let receivers wait on the eventfd() filedescriptor\n");
      sleep(2);*/
      
    
  } else {
    /* Receiver mode 
     * This is the step where the receiver connects to the transmitter
     * then receives the eventfd that the transmitter uses to tell the
     * receiver that the transmitter has queued a packet
     */
    sockaddr_remote.sun_family = AF_UNIX;
    strcpy(sockaddr_remote.sun_path, sock_path);
    remote_len =
      strlen(sockaddr_remote.sun_path) + sizeof(sockaddr_remote.sun_family);
    
    PRINT_DEBUG("going to connect to sock_path %s...\n",
                sockaddr_remote.sun_path);
    if (connect(sock_fd,
                (struct sockaddr *)&sockaddr_remote,
                remote_len)){
      PRINT_ERR("FAILED connect() sock_fd %d remote_len=%u: %d %s\n",
                sock_fd, remote_len, errno, strerror(errno));
      exit(EXIT_FAILURE);
    }
    
    PRINT_DEBUG("Connected to server sock_fd %d len %d path '%s'.\n",
                sock_fd,remote_len, sockaddr_remote.sun_path);
    /* receiver the SCM_RIGHTS message containing the eventfd that the
     * transmitter uses to tell the receiver that the transmitter has queued a
     * packet */
    scm_fds[0] = -1;
    scm_fds[1] = -1;
    if (recev_fds(sock_fd,
                  scm_fds, 2) < 1) {
        PRINT_ERR("FAILED rec_fds() to receiver %d for eventfd %d %d: %d %s\n", i,
                  scm_fds[0], scm_fds[1],
                  errno, strerror(errno));
        exit(EXIT_FAILURE);
    } else {
      sender_wakeup_eventfd_fd = scm_fds[0];
      receiver_wakeup_eventfd_fd = scm_fds[1];
      PRINT_DEBUG("Received sender eventfd %d and receiver fd %d\n",
                  sender_wakeup_eventfd_fd,
                  receiver_wakeup_eventfd_fd);
    }

    /*
     * prepare the epoll so that if the queue is empty the receiver can wait on
     * it for a signal from the senderr 
     */
    event.events = EPOLLIN;
    event.data.fd = receiver_wakeup_eventfd_fd;
    rc = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, receiver_wakeup_eventfd_fd, &event);
    if (rc < 0) {
      PRINT_ERR("epoll_ctl failed for epoll_fd %d receiver_wakeup_evenfd %d: %d %s\n",
                epoll_fd, receiver_wakeup_eventfd_fd, errno, strerror(errno));
        exit(EXIT_FAILURE);
    } else {
      PRINT_DEBUG("Successfully called epoll_ctl() epoll_fd=%d receiver_wakeup_evenfd %d \n",
                  epoll_fd, receiver_wakeup_eventfd_fd);
    }

    /*
     * We also want to reguister for sender disconnect
     */
    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN | EPOLLRDHUP;
    event.data.fd = sock_fd;
    rc = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock_fd, &event);
    if (rc < 0) {
      PRINT_ERR("epoll_ctl failed for epoll_fd %d sock_fd %d: %d %s\n",
                epoll_fd, sock_fd, errno, strerror(errno));
        exit(EXIT_FAILURE);
    } else {
      PRINT_DEBUG("Successfully called epoll_ctl() epoll_fd=%d soc_fd %d \n",
                  epoll_fd, sock_fd);
    }
    
  } /* else of "if (is_transmitter)" */

  /*
   * If I am a transmitter, let me go through the transmission loop
   */
  if (is_transmitter) {
    bool is_pulse_receiver;
    /* For the transmitter, we will be working on all receivers */
    shm_receiver_queue_t *receiver_queue =  &queue->receiver_queue[0];
    
    /*
     * Let's send initial pulse over the AF_UNIX socket to allow recievers to
     * almost take the initial timestamp at the same time
     */
    PRINT_INFO("Going to send synchronization pulse to %u receivers\n",
               num_receivers);
    if (is_use_eventfd_to_pulse_receivers) {
      PULSE_RECEIVER(num_receivers, is_transmitter);
    } else {
      for (i = 0; i < num_receivers; i++) {
        uint8_t pulse = 1;
        rc = write(server_accept_fd[i], &pulse, sizeof(pulse));
        if (!rc) {
          PRINT_ERR("Cannot send initial pulse to receiver %u accept_fd %d "
                    "queue= (%s): "
                    "%d %s\n",
                    i,
                    server_accept_fd[i],
                    print_queue(queue),
                    errno, strerror(errno));
          ret_value = EXIT_FAILURE;
          goto out;
        } else {
          PRINT_DEBUG("Successfully sent Synchronization pulse to receiver %u\n", i);
          receiver_queue[i].receiver_waiting_for_pulse_from_sender = false;
        }
      }
    }
      
    
    /* get time stamp when starting to send */
    clock_gettime(CLOCK_MONOTONIC, &start_ts);

    uint32_t sequence = 0;
    while (num_objs > num_sent_received_objs) {
      /* No receiver needs pulsing unless we know that */
      is_pulse_receiver = false;

      /*Init batch size to what user wants */
      curr_batch_size = batch_size;

      /*
       * If remaining objecets fit into one packet, then reduce batch to 1
       */
      if (num_objs - num_sent_received_objs < objs_per_packet) {
        curr_batch_size = 1;
        objs_per_packet = num_objs - num_sent_received_objs;
      }
      /*** Lock the queue ***/
      QUEUE_LOCK;
      for (i = 0; i < num_receivers; i++) {
        /*
         * If a queue length is zero, then we have to wakeup that receiver
         */
        if (!receiver_queue[i].num_queued_packets) {
          is_pulse_receiver = true;
        }
        /*
         * If the batch size is greater than the empty queue space, then
         * reduce the batch size to the remaining number of packets
         * It possible that while we are queueing these packets that the
         * receiver will dequeue some packets and hence the remaining queue
         * length will increas. So after sending the adjusted batch, we will
         * check to see if the queue is still full
         * However we do NOT want to lock/unlock the queue after every packet to
         * check if it is possible to increas the batch size
         */
        if (curr_batch_size >
            queue->queue_len - receiver_queue[i].num_queued_packets) {
          curr_batch_size =
            queue->queue_len - receiver_queue->num_queued_packets;
        }
      }
        /*** Unlock the queue ***/
      QUEUE_UNLOCK;

      /*if (curr_batch_size > (uint32_t)batch_size) {
        curr_batch_size = (uint32_t)batch_size;
        }*/
      
      /* REDUCE objects per packet and/or batch size according to remaining
         number of objects */
      if ((num_objs - num_sent_received_objs)
                 < objs_per_packet * curr_batch_size) {
        curr_batch_size = (num_objs - num_sent_received_objs)/objs_per_packet;
      }

      /*
       * NOw let's queue one batch of packets
       * we already adjusted the current batch size for queue
       */
      for (i = 0; i < (uint32_t)curr_batch_size; i++) {
        packet =
          (packet_t *)&queue->packets[((queue->tail + i) & (queue->queue_len - 1)) * (uint32_t)queue->packet_size];
        packet->sequence = sequence;
        data = &packet->data[0];
        for (j = 0; j < objs_per_packet; j++) {
          /* Inc object counter so that we detect loss */
          memcpy(data, obj, obj_size);
          PRINT_DEBUG("***Sent object counter %u %uth packet sequence %u tail %u, "
                      "from data %p packet %p\n"
                      "\tSo far sender_wakeup_needed %u lost objects %u.\n"
                      "\tSo sent %d objects %u packets %u batchs\n"
                      "\tqueue=(%s)\n",
                      obj->counter, i, packet->sequence, queue->tail, data, packet,
                      num_sender_wakeup_needed,
                      num_lost_objs,
                      num_sent_received_objs,
                      num_packets,
                      num_batchs,
                      print_queue(queue));
          obj->counter++;
          data = (uint8_t *)((uintptr_t)(data) + (uintptr_t)obj_size);
          num_sent_received_objs++; /* inc number of packed objects */
        } /* for (j = 0; j < objs_per_packet; j++) */
        /* record objects packed in packet */
        packet->num_objs = objs_per_packet ;
        /* Inc number of packed packets */
        num_packets++; 
        sequence++; /* Inc sequence number of packets */
      } /* for (i = 0; i < (uint32_t)curr_batch_size; i++)*/
      /* Inc number of queued batchs */
      if (curr_batch_size) {
        num_batchs++;
      } else {
        num_sender_zero_curr_batch_size++;
      }

      /* 
       * advance the tail and the number of queued packets by the batch size 
       * If the queue is full, then 
       * - We will pulse the receiver
       * - we will wait for the receiver(s) pulse
       * 
       * NOTE: Why we MUST pulse the receiver when we wait for receiver's pulse?
       * Consider the following scenario that would lead to a deadlock
       * - The receiver drained all packets in the queue hence it will be
       *   waiting for the sender's pulse
       *   REMEMBER that
       *     while draining packets, the receiver does NOT lock the queue mutex
       *     Instead it will wait till it dequeues a batch, then lock
       *     the queue mutex and update the "head" and "num_queued_packets"
       * - The sender is queueing packets into the queue
       *   REMEMBER also that while queueing packets, the sender does NOT lock 
       *            the queue mutex
       *            Instead it waits till it finished the current batch or
       *            fills up the queue
       * - Even though the receiver locks the mutex when it checks whether the
       *   sender reached the high water mark, because the sender updates the
       *   queue after it finishes queueing, it is possible that the receiver
       *   sees that the sender did NOT reach the high water mark even though
       *   the sender has actually reached the high water mark
       * - Hence by the time the sender locks the mutex and decides that it has
       *   reached the high water mark, the sender would have thought that the
       *   queue is empty and will be waiting for a pulse from the sender
       * - At the same time the sender would be waiting for a pulse from the
       *   receiver
       * - Hence to be on the safe side, the sender will always pulse the
       *   receiver when it reaches the high water mark
       */
      bool is_sender_wakeup_needed = false;
      QUEUE_LOCK;
      /* Increment for all receivers in one shot because we have 1 tail */
      QUEUE_TAIL_INC(curr_batch_size);
      /* check if we need to pulse any receiver to avoid possible deadlock as
         mentioned above */
      for (i = 0; i < num_receivers; i++) {
        /* Advance each receive tail and queue_len*/
        PRINT_DEBUG("***ADVANCED %uth TAIL by curr_batch %u object counter %u "
                    "last sequence %u tail %u, "
                    "from data %p packet %p\n"
                    "\tSo far sender_wakeup_needed %u lost objects %u.\n"
                    "\tSo sent %d objects %u packets %u batchs\n"
                    "\tqueue=(%s)\n",
                    i,
                    curr_batch_size,
                    obj->counter, packet->sequence, queue->tail, data,
                    packet,
                    num_sender_wakeup_needed,
                    num_lost_objs,
                    num_sent_received_objs,
                    num_packets,
                    num_batchs,
                    print_queue(queue));
        
        if (queue->queue_len ==
            receiver_queue[i].num_queued_packets) {
          receiver_queue[i].high_water_mark_reached = true;
          is_sender_wakeup_needed = true;
          /* As mentioned above, we will wakeup receiver to avoid possible
             deadlock */
          if (receiver_queue[i].receiver_waiting_for_pulse_from_sender) {
            is_pulse_receiver = true;
            /* Clear the need for pulse receiver because we will send it within
               the next few instructions */
            if (is_use_eventfd_to_pulse_receivers) {
              receiver_queue[i].receiver_waiting_for_pulse_from_sender = false;
            }
          }
        }
      } /*  for (i = 0; i < num_receivers; i++) */
      QUEUE_UNLOCK;

      /* Send pulse if at least one receiver needs to be waken up 
       * We want to do that BEFORE we check if we have reached high water
       * because if at least one receiver reached high water mark we will block
       * and we want to make sure that all receviers are unlbockied to be able
       * to unblock the sender out 
       */
      if(is_pulse_receiver) {
        if (is_use_eventfd_to_pulse_receivers) {
          PULSE_RECEIVER(num_receivers, is_transmitter);
        } else {
          uint8_t pulse = 1;
          for (i = 0; i < num_receivers; i++) {
            rc = write(server_accept_fd[i], &pulse, sizeof(pulse));
            if (!rc) {
              PRINT_ERR("Cannot send pulse to receiver %u accept_fd %d "
                        "curr_batch siz %u "
                        "last sequence %u tail %u, "
                        "\tSo far sender_wakeup_needed %u lost objects %u.\n"
                        "\tSo sent %d objects %u packets %u batchs\n"
                        "\tqueue= (%s): "
                        "%d %s\n",
                        i,
                        server_accept_fd[i],
                        curr_batch_size,
                        sequence, queue->tail,
                        num_sender_wakeup_needed,
                        num_lost_objs,
                        num_sent_received_objs,
                        num_packets,
                        num_batchs,
                        print_queue(queue),
                        errno, strerror(errno));
              ret_value = EXIT_FAILURE;
              goto out;
            } else {
              PRINT_DEBUG("Successfully pulsed receiver %u\n", i);
              receiver_queue[i].receiver_waiting_for_pulse_from_sender = false;
            }
          }
        }
        is_pulse_receiver = false; /* NO need anymore because I just
                                      pulsed all receiver(s)*/
      }

      /*
       * If at least the queue of one of the receivers has reached high water
       * mark, we have to block until that receiver (or other receiver) wakes
       * us up */
      if (is_sender_wakeup_needed) {
        /* If sender reached high water mark for at least one receiver, Inc
         * number of times sender will block until receiver drains queue
         * because of reaching high water mark */
        num_sender_wakeup_needed ++;

        PRINT_DEBUG("Going to wait on sender_wakeup fd %d,\n "
                  "\tSo far sender_wakeup_needed %u.\n"
                   "\tSo far sent %d objects %u packets %u batches batch size %d\n"
                    "\tqueue=(%s)\n",
                    sender_wakeup_eventfd_fd,
                    num_sender_wakeup_needed,
                    num_sent_received_objs,
                    num_packets,
                    num_batchs,
                    curr_batch_size,
                    print_queue(queue));

        /* wait for some receiver to wake us up */
        rc = epoll_wait(epoll_fd, &event, 1, -1);
        if (rc < 0) {
          PRINT_ERR("epoll_wait failed for epoll_fd %d sender_evenfd %d "
                    "queue=(%s): %d %s\n",
                    epoll_fd,  sender_wakeup_eventfd_fd,
                    print_queue(queue), errno, strerror(errno));
          exit(EXIT_FAILURE);
        } else {
          PRINT_DEBUG("Successfully came out of epoll_wait epoll_fd=%d "
                      "sender_evenfd %d queue=(%s)\n",
                      epoll_fd, sender_wakeup_eventfd_fd,
                      print_queue(queue));
          /* the eventfd are ruinning in semaphore moddde. Hence we have read to
           * decrement, otherrrwise we will continue to be waken up */
          uint64_t eventfd_read;
          read(sender_wakeup_eventfd_fd, &eventfd_read, sizeof(eventfd_read));
        }
      } else {
        PRINT_DEBUG("NO NEED for sender_wakeup fd %d,\n "
                    "\tSo far sender_wakeup_needed %u.\n"
                    "\tSo far sent %d objects %u packets %u batches batch size %d\n"
                    "\tqueue=(%s)\n",
                    sender_wakeup_eventfd_fd,
                    num_sender_wakeup_needed,
                    num_sent_received_objs,
                    num_packets,
                    num_batchs,
                    curr_batch_size,
                    print_queue(queue));
      }
      /* Slow ourselves if we requested */
      for (i = 0; i < slow_factor; i++) {
        static uint32_t __attribute__ ((unused)) a;
        uint32_t b = 5000, c = 129;
        a = b * c;
      }
      
    }
    /* get time stamp when finish sending */
    clock_gettime(CLOCK_MONOTONIC, &end_ts);
  }
  


  /*
   * If I am a receiver I wil go through the receiving loop
   */
  if (!is_transmitter) {
    uint32_t counter = 0;
    uint32_t sequence = 0;
    bool pulse_myself = false;
    bool sender_exited = false;
    uint32_t num_epoll_wait = 0; /* Number of times we waited in epoll_wait */
    int sender_exited_fd = -1; /* Sender's FD when we get EPOLLRDHUP */
    /* For a receiver, we will be working on the head and queue_len of that
       PARTICULAR receiver only except in one or two cases */
    shm_receiver_queue_t *receiver_queue = &queue->receiver_queue[receiver_id];


    /*
     * Wait for the synchronization pulse before taking the time stamp
     */
    rc = epoll_wait(epoll_fd, &event, 1, -1);
    if (rc < 0) {
      PRINT_ERR("Initial pulse epoll_wait failed for epoll_fd %d "
                "received_evenfd %d"
                "\tqueue=(%s): %d %s\n",
                epoll_fd,  receiver_wakeup_eventfd_fd,
                print_queue(queue), errno, strerror(errno));
      ret_value = EXIT_FAILURE;
      goto out;
    }
    /*
     * If we got EPOLLRDHUP, then the sender has exited. So we will exit
     */
    if (event.events & EPOLLRDHUP) {
      PRINT_INFO("Receiver %u exitted PREMATURELY\n", receiver_id);
      ret_value = EXIT_SUCCESS;
      goto out;
    }

    /* read the pulse that woke us up */
    READ_PULSE(is_use_eventfd_to_pulse_receivers);

    PRINT_INFO("Receiver %u received synchronization pulse\n", receiver_id);

    /* get time stamp when start receiving */
    clock_gettime(CLOCK_MONOTONIC, &start_ts);

    PRINT_DEBUG("Going to call epoll_wait() on epoll_fd=%d for evenfd %d. \n",
                epoll_fd, receiver_wakeup_eventfd_fd);
    QUEUE_LOCK;
    receiver_queue->receiver_waiting_for_pulse_from_sender = true;
    QUEUE_UNLOCK;    
    while((rc = epoll_wait(epoll_fd, &event, 1, -1)) > 0) {
      num_epoll_wait++;

      PRINT_DEBUG("Came out of epoll_wait %u times epoll_fd=%d "
                  "receiver_wakeup_evenfd %d num_epoll_wait %u queue=(%s)\n",
                  num_epoll_wait,
                  epoll_fd, receiver_wakeup_eventfd_fd,
                  num_epoll_wait,
                    print_queue(queue));
      /*
       * If we got EPOLLRDHUP, then the sender has exited. So we will exit
       */
      if (event.events & EPOLLRDHUP) {        
        sender_exited = true;
        sender_exited_fd =  event.data.fd;
      } else {
        /* the receive_wakeup_eventfd_fd is running in semaphore moddde. Hence
         * we have read to decrement, otherrrwise we will continue to be waken
         * up */
        READ_PULSE(is_use_eventfd_to_pulse_receivers);
      }
    

      /* User have to option to continue instead of pulsing ourself */
    begin_receive_loop:
      
      QUEUE_LOCK;
      /*
       * Adjust the batch size according to the currently queued packets
       *
       * remember that when the sender reaches high water mark, it will always
       * pulse the receiver. Hence depending on the queu size, it is possible
       * by the time that extra pulse is received by the reaciver, the receiver
       * may have already empties the queue. Hence it is OK to have a batch
       * size of zero
       */
      curr_batch_size = batch_size;
      if (receiver_queue->num_queued_packets < curr_batch_size) {
        curr_batch_size = receiver_queue->num_queued_packets;
      }
      QUEUE_UNLOCK;

      /*
       * Let's dequeue one batch and read the contents
       */
      for (i = 0; i < (uint32_t)curr_batch_size; i++) {
        packet =
          (packet_t *)&queue->packets[((receiver_queue->head + i) & (queue->queue_len - 1)) * (uint32_t)queue->packet_size];
        data = &packet->data[0];
        if (packet->sequence != sequence) {
          num_lost_packets++;
          PRINT_ERR("PACKET LOSS detected: received SEQUENCE %u, expecting %u "
                    "curr_batch_size %u head %u from %uth packet %p data %p \n"
                    "\tSo far sender_wakeup_needed %u lost objects %u lost packets %u.\n"
                    "\tSo received %d objects %u packets %u batchs\n"
                    "\tqueue=(%s)\n",
                    packet->sequence, sequence,
                    curr_batch_size, receiver_queue->head, i, packet, data,
                    num_sender_wakeup_needed,
                    num_lost_objs,
                    num_lost_packets,
                    num_sent_received_objs,
                    num_packets,
                    num_batchs,
                    print_queue(queue));
          ret_value = EXIT_FAILURE;
          goto out;
        }
        for (j = 0; j < packet->num_objs; j++) {
          memcpy(obj, data, obj_size);
          /* Inc object counter so that we detect loss */
          if (obj->counter > counter) {
            PRINT_ERR("LOSS detected: received counter %u, expecting %u "
                      "from data %p \n"
                      "\tSo far sender_wakeup_needed %u lost objects %u.\n"
                      "\tSo received %d objects %u packets %u batchs\n"
                      "\tqueue=(%s)\n",
                      obj->counter, counter, data,
                      num_sender_wakeup_needed,
                      num_lost_objs,
                      num_sent_received_objs,
                      num_packets,
                      num_batchs,
                      print_queue(queue));
            counter++;
            ret_value = EXIT_FAILURE;
            goto out;
            /*exit(EXIT_FAILURE);*/
          } else if (obj->counter < counter)   {
            PRINT_ERR("Invalid received counter %u, expecting %u in %u packet (%s) "
                      "%uth obj in packet curr_batch_size %u datap %p packet %p\n"
                      "\tSo far sender_wakeup_needed %u lost objects %u.\n"
                      "\tSo received %d objects %u packets %u batchs\n"
                      "\tqueue=(%s)\n",
                      obj->counter, counter,
                      i, print_packet(packet), j, curr_batch_size, data, packet,
                      num_sender_wakeup_needed,
                      num_lost_objs,
                      num_sent_received_objs,
                      num_packets,
                      num_batchs,
                      print_queue(queue));
            counter++;
            ret_value = EXIT_FAILURE;
            goto out;
            /*exit(EXIT_FAILURE);*/
          } else {
            PRINT_DEBUG("***ID %u Recevied object counter %u, expecting %u "
                        "%uth packet sequence %u at head %u from data %p \n"
                        "\tSo far sender_wakeup_needed %u lost objects %u.\n"
                        "\tSo received %d objects %u packets %u batchs\n"
                        "\tqueue=(%s)\n",
                        receiver_id,
                        obj->counter,
                        counter, i, packet->sequence,
                        receiver_queue->head, data,
                        num_sender_wakeup_needed,
                        num_lost_objs,
                        num_sent_received_objs,
                        num_packets,
                        num_batchs,
                        print_queue(queue));
            counter++;
          }
          data = (uint8_t *)((uintptr_t)(data) + (uintptr_t)obj_size);  
          num_sent_received_objs++; /* inc number of unpacked objects */
        }
        num_packets++; /* Inc number of packed packets */
        sequence++; /* Inc expected packet sequence number */
      } /* for (i = 0; i < (uint32_t)curr_batch_size; i++) { */
      /* Inc number of queued batchs */
      num_batchs = curr_batch_size ? num_batchs + 1 : num_batchs; 

      
      QUEUE_LOCK;
      /* Update the queue after dequeueing an entire batch */
      QUEUE_HEAD_INC(curr_batch_size, receiver_id);
      PRINT_DEBUG("***ADVANCED HEAD by curr_batch %u object counter %u %uth packet sequence %u tail %u, "
                  "from data %p packet %p\n"
                  "\tSo far sender_wakeup_needed %u lost objects %u.\n"
                  "\tSo sent %d objects %u packets %u batchs\n"
                  "\tqueue=(%s)\n",
                      curr_batch_size,
                  obj->counter, i,
                  packet->sequence, queue->tail, data, packet,
                  num_sender_wakeup_needed,
                  num_lost_objs,
                  num_sent_received_objs,
                  num_packets,
                  num_batchs,
                  print_queue(queue));
      /*
       * If there are still some packets in the queue, then we need to pulse
       * ourselves so that we wakeup and continue receiving the data
       */
      pulse_myself = !!(receiver_queue->num_queued_packets);

      /*
       * If I am NOT going to pulse myself then this means that the queue is
       * empty and hence  I will need a wakeup pulse from the sender
       */
      receiver_queue->receiver_waiting_for_pulse_from_sender = !pulse_myself;
      num_receiver_wakeup_needed +=
        receiver_queue->receiver_waiting_for_pulse_from_sender;

      /*
       * If, before, while, or after we draining this batch, sender exceeded
       * high water mark AND we have went below the low water mark AFTER
       * draining this batch, then pulse the sender Otherwise do NOT pulse the
       * sender
       */
      if (receiver_queue->high_water_mark_reached) {
        /*
         * NOTE: V.I.
         * Why it may be SAFE to unlock the queue BEFORE we check whether we
         * have dequeued enough packets to go below the low water mark and Why
         * we will NOT do it ?
         * - If high_water_mark_reached is true, then the sender is waiting on
         *   the "sender_wakeup_eventfd_fd"
         * - Hence the contentns of the queue CANNOT be modified by someone else
         * - Hence it is OK to check for the contents of the queue without
         *   locking   it
         * - Hoiwever if we allow multiple threads to dequieue the same queue
         *   I.e. it is a single receiver with multiple threads,  i.e. a packet
         *   may be dequeued by more than one thread but it is dequeued once,
         *   then it is no longer safe to do that because 
         *   - by the time we come  here another thread may have already brought
         *     up the queue to below low water mark 
         *   - HEnce that other thread and the other threads may have already
         *     pulsed the sender
         *   - Hence the sender may have already wakenfd, up and started queuing
         *     packets and hence modifuying queue->num_queued_packet
         *   - Because we are not 100% sure that checking  an integer is an
         *     atomic operation, then we will lock the mutex to be on the safe side
         *   - The other reason is that because the sender may be waken up, the
         *     sender may have already queued enough packets to fill up the
         *     queue an d hence rasing it back ab ove the low water mark
         */
        if (receiver_queue->num_queued_packets < low_water_mark) {
          /* no longer in high water mark state as the queue len for this
             receiover went down */
          receiver_queue->high_water_mark_reached = false;
          /*
           * If we are the last receiver reaching the high water mark then
           * going below the low water mark, then we will pulse the
           * receiver.
           * Otherwise we will NOT pulse it because the last one will
           */
          bool is_last_receiver_reach_low_water_mark = true;
          for (i = 0; i < num_receivers; i++) {
            if (queue->receiver_queue[i].high_water_mark_reached) {
              is_last_receiver_reach_low_water_mark = false;
              break;
            }
          }
          /* Unlock AFTER we check if we went below the low water mark */
          QUEUE_UNLOCK;
          /* Wakeup the sender, which is waiting until queue is below water
             mark*/
          if (is_last_receiver_reach_low_water_mark) {
            PULSE_SENDER;
            num_last_receiver_reach_low_water_mark++;
          } else {
            num_non_last_receiver_reach_low_water_mark++;
          }
        } else {
          /* Unlock AFTER we check if we went below the low water mark */
          QUEUE_UNLOCK;
        } 
      } else {
        /* Queue for this receiver did NOT into high water mark node. Just
           unlock queue */
        QUEUE_UNLOCK;
      }
      
      /* Slow down if required 
       We want to do that BEFORE checking if we have to pulse ourselves
       because if we do NOT have to pulse ourselves we go back to the
       beginning of the loop to read the next bactc immediately*/
      for (i = 0; i < slow_factor; i++) {
        static uint32_t __attribute__ ((unused)) a;
        uint32_t b = 5000, c = 129;
        a = b * c;
      }

      /* If we have to pulse ourself, let's do that  */
      if (pulse_myself) {
        /* If the user requested that if there are still some packets in the
           queue  receiver continuue dequeue immediately instead of
           deferring the dequeue after each batch, let's do that */
        if (receiver_defer_after_each_batch) {
          PULSE_RECEIVER(1, is_transmitter); /* Just ourselves, so it is "1" */
        } else {
          goto begin_receive_loop;
        }
      } else {
        if (sender_exited) {
          /* 
           * sender has exited and we do not need to pulse ourself, which
           * means the queue is empty. 
           * Hence we will also exit
           */
          break;
        } else {
          PRINT_DEBUG("NO NEED to  pulse myself (receiver_wakeup fd %d).\n "
                      "\tSo far num_pulse_myself %u lost objs %u.\n"
                      "\tSo received %d objects %u packets %u batchs curr_batch size %u\n"
                      "\tqueue=(%s)\n",
                      receiver_wakeup_eventfd_fd,
                      num_pulse_myself,
                      num_lost_objs,
                      num_sent_received_objs,
                      num_packets,
                      num_batchs,
                      curr_batch_size,
                      print_queue(queue));
        }
      }
    } /* while((rc = epoll_wait(epoll_fd, &event, 1, -1)) > 0) { */
      
    /*
     * We got out of the epoll loop
     */
    if (rc < 0) {
      PRINT_ERR("epoll_wait failed for epoll_fd %d received_evenfd %d"
                "\tqueue=(%s): %d %s\n",
                epoll_fd,  receiver_wakeup_eventfd_fd,
                print_queue(queue), errno, strerror(errno));
      ret_value = EXIT_FAILURE;
      goto out;
    }
    /* get time stamp when finish receiving */
    clock_gettime(CLOCK_MONOTONIC, &end_ts);

    /* Inform the user */
    if (sender_exited) {
      PRINT_INFO("Sender EPOLLRDHUP on fd=%u "
                 "epoll_fd=%d  after epoll_wait %u times\n\t  queue=(%s)\n\n",
                 sender_exited_fd,
                 epoll_fd,
                 num_epoll_wait,
                 print_queue(queue));
    }
  }

  /*
   * convert timers to timval then use timersub to subtract them
   */
  TIMESPEC_TO_TIMEVAL(&start_tv, &start_ts);
  TIMESPEC_TO_TIMEVAL(&end_tv, &end_ts);
  timersub(&end_tv, &start_tv, &time_diff);


  /*
   * NOTE:
   * there is NO NEED for the sender to sleep
   * When a recevier gets EPOLLHUP on the AF_UNIX socket becayuse the sender
   * has transmitted all packets and exitted, the receiver will drain all
   * remaining packets in the queue and then exit
   * Hence all packets are delivered to all receivers correctly
   */

 out:
  print_stats();

  close_fds();

   
  exit(ret_value);
}


 

            
