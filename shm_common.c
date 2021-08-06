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
 *
 * What is this
 * ===========
 * common functions to be used by various files
 */


#include "shm_common.h"



/*
 * bind an open unix socket to sockpath
 */
static int  __attribute__ ((unused))
bind_to_sock_path(char *sock_path, int sock_fd)
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
    PRINT_ERR("Cannot bind socket %d to sockpath %s length %d: %d %s\n",
              sock_fd, sockaddr.sun_path, len,
              errno, strerror(errno));
    return (-1);
  } else {
    PRINT_DEBUG("Succesfully bound socker %d to sockpath '%s' length %d\n",
              sock_fd, sockaddr.sun_path, len);
  }
  return (0);
}

/*
 * Prepares msg and cmsg for the "num_fs" file descriptors to be either sent or
 * received
 * It does NOT copy into or extract from the msg or cmsg the file descriptors
 */

void
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



/*
 * Sender listens on socket
 * recveiver connects
 * Sender sends two eventfds
 * Receiver receives two eventfds
 */
int
send_receive_eventfd(bool is_transmitter,
                     bool is_use_eventfd,
                     int *sender_wakeup_fd_p,
                     int *receiver_wakeup_fd_p,
                     int *epoll_fd_p,
                     int *sock_fd_p,
                     int *server_accept_fd,                     
                     char *sock_path,
                     uint32_t num_receivers)
{
  int rc = 0;
  int sender_wakeup_eventfd_fd = -1;
  int receiver_wakeup_eventfd_fd = -1;
  int epoll_fd = -1;
  int sock_fd = -1;
  int scm_fds[2];
  struct epoll_event event;
  int eventfd_flags = EFD_NONBLOCK | EFD_SEMAPHORE;
  socklen_t remote_len;
  struct sockaddr_un sockaddr_remote;
  uint32_t i;

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

  if (is_transmitter) {
    /*
     * Transmitter mode
     * This is the step where the tranismitter receivers a connection from a
     * receiver and send the eventfd that the transmitter uses to tell the
     * receiver that the transmitter has queued a packet
     */


    /* Bind it to the sockpath */
    if ((rc = bind_to_sock_path(sock_path, sock_fd)) < 0) {
      goto out;
    }

    /*
     * Put socket in listen mode
     */
    if (listen(sock_fd, MAX_NUM_RECEIVERS) == -1) {
      PRINT_ERR("listen() sock_fd %d backlong=%u "
                "failed : %d %s\n",
                sock_fd, MAX_NUM_RECEIVERS, errno, strerror(errno));
      rc = -1;
      goto out;
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
      rc = -1;
      goto out;
    }
    receiver_wakeup_eventfd_fd = eventfd(0,eventfd_flags);
    if (receiver_wakeup_eventfd_fd == -1) {
      PRINT_ERR("receiver_wakeup_eventfd_fd failed with flags 0x%x: %d %s\n",
                eventfd_flags,
                errno, strerror(errno));
      rc = -1;
      goto out;
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
        rc = -1;
        goto out;
      }

      PRINT_DEBUG("Connected to receiver %d accept_fd %d , len %d path '%s'.\n",
                  i, server_accept_fd[i],remote_len, sockaddr_remote.sun_path);
      /*
       * Send the SCM_RIGHTS message containing the eventfd
       */
      if (send_fds(server_accept_fd[i],
                   scm_fds, 2) < 1) {
        PRINT_ERR("send_fds() to receiver %d for eventfd %d %d failed : %d %s\n", i,
                  scm_fds[0], scm_fds[1],
                  errno, strerror(errno));
        rc = -1;
        goto out;
      }
    }
    PRINT_INFO("Connected to %u receivers..\n\n",num_receivers);
    
    /*
     * prepare the epoll for sender so that we can wait on it if we find that
     * the queue is full
     */
    if (is_use_eventfd) {
      event.events = EPOLLIN;
      event.data.fd = sender_wakeup_eventfd_fd;
      int j = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sender_wakeup_eventfd_fd,
                        &event);
      if (j < 0) {
        PRINT_ERR("epoll_ctl failed for epoll_fd %d sender_wakeup_evenfd %d: "
                  "%d %s\n",
                  epoll_fd, sender_wakeup_eventfd_fd, errno, strerror(errno));
        rc = -1;
        goto out;
      } else {
        PRINT_DEBUG("Successfully called epoll_ctl() epoll_fd=%d "
                    "sender_wakeup_evenfd %d \n",
                    epoll_fd, sender_wakeup_eventfd_fd);
      }
    } else {
      for (i = 0; i < num_receivers; i++) {
        /* Remember we have to make the accept_fd[i] non blocing before using
           it with epoll() */
        if ((rc = fcntl(server_accept_fd[i], F_SETFL, O_NONBLOCK)) < 0) {
          PRINT_ERR("FAILED to make server_accept_ds[%u] %d non-blocking: "
                    "%d %s\n",
                    server_accept_fd[i], i , errno, strerror(errno));
          goto out;
        }
          
        memset(&event, 0, sizeof(event));        
        event.events = EPOLLIN;
        event.data.fd = server_accept_fd[i];
        int j = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_accept_fd[i],
                          &event);
        if (j < 0) {
          PRINT_ERR("epoll_ctl failed for epoll_fd %d "
                    "server_accept_fd[%u] %d: %d %s\n",
                    epoll_fd, server_accept_fd[i], i,
                    errno, strerror(errno));
          rc = -1;
          goto out;
        } else {
          PRINT_DEBUG("Successfully called epoll_ctl() "
                      "epoll_fd=%d server_accept_fd[%u] evenfd %d \n",
                      epoll_fd, server_accept_fd[i], i);
        }
      }
    }
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
      rc = -1;
      goto out;
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
        PRINT_ERR("FAILED rec_fds() to receiver from sock_fd %d: %d %s\n",
                  sock_fd,
                  errno, strerror(errno));
      rc = -1;
      goto out;
    } else {
      sender_wakeup_eventfd_fd = scm_fds[0];
      receiver_wakeup_eventfd_fd = scm_fds[1];
      PRINT_DEBUG("Received sender eventfd %d and receiver fd %d\n",
                  sender_wakeup_eventfd_fd,
                  receiver_wakeup_eventfd_fd);
    }

    /*
     * Make the socket non-blocing so that we can use it with epoll
     */
    if ((rc = fcntl(sock_fd, F_SETFL, O_NONBLOCK)) < 0) {
      PRINT_ERR("FAILED to make sock_fd %d non-blocking: %d %s\n",
                sock_fd, errno, strerror(errno));
      goto out;
    }

    /*
     * prepare the epoll so that if the queue is empty the receiver can wait on
     * it for a signal from the senderr 
     */
    if (is_use_eventfd) {
      event.events = EPOLLIN;
      event.data.fd = receiver_wakeup_eventfd_fd;
      rc = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, receiver_wakeup_eventfd_fd,
                     &event);
      if (rc < 0) {
        PRINT_ERR("epoll_ctl failed for epoll_fd %d "
                  "receiver_wakeup_evenfd %d: %d %s\n",
                  epoll_fd, receiver_wakeup_eventfd_fd, errno, strerror(errno));
        goto out;
      } else {
        PRINT_DEBUG("Successfully called epoll_ctl() epoll_fd=%d "
                    "receiver_wakeup_evenfd %d \n",
                    epoll_fd, receiver_wakeup_eventfd_fd);
      }
    }
    
    /*
     * We will always monitor the AF_UNIX socket even when we use eventfd for
     * singalling because we use it to detect sender closing the socket and
     * exiting
     */
    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN | EPOLLRDHUP;
    event.data.fd = sock_fd;
    rc = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock_fd, &event);
    if (rc < 0) {
      PRINT_ERR("epoll_ctl failed for epoll_fd %d sock_fd %d: %d %s\n",
                epoll_fd, sock_fd, errno, strerror(errno));
      goto out;
    } else {
      PRINT_DEBUG("Successfully called epoll_ctl() epoll_fd=%d soc_fd %d \n",
                  epoll_fd, sock_fd);
    }
  } /* else of "if (is_transmitter)" */

 out:
  if (rc < 0) {
    *sender_wakeup_fd_p = -1;
    *receiver_wakeup_fd_p = -1;
    *epoll_fd_p = -1;
    *sock_fd_p = -1;
  } else {
    *sender_wakeup_fd_p = sender_wakeup_eventfd_fd;
    *receiver_wakeup_fd_p = receiver_wakeup_eventfd_fd;
    *epoll_fd_p = epoll_fd;
    *sock_fd_p = sock_fd;
  }
  return (rc);
}
