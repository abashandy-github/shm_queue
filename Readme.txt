Introduction
============

This repository contains experimental proof-of-concept- code that implements
few point-to-point and point-to-multipiont IPC mechanisms where there is one sender process and
one or more receiving processes

Overview
=========
Point to multi-point IPC
- shm_queue.c:
  scalable, fast and relatively complex shared memory queue implemenation that
  supports both point to point and point to multipoint
- tcp_queue.c: Brute force use of TCP where the sender loops and calls
  "write()" to send messages to every receiver while each receiver just waits
  on read() to read incoming messages
    

Point to point IPC
- shm_simple_queue.c, shm_common.c, shm_common.h:
  A simple lockless point to point implementation of shared memory queue where
  the sending by the sender and receiving by the receiver is serialized. 
- tcp_queue_with_epoll.c
  This is similar to tcp_queue.c but has two differences
  - It is point to point
  - The sender behavior is the same as tcp_queue.c. However after receiving a
    message, the receiver calls epoll_wait() to wait on being unblocked when
    data arrives at the TCP socket
  The objective of this experimental code is to have some realism to using
  TCP for IPC by emulating a receiving thread that services multiple file
  descriptors, one of them is the socket used for reciving


How things work
===============
See the comments at the top of each file for explanation of how each
implementation works
Also take a look at documents under doc/
