;;  Load-balancing broker
;;  Clients and workers are shown here in-process

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defconstant +nbr-clients+ 10)
(defconstant +nbr-workers+ 3)

;;  Basic request-reply client using REQ socket
;;  Because s_send and s_recv can't handle 0MQ binary identities, we
;;  set a printable text identity to allow routing.

(defun s-set-id (socket)
  (let ((identity (format nil "~4,'0x-~4,'0x" (random #x10000) (random #x10000))))
    (zsocket-set-identity socket identity)))

;;
(defun client-task (&rest args)
  (declare (ignore args))
  (with-zctx (context)
    (with-zsockets context
      ((client :zmq-req))
      (s-set-id client) ;;  Set a printable identity
      (zsocket-connect client "ipc://frontend.ipc")

      ;;  Send request, get reply
      (zstr-send client "HELLO")
      (let ((reply (zstr-recv client)))
	(format t "Client: ~s~%" reply)))))

;;  .split worker task
;;  While this example runs in a single process, that is just to make
;;  it easier to start and stop the example. Each thread has its own
;;  context and conceptually acts as a separate process.
;;  This is the worker task, using a REQ socket to do load-balancing.
;;  Because s_send and s_recv can't handle 0MQ binary identities, we
;;  set a printable text identity to allow routing.

(defun worker-task (&rest args)
  (declare (ignore args))
  (with-zctx (context)
    (with-zsockets context
	((worker :zmq-req))
      (s-set-id worker) ;;  Set a printable identity
      (zsocket-connect worker "ipc://backend.ipc")

      ;;  Tell broker we're ready for work
      (zstr-send worker "READY")

      (loop do
	   ;;  Read and save all frames until we get an empty frame
	   ;;  In this example there is only 1, but there could be more
	   (let ((identity (zstr-recv worker)))
	     (assert (zerop (length (zstr-recv worker))))

	     ;;  Get request, send reply
	     (let ((request (zstr-recv worker)))
	       (format t "Worker: ~s~%" request))

	     (zstr-sendm worker identity)
	     (zstr-sendm worker "")
	     (zstr-send worker "OK"))))))

;;  .split main task
;;  This is the main task. It starts the clients and workers, and then
;;  routes requests between the two layers. Workers signal READY when
;;  they start; after that we treat them as ready when they reply with
;;  a response back to a client. The load-balancing data structure is
;;  just a queue of next available workers.

(defun main ()
  ;;  Prepare our context and sockets
  (with-zctx (context)
    (with-zsockets context
	((frontend :zmq-router)
	 (backend :zmq-router))
      (zsocket-bind frontend "ipc://frontend.ipc")
      (zsocket-bind backend "ipc://backend.ipc")

      (loop repeat +nbr-clients+ do
	   (zthread-new #'client-task))

      (loop repeat +nbr-workers+ do
	   (zthread-new #'worker-task))

      ;;  .split main task body
      ;;  Here is the main loop for the least-recently-used queue. It has two
      ;;  sockets; a frontend for clients and a backend for workers. It polls
      ;;  the backend in all cases, and polls the frontend only when there are
      ;;  one or more workers ready. This is a neat way to use 0MQ's own queues
      ;;  to hold messages we're not ready to process yet. When we get a client
      ;;  reply, we pop the next available worker and send the request to it,
      ;;  including the originating client identity. When a worker replies, we
      ;;  requeue that worker and forward the reply to the original client
      ;;  using the reply envelope.

      (loop
	 with client-nbr = +nbr-clients+
	 with available-workers = 0
	 with worker-queue = (zlist-new)
	 do
	   (with-zpollset (items
			   (backend :zmq-pollin)
			   (frontend :zmq-pollin))
	     ;;  Poll frontend only if we have available workers
	     (unless (zpollset-poll items (if (plusp available-workers) 2 1) -1)
	       (loop-finish)) ;;  Interrupted

	     ;;  Handle worker activity on backend
	     (when (member :zmq-pollin (zpollset-events items 0))
	       ;;  Queue worker identity for load-balancing
	       (let ((worker-id (zstr-recv backend)))
		 (assert (< available-workers +nbr-workers+))
		 (zlist-append worker-queue worker-id)
		 (incf available-workers))

	       ;;  Second frame is empty
	       (assert (zerop (length (zstr-recv backend))) ())

	       ;;  Third frame is READY or else a client reply identity
	       (let ((client-id (zstr-recv backend)))

		 ;;  If client reply, send rest back to frontend
		 (unless (string= client-id "READY")
		   (assert (zerop (length (zstr-recv backend))))
		   (let ((reply (zstr-recv backend)))
		     (zstr-sendm frontend client-id)
		     (zstr-sendm frontend "")
		     (zstr-send frontend reply))
		   (when (zerop (decf client-nbr))
		     (loop-finish))))) ;;  Exit after N messages

	     ;;  .split handling a client request
	     ;;  Here is how we handle a client request:

	     (when (member :zmq-pollin (zpollset-events items 1))
	       ;;  Now get next client request, route to last-used worker
	       ;;  Client request is [identity][empty][request]
	       (let ((client-id (zstr-recv frontend)))
		 (assert (zerop (length (zstr-recv frontend))))
		 (let ((request (zstr-recv frontend)))
		   (zstr-sendm backend (zlist-first worker-queue))
		   (zstr-sendm backend "")
		   (zstr-sendm backend client-id)
		   (zstr-sendm backend "")
		   (zstr-send backend request)))
	       ;; Dequeue and drop the next worker identity
	       (zlist-pop worker-queue)
	       (decf available-workers))))))
  0)
