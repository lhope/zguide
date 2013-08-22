;;  Asynchronous client-to-server (DEALER to ROUTER)
;;
;;  While this example runs in a single process, that is to make
;;  it easier to start and stop the example. Each task has its own
;;  context and conceptually acts as a separate process.

(ql:quickload "cl-czmq")
(ql:quickload "cl-launch")
(use-package :cl-czmq)

;;  This is our client task
;;  It connects to the server, and then sends a request once per second
;;  It collects responses as they arrive, and it prints them out. We will
;;  run several client tasks in parallel, each with a different random ID.

(defun client-task (&rest args)
  (declare (ignore args))
  (with-zctx (ctx)
    (with-zsockets ctx
	((client :zmq-dealer))

      ;;  Set random identity to make tracing easier
      (let ((identity (format nil "~4,'0X-~4,'0X" (random #x10000) (random #x10000))))
	(zsocket-set-identity client identity)
	(zsocket-connect client "tcp://localhost:5570")

	(with-zpollset (items (client :zmq-pollin))
	  (loop with request-nbr = 0 do
	       ;;  Tick once per second, pulling in arriving messages
	       (dotimes (centitick 100)
		 (zpollset-poll items 1 (* 10 cl-czmq::ZMQ_POLL_MSEC))
		 (when (member :zmq-pollin (zpollset-events items))
		   (let ((msg (zmsg-recv client)))
		     (zframe-print (zmsg-last msg) identity)
		     (zmsg-destroy msg))))
	     (zstr-send client "request #~d" (incf request-nbr))))))))


;;  .split worker task
;;  Each worker task works on one request at a time and sends a random number
;;  of replies back, with random delays between replies:

(defun server-worker (ctx pipe &rest args)
  (declare (ignore args pipe))
  (with-zsockets ctx
      ((worker :zmq-dealer))
    (zsocket-connect worker "inproc://backend")
    (loop do
	 ;;  The DEALER socket gives us the reply envelope and message
	 (let ((msg (zmsg-recv worker)))
	   (assert msg () "No msg! ~A" (cl-czmq::%zmq-err))
	   (let ((identity (zmsg-pop msg))
		 (content (zmsg-pop msg)))
	     (assert content)
	     (zmsg-destroy msg)

	     ;;  Send 0..4 replies back
	     (loop repeat (random 5) do
		;;  Sleep for some fraction of a second
		  (sleep (* .001 (1+ (random 1000))))
		  (zframe-send identity worker :ZFRAME-REUSE :ZFRAME-MORE)
		  (zframe-send content worker :ZFRAME-REUSE))
	     (zframe-destroy identity)
	     (zframe-destroy content)
	     )))))

;;  .sb server task
;;  This is our server task.
;;  It uses the multithreaded server model to deal requests out to a pool
;;  of workers and route replies back to clients. One worker can handle
;;  one request at a time but one client can talk to multiple workers at
;;  once.

;;static void server_worker (void *args, zctx_t *ctx, void *pipe);

(defun server-task (&rest args)
  (declare (ignore args))
  (with-zctx (ctx)
    (with-zsockets ctx
	((frontend :zmq-router)
	 (backend :zmq-dealer))
      ;;  Frontend socket talks to clients over TCP
      (zsocket-bind frontend "tcp://*:5570")

      ;;  Backend socket talks to workers over inproc
      (zsocket-bind backend "inproc://backend")

      ;;  Launch pool of worker threads, precise number is not critical
      (loop repeat 5 do
	 (zthread-fork ctx #'server-worker))

      ;;  Connect backend to frontend via a proxy
      (zsocket-proxy frontend backend))))

;;  The main thread simply starts several clients and a server, and then
;;  waits for the server to finish.

(defun main ()
  ;; randomize
  (setf *random-state* (make-random-state t))
  (zthread-new #'client-task)
  (zthread-new #'client-task)
  (zthread-new #'client-task)
  (zthread-new #'server-task)
  (sleep 5) ;;  Run for 5 seconds then quit
  ;; need to explicitly quit as the threads don't die on their own.
  (cl-launch:quit 0))
