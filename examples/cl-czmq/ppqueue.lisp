;;  Paranoid Pirate queue


(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defconstant +heartbeat-liveness+ 3) ;;  3-5 is reasonable
(defconstant +heartbeat-interval+ 1000) ;;  msecs

;;  Paranoid Pirate Protocol constants
(defparameter *ppp-ready* #(1))    ;;  Signals worker is ready
(defparameter *ppp-heartbeat* #(2)) ;;  Signals worker heartbeat

;;  .split worker class structure
;;  Here we define the worker class; a structure and a set of functions that
;;  act as constructor, destructor, and methods on worker objects:

(defstruct worker
  identity  ;;  Identity of worker
  id-string ;;  Printable identity
  expiry)   ;;  Expires at this time

;;  Construct new worker
(defun s-worker-new (identity)
  (make-worker
   :identity identity
   :id-string (zframe-strhex identity)
   :expiry (+ (get-internal-real-time)
	      (* +heartbeat-interval+ +heartbeat-liveness+))))

;;  Destroy specified worker object, including identity frame.
(defun s-worker-destroy (worker)
  (when (worker-identity worker)
    (zframe-destroy (worker-identity worker)))
  nil)

;;  .split worker ready method
;;  The ready method puts a worker to the end of the ready list.

(defun s-worker-ready (worker workers)
  (loop for list-worker = (zlist-first workers) then (zlist-next workers)
     while list-worker do
       (when (string= (worker-id-string worker) (worker-id-string list-worker))
	 (zlist-remove workers list-worker)
	 (s-worker-destroy list-worker)))
  (zlist-append workers worker))

;;  .split get next available worker
;;  The next method returns the next available worker identity:

(defun s-workers-next (workers)
  (let ((worker (zlist-pop workers)))
    (assert worker)
    (let ((frame (worker-identity worker)))
      (setf (worker-identity worker) nil)
      (s-worker-destroy worker)
      frame)))

;;  .split purge expired workers
;;  The purge method looks for and kills expired workers. We hold workers
;;  from oldest to most recent, so we stop at the first alive worker:

(defun s-workers-purge (workers)
  (loop for worker = (zlist-first workers)
     while worker do
       (when (< (get-internal-real-time) (worker-expiry worker))
	 (loop-finish)) ;;  Worker is alive, we're done here
       (zlist-remove workers worker)
       (s-worker-destroy worker)))

;;  .split main task
;;  The main task is a load-balancer with heartbeating on workers so we
;;  can detect crashed or blocked worker tasks:

(defun main ()
  (with-zctx (ctx)
    (with-zsockets ctx
	((frontend :zmq-router)
	 (backend :zmq-router))
      (zsocket-bind frontend "tcp://*:5555") ;;  For clients
      (zsocket-bind backend  "tcp://*:5556") ;;  For workers

      (loop
	 ;;  List of available workers
	 with workers = (zlist-new)
	 ;;  Send out heartbeats at regular intervals
	 with heartbeat-at = (+ (get-internal-real-time) +heartbeat-interval+)
	 do
	   (with-zpollset (items
			   (backend :zmq-pollin)
			   (frontend :zmq-pollin))
	     ;;  Poll frontend only if we have available workers
	     (unless (zpollset-poll items (if (plusp (zlist-size workers)) 2 1)
				    (* +heartbeat-interval+ +zmq-poll-msec+))
	       (loop-finish)) ;;  Interrupted

	     ;;  Handle worker activity on backend
	     (when (zpollset-pollin items 0)
	       ;;  Use worker identity for load-balancing
	       (let ((msg (zmsg-recv backend)))
		 (unless msg
		   (loop-finish)) ;;  Interrupted

		 ;;  Any sign of life from worker means it's ready
		 (let* ((identity (zmsg-unwrap msg))
			(worker (s-worker-new identity)))
		   (s-worker-ready worker workers))

		 ;;  Validate control message, or return reply to client
		 (if (= (zmsg-size msg) 1)
		     (let ((frame (zmsg-first msg)))
		       (when (and (not (equalp (zframe-data frame) *ppp-ready*))
				  (not (equalp (zframe-data frame) *ppp-heartbeat*)))
			 (format t "E: invalid message from worker")
			   (zmsg-dump msg))
		       (zmsg-destroy msg))
		     (zmsg-send msg frontend))))
	     (when (zpollset-pollin items 1)
	       ;;  Now get next client request, route to next worker
	       (let ((msg (zmsg-recv frontend)))
		 (unless msg
		   (loop-finish)) ;;  Interrupted
		 (zmsg-push msg (s-workers-next workers))
		 (zmsg-send msg backend)))
	     ;;  .split handle heartbeating
	     ;;  We handle heartbeating after any socket activity. First, we send
	     ;;  heartbeats to any idle workers if it's time. Then, we purge any
	     ;;  dead workers:
	     (when (>= (get-internal-real-time) heartbeat-at)
	       (loop for worker = (zlist-first workers) then (zlist-next workers)
		  while worker do
		  (zframe-send (worker-identity worker) backend
			       :zframe-reuse :zframe-more)
		  (let ((frame (zframe-new *ppp-heartbeat*)))
		    (zframe-send frame backend)))
	       (setf heartbeat-at (+ (get-internal-real-time) +heartbeat-interval+)))
	     (s-workers-purge workers))
	 finally
	 ;;  When we're done, clean up properly
	   (loop while (plusp (zlist-size workers)) do
		(let ((worker (zlist-pop workers)))
		  (s-worker-destroy worker))))))
  0)
