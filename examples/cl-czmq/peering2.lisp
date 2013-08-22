;  Broker peering simulation (part 2)
;;  Prototypes the request-reply flow

(ql:quickload '("cl-czmq" "cl-launch" "lparallel"))
(use-package :cl-czmq)
(use-package :lparallel.raw-queue)

(defconstant +nbr-clients+ 10)
(defconstant +nbr-workers+ 3)
(defparameter *worker-ready* #(1)) ;;  Signals worker is ready

;;  Our own name; in practice this would be configured per node
(defvar *self*)

;;  .split client task
;;  The client task does a request-reply dialog using a standard
;;  synchronous REQ socket:

(defun client-task (&rest args)
  (declare (ignore args))
  (with-zctx (ctx)
    (with-zsockets ctx
	((client :zmq-req))
      (zsocket-connect client "ipc://~A-localfe.ipc" *self*)

      (loop do
	 ;;  Send request, get reply
	   (zstr-send client "HELLO")

	 (let ((reply (zstr-recv-retry client)))
	   (unless reply
	     (loop-finish)) ;;  Interrupted
	   (format t "Client: ~A~%" reply))
	 (sleep 1)))))

;;  .split worker task
;;  The worker task plugs into the load-balancer using a REQ
;;  socket:

(defun worker-task (&rest args)
  (declare (ignore args))
  (with-zctx (ctx)
    (with-zsockets ctx
	((worker :zmq-req))
      (zsocket-connect worker "ipc://~A-localbe.ipc" *self*)

      ;;  Tell broker we're ready for work
      (let ((frame (zframe-new *worker-ready*)))
	(zframe-send frame worker))

      ;;  Process messages as they arrive
      (loop do
	   (let ((msg (zmsg-recv-retry worker)))
	     (unless msg
	       (loop-finish)) ;;  Interrupted

	     (zframe-print (zmsg-last msg) "Worker: ")
	     (zframe-reset (zmsg-last msg) "OK")
	     (zmsg-send msg worker))))))

;;  .split main task
;;  The main task begins by setting-up its frontend and backend sockets
;;  and then starting its client and worker tasks:

(defun main (&optional (args cl-launch:*arguments*))
  ;;  First argument is this broker's name
  ;;  Other arguments are our peers' names
  ;;
  (when (< (length args) 1)
    (format t "syntax: peering2 me {you}...~%")
    (return-from main 0))

  (setf *self* (car args))
  (format t "I: preparing broker at ~s...~%" *self*)
  (setf *random-state* (make-random-state t))

  (with-zctx (ctx)
    (with-zsockets ctx
	((cloudfe :zmq-router)
	 (cloudbe :zmq-router)
	 (localfe :zmq-router)
	 (localbe :zmq-router))

      ;;  Bind cloud frontend to endpoint
      (zsocket-set-identity cloudfe *self*)
      (zsocket-bind cloudfe "ipc://~A-cloud.ipc" *self*)

      ;;  Connect cloud backend to all peers
      (zsocket-set-identity cloudbe *self*)
      (dolist (peer (rest args))
        (format t "I: connecting to cloud frontend at '~A'~%" peer)
        (zsocket-connect cloudbe "ipc://~A-cloud.ipc" peer))

      ;;  Prepare local frontend and backend
      (zsocket-bind localfe "ipc://~A-localfe.ipc" *self*)
      (zsocket-bind localbe "ipc://~A-localbe.ipc" *self*)

      ;;  Get user to tell us when we can start...
      (format t "Press Enter when all brokers are started: ") (finish-output)
      (read-char)

      ;;  Start local workers
      (loop repeat +nbr-workers+ do
	   (zthread-new #'worker-task))

      ;;  Start local clients
      (loop repeat +nbr-clients+ do
	   (zthread-new #'client-task))

      ;;  .split request-reply handling
      ;;  Here, we handle the request-reply flow. We're using load-balancing
      ;;  to poll workers at all times, and clients only when there are one
      ;;  or more workers available.

      ;;  Least recently used queue of available workers
      (loop with capacity = 0
	 with workers = (make-raw-queue) do
	   ;;  First, route any waiting replies from workers
	   (with-zpollset (backends
			   (localbe :zmq-pollin)
			   (cloudbe :zmq-pollin))
	     ;;  If we have no workers, wait indefinitely
	     (unless (zpollset-poll backends 2
				    (if (zerop capacity) -1 (* 1000 +zmq-poll-msec+)))
	       (loop-finish)) ;;  Interrupted

	     (let (msg)
	       (cond (;;  Handle reply from local worker
		      (zpollset-pollin backends 0)
		      (setf msg (zmsg-recv localbe))
		      (unless msg
			(loop-finish)) ;;  Interrupted
		      (let ((identity (zmsg-unwrap msg)))
			(push-raw-queue identity workers)
			(incf capacity))

		      ;;  If it's READY, don't route the message any further
		      (let ((frame (zmsg-first msg)))
			(when (equalp (zframe-data frame) *worker-ready*)
			  (setf msg (zmsg-destroy msg)))))
		     (;;  Or handle reply from peer broker
		      (zpollset-pollin backends 1)
		      (setf msg (zmsg-recv cloudbe))
		      (unless msg
			(loop-finish)) ;;  Interrupted
		      ;;  We don't use peer broker identity for anything
		      (let ((identity (zmsg-unwrap msg)))
			(zframe-destroy identity))))

	       ;;  Route reply to cloud if it's addressed to a broker
	       (when msg
		 (dolist (arg (rest args))
		   (let ((data (zframe-data (zmsg-first msg))))
		     (when (equalp (map 'vector #'char-code arg) data)
		       (setf msg (nth-value 1 (zmsg-send msg cloudfe)))
		       ;; we are done here, so we quit the loop.
		       (return)))))

	       ;;  Route reply to client if we still need to
	       (when msg
		 (zmsg-send msg localfe)))

	     ;;  .split route client requests
	     ;;  Now we route as many client requests as we have worker capacity
	     ;;  for. We may reroute requests from our local frontend, but not from
	     ;;  the cloud frontend. We reroute randomly now, just to test things
	     ;;  out. In the next version, we'll do this properly by calculating
	     ;;  cloud capacity:

	     (loop
		with msg
		with reroutable
		while (plusp capacity) do
		  (with-zpollset (frontends
				  (localfe :zmq-pollin)
				  (cloudfe :zmq-pollin))
		    (let ((rc (zpollset-poll frontends 2 0)))
		      (assert rc))
		    ;;  We'll do peer brokers first, to prevent starvation
		    (cond ((zpollset-pollin frontends 1)
			   (setf msg (zmsg-recv cloudfe)
				 reroutable nil))
			  ((zpollset-pollin frontends 0)
			   (setf msg (zmsg-recv localfe)
				 reroutable t))
			  (t
			   (loop-finish))) ;;  No work, go back to backends

		    ;;  If reroutable, send to cloud 20% of the time
		    ;;  Here we'd normally use cloud status information
		    ;;
		    (if (and reroutable (rest args)
			     (zerop (random 5)))
			;;  Route to random broker peer
			(let ((peer (nth (1+ (random (1- (length args)))) args)))
			  (zmsg-pushmem msg peer)
			  (zmsg-send msg cloudbe)
			  )
			;; else
			(let ((frame (pop-raw-queue workers)))
			  (zmsg-wrap msg frame)
			  (zmsg-send msg localbe)
			  (decf capacity))))))
	 finally
	   ;;  When we're done, clean up properly
	 (loop while (plusp (raw-queue-count workers)) do
	      (let ((frame (pop-raw-queue workers)))
		(zframe-destroy frame))))))
  0)
