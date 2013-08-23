;;  Broker peering simulation (part 3)
;;  Prototypes the full flow of status and tasks

(ql:quickload '("cl-czmq" "cl-launch" "lparallel"))
(use-package :cl-czmq)
(use-package :lparallel.raw-queue)

(defconstant +nbr-clients+ 10)
(defconstant +nbr-workers+ 3)
(defparameter *worker-ready* #(1)) ;;  Signals worker is ready

;;  Our own name; in practice this would be configured per node
(defvar *self*)

;;  .split client task
;;  This is the client task. It issues a burst of requests and then
;;  sleeps for a few seconds. This simulates sporadic activity; when
;;  a number of clients are active at once, the local workers should
;;  be overloaded. The client uses a REQ socket for requests and also
;;  pushes statistics to the monitor socket:

(defun client-task (&rest args)
  (declare (ignore args))
  (with-zctx (ctx)
    (with-zsockets ctx
	((client :zmq-req)
	 (monitor :zmq-push))
      (zsocket-connect client "ipc://~a-localfe.ipc" *self*)
      (zsocket-connect monitor "ipc://~a-monitor.ipc" *self*)

      (loop do
	   (sleep (random 5))
	   (loop with burst = (random 15)
	      repeat burst do
		(let ((task-id (format nil "~4,'0x" (random #x10000))))
		  ;;  Send request with random hex ID
		  (zstr-send client task-id)

		  ;;  Wait max ten seconds for a reply, then complain
		  (with-zpollset (pollset (client :zmq-pollin))
		    (unless (zpollset-poll pollset 1 (* 10 1000 +zmq-poll-msec+))
		      ;; LH: I suspect the original break; was a bug never caught.
		      (zstr-send monitor "E: CLIENT EXIT - interrupted - ~A" task-id)
		      (return-from client-task)) ;;  Interrupted

		    (if (zpollset-pollin pollset 0)
			(let ((reply (zstr-recv client)))
			  (unless reply
			    (loop-finish)) ;;  Interrupted
			  ;;  Worker is supposed to answer us with our task id
			  (assert (string= reply task-id))
			  (zstr-send monitor "~A" reply))
			(progn
			  (zstr-send monitor "E: CLIENT EXIT - lost task ~s" task-id)
			  (return-from client-task))))))))))

;;  .split worker task
;;  This is the worker task, which uses a REQ socket to plug into the
;;  load-balancer. It's the same stub worker task that you've seen in
;;  other examples:

(defun worker-task (&rest args)
  (declare (ignore args))
  (with-zctx (ctx)
    (with-zsockets ctx
	((worker :zmq-req))
      (zsocket-connect worker "ipc://~A-localbe.ipc" *self*)

      ;;  Tell broker we're ready for work
      (let ((frame (zframe-new *WORKER-READY*)))
	(zframe-send frame worker))

      ;;  Process messages as they arrive
      (loop do
	   (let ((msg (zmsg-recv worker)))
	     (unless msg
	       (loop-finish)) ;;  Interrupted

	     ;;  Workers are busy for 0/1 seconds
	     (sleep (random 2))
	     (zmsg-send msg worker))))))

;;  .split main task
;;  The main task begins by setting up all its sockets. The local frontend
;;  talks to clients, and our local backend talks to workers. The cloud
;;  frontend talks to peer brokers as if they were clients, and the cloud
;;  backend talks to peer brokers as if they were workers. The state
;;  backend publishes regular state messages, and the state frontend
;;  subscribes to all state backends to collect these messages. Finally,
;;  we use a PULL monitor socket to collect printable messages from tasks:

(defun main (&optional (args cl-launch:*arguments*))
  ;;  First argument is this broker's name
  ;;  Other arguments are our peers' names
  (unless args
    (format t "syntax: peering3 me {you}...~%")
    (return-from main 0))

  (setf *self* (first args))
  (format t "I: preparing broker at ~s...~%" *self*)
  (setf *random-state* (make-random-state t))

  (with-zctx (ctx)
    (with-zsockets ctx
	((localfe :zmq-router)
	 (localbe :zmq-router)
	 (cloudfe :zmq-router)
	 (cloudbe :zmq-router)
	 (statebe :zmq-pub)
	 (statefe :zmq-sub)
	 (monitor :zmq-pull))

      ;;  Prepare local frontend and backend
      (zsocket-bind localfe "ipc://~a-localfe.ipc" *self*)
      (zsocket-bind localbe "ipc://~a-localbe.ipc" *self*)

      ;;  Bind cloud frontend to endpoint
      (zsocket-set-identity cloudfe *self*)
      (zsocket-bind cloudfe "ipc://~a-cloud.ipc" *self*)

      ;;  Connect cloud backend to all peers
      (zsocket-set-identity cloudbe *self*)
      (dolist (peer (rest args))
        (format t "I: connecting to cloud frontend at '~a'~%" peer)
        (zsocket-connect cloudbe "ipc://~a-cloud.ipc" peer))

      ;;  Bind state backend to endpoint
      (zsocket-bind statebe "ipc://~a-state.ipc" *self*)

      ;;  Connect state frontend to all peers
      (zsocket-set-subscribe statefe "")
      (dolist (peer (rest args))
        (format t "I: connecting to state backend at '~a'~%" peer)
        (zsocket-connect statefe "ipc://~a-state.ipc" peer))

      ;;  Prepare monitor socket
      (zsocket-bind monitor "ipc://~a-monitor.ipc" *self*)

      ;;  .split start child tasks
      ;;  After binding and connecting all our sockets, we start our child
      ;;  tasks - workers and clients:

      (loop repeat +nbr-workers+ do
	   (zthread-new #'worker-task))

      ;;  Start local clients
      (loop repeat +nbr-clients+ do
	 (zthread-new #'client-task))

      ;;  Queue of available workers
      (loop
	 with local-capacity = 0
	 with cloud-capacity = 0
	 with workers = (make-raw-queue)

	 ;;  .split main loop
	 ;;  The main loop has two parts. First, we poll workers and our two service
	 ;;  sockets (statefe and monitor), in any case. If we have no ready workers,
	 ;;  then there's no point in looking at incoming requests. These can remain
	 ;;  on their internal 0MQ queues:
	 do
	   (with-zpollset (primary
			   (localbe :zmq-pollin)
			   (cloudbe :zmq-pollin)
			   (statefe :zmq-pollin)
			   (monitor :zmq-pollin))
	     ;;  If we have no workers ready, wait indefinitely
	     (unless (zpollset-poll primary 4 (if (plusp local-capacity) (* 1000 +ZMQ-POLL-MSEC+) -1))
	       (loop-finish)) ;;  Interrupted

	     ;;  Track if capacity changes during this iteration
	     (let ((previous local-capacity)
		   msg) ;;  Reply from local worker

	       (cond ((zpollset-pollin primary 0)
		      (setf msg (zmsg-recv localbe))
		      (unless msg
			(loop-finish)) ;;  Interrupted
		      (let ((identity (zmsg-unwrap msg)))
			(push-raw-queue identity workers)
			(incf local-capacity))

		      ;;  If it's READY, don't route the message any further
		      (let ((frame (zmsg-first msg)))
			(when (equalp (zframe-data frame) *worker-ready*)
			  (setf msg (zmsg-destroy msg)))))
		     (;;  Or handle reply from peer broker
		      (zpollset-pollin primary 1)
		      (setf msg (zmsg-recv cloudbe))
		      (unless msg
			(loop-finish)) ;;  Interrupted
		      ;;  We don't use peer broker identity for anything
		      (let ((identity (zmsg-unwrap msg)))
			(zframe-destroy identity))))

	       ;;  Route reply to cloud if it's addressed to a broker
	       (dolist (arg (rest args))
		 (unless msg (return))
		 (let ((data (zframe-data (zmsg-first msg))))
		   (when (equalp (map 'vector #'char-code arg) data)
		     (setf msg (nth-value 1 (zmsg-send msg cloudfe))))))
	       ;;  Route reply to client if we still need to
	       (when msg
		 (zmsg-send msg localfe))

	       ;;  .split handle state messages
	       ;;  If we have input messages on our statefe or monitor sockets, we
	       ;;  can process these immediately:

	       (when (zpollset-pollin primary 2)
		 (zstr-recv statefe) ;; throw away peer
		 (let ((status (zstr-recv statefe)))
		   (setf cloud-capacity (parse-integer status))))
	       (when (zpollset-pollin primary 3)
		 (let ((status (zstr-recv monitor)))
		   (format t "~a~%" status)))

	       ;;  .split route client requests
	       ;;  Now route as many clients requests as we can handle. If we have
	       ;;  local capacity, we poll both localfe and cloudfe. If we have cloud
	       ;;  capacity only, we poll just localfe. We route any request locally
	       ;;  if we can, else we route to the cloud.

	       (loop while (plusp (+ local-capacity cloud-capacity)) do
		    (with-zpollset (secondary
				    (localfe :zmq-pollin)
				    (cloudfe :zmq-pollin))
		      (let ((rc (if (plusp local-capacity)
				    (zpollset-poll secondary 2 0)
				    (zpollset-poll secondary 1 0))))
			(assert rc))

		      (cond ((zpollset-pollin secondary 0)
			     (setf msg (zmsg-recv localfe)))
			    ((zpollset-pollin secondary 1)
			     (setf msg (zmsg-recv cloudfe)))
			    (t
			     (loop-finish))) ;;  No work, go back to primary

		      (if (plusp local-capacity)
			  (let ((frame (pop-raw-queue workers)))
			    (zmsg-wrap msg frame)
			    (zmsg-send msg localbe)
			    (decf local-capacity))
			  ;;  Route to random broker peer
			  (let ((peer (nth (1+ (random (1- (length args)))) args)))
			    (zmsg-pushmem msg peer)
			    (zmsg-send msg cloudbe)))))

	       ;;  .split broadcast capacity
	       ;;  We broadcast capacity messages to other peers; to reduce chatter,
	       ;;  we do this only if our capacity changed.

	       (unless (= local-capacity previous)
		 ;;  We stick our own identity onto the envelope
		 (zstr-sendm statebe *self*)
		 ;;  Broadcast new capacity
		 (zstr-send statebe "~d" local-capacity))))
	 finally
	 ;;  When we're done, clean up properly
	   (loop while (plusp (raw-queue-count workers)) do
		(let ((frame (pop-raw-queue workers)))
		  (zframe-destroy frame))))))
  0)
