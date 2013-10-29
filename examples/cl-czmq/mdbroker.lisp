;;  Majordomo Protocol broker
;;  A minimal lisp implementation of the Majordomo Protocol as defined in
;;  http://rfc.zeromq.org/spec:7 and http://rfc.zeromq.org/spec:8.

(ql:quickload '("cl-czmq" "cl-launch"))
(use-package :cl-czmq)

(load "mdp.lisp")

;;  We'd normally pull these from config data

(defconstant +heartbeat-liveness+ 3) ;;  3-5 is reasonable
(defconstant +heartbeat-interval+ 2500) ;;  msecs
(defconstant +heartbeat-expiry+
  (* +heartbeat-interval+ +heartbeat-liveness+))

;;  .split broker class structure
;;  The broker class defines a single broker instance:

(defclass broker ()
  ((ctx :initarg :ctx) ;;  Our context
   (socket :initarg :socket) ;;  Socket for clients & workers
   (verbose :initarg :verbose) ;;  Print activity to stdout
   (endpoint :initarg :endpoint) ;;  Broker binds to this endpoint
   (services :initarg :services) ;;  Hash of known services
   (workers :initarg :workers) ;;  Hash of known workers
   (waiting :initarg :waiting) ;;  List of waiting workers
   (heartbeat-at :initarg :heartbeat-at))) ;;  When to send HEARTBEAT

#||
(defgeneric s-broker-new (verbose))
(defgeneric s-broker-destroy (broker))
(defgeneric s-broker-bind (broker endpoint))
(defgeneric s-broker-worker-msg (broker sender msg))
(defgeneric s-broker-client-msg (broker sender msg)
(defgeneric s-broker-purge (broker))
||#

;;  .split service class structure
;;  The service class defines a single service instance:

(defclass service ()
  ((broker :initarg :broker) ;;  Broker instance
   (name :initarg :name) ;;  Service name
   (requests :initarg :requests) ;;  List of client requests
   (waiting :initarg :waiting) ;;  List of waiting workers
   (workers :initarg :workers))) ;;  How many workers we have

#||
static service_t *
    s_service_require (broker_t *self, zframe_t *service_frame);
static void
    s_service_destroy (void *argument);
static void
    s_service_dispatch (service_t *service, zmsg_t *msg);
||#

;;  .split worker class structure
;;  The worker class defines a single worker, idle or active:

(defclass worker ()
  ((broker :initarg :broker) ;;  Broker instance
   (id-string :initarg :id-string) ;;  Identity of worker as string
   (identity :initarg :identity) ;;  Identity frame for routing
   (service :initarg :service) ;;  Owning service, if known
   (expiry :initarg :expiry))) ;;  When worker expires, if no heartbeat

#||
static worker_t *
    s_worker_require (broker_t *self, zframe_t *identity);
static void
    s_worker_delete (worker_t *self, int disconnect);
static void
    s_worker_destroy (void *argument);
static void
    s_worker_send (worker_t *self, char *command, char *option,
                   zmsg_t *msg);
static void
    s_worker_waiting (worker_t *self);
||#

;;  .split broker constructor and destructor
;;  Here are the constructor and destructor for the broker:

(defun s-broker-new (verbose)
  (let ((ctx (zctx-new)))
    (make-instance
     'broker
     ;;  Initialize broker state
     :ctx ctx
     :socket (zsocket-new ctx :zmq-router)
     :verbose verbose
     :services (zhash-new)
     :workers (zhash-new)
     :waiting (zlist-new)
     :heartbeat-at (+ (zclock-time) +heartbeat-interval+))))

(defun s-broker-destroy (broker)
  (when broker
    (with-slots (ctx services workers waiting) broker
      (zctx-destroy ctx)
      (zhash-destroy services)
      (zhash-destroy workers)
      (zlist-destroy waiting)))
  nil)

;;  .split broker bind method
;;  This method binds the broker instance to an endpoint. We can call
;;  this multiple times. Note that MDP uses a single socket for both clients 
;;  and workers:

(defun s-broker-bind (broker endpoint)
  (with-slots (socket) broker
    (zsocket-bind socket endpoint))
  (zclock-log "I: MDP broker/0.2.0 is active at ~s" endpoint))

;;  .split broker worker_msg method
;;  This method processes one READY, REPLY, HEARTBEAT, or
;;  DISCONNECT message sent to the broker by a worker:

(defun s-broker-worker-msg (broker sender msg)
  (assert (plusp (zmsg-size msg))) ;;  At least, command

  (with-slots (socket workers) broker
    (let* ((command (zmsg-pop msg))
	   (id-string (zframe-strhex sender))
	   (worker-ready (when (zhash-lookup workers id-string) t))
	   (worker (s-worker-require broker sender)))
      (with-slots (service expiry) worker
	(cond ((zframe-streq command *mdpw-ready*)
	       (cond (worker-ready ;;  Not first command in session
		      (s-worker-delete worker t))
		     ((and (>= (zframe-size sender) 4) ;;  Reserved service name
			   (let ((sender-string (zframe-strdup sender)))
			     (and (>= (length sender-string) 4)
				  (string= sender-string "mmi." :end1 4))))
		      (s-worker-delete worker t))
		     (t
		      ;;  Attach worker to service and mark as idle
		      (let ((service-frame (zmsg-pop msg)))
			(setf service (s-service-require broker service-frame))
			(incf (slot-value service 'workers))
			(s-worker-waiting worker)
			(zframe-destroy service-frame)))))
	      ((zframe-streq command *mdpw-reply*)
	       (if worker-ready
		   ;;  Remove and save client return envelope and insert the
		   ;;  protocol header and service name, then rewrap envelope.
		   (let ((client (zmsg-unwrap msg)))
		     (zmsg-pushstr msg (slot-value service 'name))
		     (zmsg-pushstr msg *mdpc-client*)
		     (zmsg-wrap msg client)
		     (zmsg-send msg socket)
		     (s-worker-waiting worker))
		   (s-worker-delete worker t)))
	      ((zframe-streq command *mdpw-heartbeat*)
	       (if worker-ready
		   (setf expiry (+ (zclock-time) +heartbeat-expiry+))
		   (s-worker-delete worker t)))
	      ((zframe-streq command *mdpw-disconnect*)
	       (s-worker-delete worker nil))
	      (t
	       (zclock-log "E: invalid input message")
	       (zmsg-dump msg))))
      (let ((reply-p (zframe-streq command *mdpw-reply*)))
	(zframe-destroy command)
	(unless reply-p
	  (zmsg-destroy msg))))))

;;  .split broker client_msg method
;;  Process a request coming from a client. We implement MMI requests
;;  directly here (at present, we implement only the mmi.service request):

(defun s-broker-client-msg (broker sender msg)
  (assert (>=  (zmsg-size msg) 2)) ;;  Service name + body

  (let* ((service-frame (zmsg-pop msg))
	 (service (s-service-require broker service-frame)))

    ;;  Set reply return identity to client sender
    (zmsg-wrap msg (zframe-dup sender))

    ;;  If we got a MMI service request, process that internally
    (let ((service-name (zframe-strdup service-frame)))
      (cond ((and (>= (length service-name) 4)
		  (string= service-name "mmi." :end1 4))
	     (let ((return-code ""))
	       (if (string= service-name "mmi.service")
		   (let* ((name (zframe-strdup (zmsg-last msg)))
			  (service (zhash-lookup (slot-value broker 'services) name)))
		     (when service
		       (setf return-code 
			     (if (plusp (slot-value service 'workers))
				 "200" "404"))))
		   (setf return-code "501"))

	       (zframe-reset (zmsg-last msg) return-code))

	     ;;  Remove & save client return envelope and insert the
	     ;;  protocol header and service name, then rewrap envelope.
	     (let ((client (zmsg-unwrap msg)))
	       (zmsg-push msg (zframe-dup service-frame))
	       (zmsg-pushstr msg *mdpc-client*)
	       (zmsg-wrap msg client)
	       (zmsg-send msg (slot-value broker 'socket))))
	    (t
	     ;;  Else dispatch the message to the requested service
	     (s-service-dispatch service msg))))
    (zframe-destroy service-frame)))

;;  .split broker purge method
;;  This method deletes any idle workers that haven't pinged us in a
;;  while. We hold workers from oldest to most recent so we can stop
;;  scanning whenever we find a live worker. This means we'll mainly stop
;;  at the first worker, which is essential when we have large numbers of
;;  workers (we call this method in our critical path):

(defun s-broker-purge (broker)
  (with-slots (verbose waiting) broker
    (loop for worker = (zlist-first waiting)
       while worker do
	 (with-slots (expiry id-string) worker
	   (when (< (zclock-time) expiry)
	     (loop-finish)) ;;  Worker is alive, we're done here
	   (when verbose
	     (zclock-log "I: deleting expired worker: ~s" id-string))

	   (s-worker-delete worker nil)))))

;;  .split service methods
;;  Here is the implementation of the methods that work on a service:

;;  Lazy constructor that locates a service by name or creates a new
;;  service if there is no service already with that name.

(defun s-service-require (broker service-frame)
  (assert service-frame)
  (with-slots (services) broker
    (let* ((name (zframe-strdup service-frame))
	   (service (zhash-lookup services name)))
      (unless service
	(setf service
	      (make-instance 'service
			     :broker broker
			     :name name
			     :requests (zlist-new)
			     :waiting (zlist-new)
			     :workers 0))
	(zhash-insert services name service)
	(zhash-freefn services name #'s-service-destroy))
      service)))

;;  Service destructor is called automatically whenever the service is
;;  removed from broker->services.

(defun s-service-destroy (service)
  (with-slots (requests waiting) service
    (loop while (plusp (zlist-size requests)) do
	 (let ((msg (zlist-pop requests)))
	   (zmsg-destroy msg)))
    (zlist-destroy requests)
    (zlist-destroy waiting)))

;;  .split service dispatch method
;;  This method sends requests to waiting workers:

(defun s-service-dispatch (service msg)
  (assert service)

  (with-slots (requests broker waiting) service
    (when msg                    ;;  Queue message if any
      (zlist-append requests msg))

    (s-broker-purge broker)

    (loop while (and (plusp (zlist-size waiting))
		     (plusp (zlist-size requests))) do
	 (let ((worker (zlist-pop waiting))
	       (msg    (zlist-pop requests)))
	   (zlist-remove (slot-value broker 'waiting) worker)
	   (s-worker-send worker *mdpw-request* nil msg)
	   ;;(zmsg-destroy msg)
	   ))))

;;  .split worker methods
;;  Here is the implementation of the methods that work on a worker:

;;  Lazy constructor that locates a worker by identity, or creates a new
;;  worker if there is no worker already with that identity.

(defun s-worker-require (broker identity)
  (assert (and identity (not (cffi:null-pointer-p identity))))

  ;;  self->workers is keyed off worker identity
  (with-slots (workers verbose) broker
    (let* ((id-string (zframe-strhex identity))
	   (worker (zhash-lookup workers id-string)))
      (unless worker
	(setf worker
	      (make-instance 'worker
			     :service nil
			     :broker broker
			     :id-string id-string
			     :identity (zframe-dup identity)))
	(zhash-insert workers id-string worker)
	(zhash-freefn workers id-string #'s-worker-destroy)
        (when verbose
	  (zclock-log "I: registering new worker: ~s" id-string)))
      worker)))

;;  This method deletes the current worker.

(defun s-worker-delete (worker disconnect)
  (assert worker)
  (when disconnect
    (s-worker-send worker *mdpw-disconnect* nil nil))

  (with-slots (service broker id-string) worker
    (when service
      (with-slots (waiting workers) service
	(zlist-remove waiting worker)
        (decf workers)))
    (with-slots (waiting workers) broker
      (zlist-remove waiting worker)
      ;;  This implicitly calls s-worker-destroy
      (zhash-delete workers id-string))))

;;  Worker destructor is called automatically whenever the worker is
;;  removed from broker->workers.

(defun s-worker-destroy (worker)
  (zframe-destroy (slot-value worker 'identity)))

;;  .split worker send method
;;  This method formats and sends a command to a worker. The caller may
;;  also provide a command option, and a message payload:

(defun s-worker-send (worker command option msg)
  (unless msg
    (setf msg (zmsg-new)))

  ;;  Stack protocol envelope to start of message
  (when option
    (zmsg-pushstr msg option))
  (zmsg-pushstr msg command)
  (zmsg-pushstr msg *mdpw-worker*)

  ;;  Stack routing envelope to start of message
  (with-slots (identity broker) worker
    (zmsg-wrap msg (zframe-dup identity))

    (with-slots (verbose socket) broker
      (when verbose
	(zclock-log "I: sending ~A to worker" (mdps-command command))
	(zmsg-dump msg))
      (zmsg-send msg socket))))

;;  This worker is now waiting for work

(defun s-worker-waiting (worker)
  ;;  Queue to broker and service waiting lists
  
  (with-slots (broker service expiry) worker
    (assert broker)
    (zlist-append (slot-value broker 'waiting) worker)
    (zlist-append (slot-value service 'waiting) worker)
    (setf expiry (+ (zclock-time) +heartbeat-expiry+))
    (s-service-dispatch service nil)))

;;  .split main task
;;  Finally, here is the main task. We create a new broker instance and
;;  then process messages on the broker socket:
(defun main (&optional (args cl-launch:*arguments*))
  (let* ((verbose (and args (string= (car args) "-v")))
	 (broker (s-broker-new verbose)))
    (s-broker-bind broker "tcp://*:5555")

    ;;  Get and process messages forever or until interrupted
    (with-slots (socket verbose heartbeat-at waiting) broker
      (loop do
	   (with-zpollset (items (socket :zmq-pollin))
	     (unless (zpollset-poll items 1 +heartbeat-interval+)
	       (loop-finish)) ;;  Interrupted

	     ;;  Process next input message, if any
	     (when (zpollset-pollin items)
	       (let ((msg (zmsg-recv socket)))
		 (unless msg
		   (loop-finish)) ;;  Interrupted
		 (when verbose
		   (zclock-log "I: received message:")
		   (zmsg-dump msg))
		 (let ((sender (zmsg-pop msg))
		       (empty  (zmsg-pop msg))
		       (header (zmsg-pop msg)))

		   (cond ((zframe-streq header *mdpc-client*)
			  (s-broker-client-msg broker sender msg))
			 ((zframe-streq header *mdpw-worker*)
			  (s-broker-worker-msg broker sender msg))
			 (t
			  (zclock-log "E: invalid message:")
			  (zmsg-dump msg)
			  (zmsg-destroy msg)))
		   (zframe-destroy sender)
		   (zframe-destroy empty)
		   (zframe-destroy header))))
	     
	     ;;  Disconnect and delete any expired workers
	     ;;  Send heartbeats to idle workers if needed
	     (when (> (zclock-time) heartbeat-at)
	       (s-broker-purge broker)
	       (loop with worker = (zlist-first waiting)
		  while worker do
		    (s-worker-send worker *mdpw-heartbeat* nil nil)
		    (setf worker (zlist-next waiting)))
	       (setf heartbeat-at (+ (zclock-time) +heartbeat-interval+))))))

    (when (zctx-interrupted)
      (format t "W: interrupt received, shutting down...~%"))

    (s-broker-destroy broker)
    0))
