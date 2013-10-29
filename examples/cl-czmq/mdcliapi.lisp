;;  mdcliapi class - Majordomo Protocol Client API
;;  Implements the MDP/Worker spec at http://rfc.zeromq.org/spec:7.

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(load "mdp.lisp")

;;  Structure of our class
;;  We access these properties only via class methods

(defclass mdcli ()
  ((ctx :initarg :ctx) ;;  Our context
   (broker :initarg :broker) ;;  connection string
   (client :initarg :client) ;;  Socket to broker
   (verbose :initarg :verbose) ;;  Print activity to stdout
   (timeout :initarg :timeout) ;;  Request timeout
   (retries :initarg :retries))) ;;  Request retries

;;  Connect or reconnect to broker

(defun s-mdcli-connect-to-broker (mdcli)
  (with-slots (client ctx broker verbose) mdcli
    (when client
      (zsocket-destroy ctx client))
    (setf client (zsocket-new ctx :zmq-req))
    (zsocket-connect client "~A" broker)
    (when verbose
      (zclock-log "I: connecting to broker at ~s..." broker))))

;;  .split constructor and destructor
;;  Here we have the constructor and destructor for our class:

;;  Constructor

(defun mdcli-new (broker verbose)
  (assert broker)

  (let ((mdcli (make-instance
		'mdcli
		:ctx (zctx-new)
		:broker (copy-seq broker)
		:client nil
		:verbose verbose
		:timeout 2500 ;;  msecs
		:retries  3))) ;;  Before we abandon

    (s-mdcli-connect-to-broker mdcli)
    mdcli))

;;  Destructor
(defun mdcli-destroy (mdcli)
  (when mdcli
    (zctx-destroy (slot-value mdcli 'ctx))
    nil))

;;  .split configure retry behavior
;;  These are the class methods. We can set the request timeout and number
;;  of retry attempts before sending requests:

;;  Set request timeout

(defun mdcli-set-timeout (mdcli timeout)
  (assert mdcli)
  (setf (slot-value mdcli 'timeout) timeout))

;;  Set request retries

(defun mdcli-set-retries (mdcli retries)
  (assert mdcli)
  (setf (slot-value mdcli 'retries) retries))

;;  .split send request and wait for reply
;;  Here is the {{send}} method. It sends a request to the broker and gets
;;  a reply even if it has to retry several times. It takes ownership of
;;  the request message, and destroys it when sent. It returns the reply
;;  message, or nil if there was no reply after multiple attempts:

(defun mdcli-send (mdcli service request)
  (assert mdcli)

  (with-slots (timeout client retries verbose) mdcli
  ;;  Prefix request with protocol frames
  ;;  Frame 1: "MDPCxy" (six bytes, MDP/Client x.y)
  ;;  Frame 2: Service name (printable string)
  (zmsg-pushstr request service)
  (zmsg-pushstr request *mdpc-client*)

  (when verbose
    (zclock-log "I: send request to '~a' service:" service);
    (zmsg-dump request))

  (loop with retries-left = retries
     while (and (plusp retries-left) (not (zctx-interrupted)))
     do
       (let ((msg (zmsg-dup request)))
	 (zmsg-send msg client))

       (with-zpollset (items (client :zmq-pollin))
	 ;;  .split body of send
	 ;;  On any blocking call, {{libzmq}} will return -1 if there was
	 ;;  an error; we could in theory check for different error codes,
	 ;;  but in practice it's OK to assume it was {{EINTR}} (Ctrl-C):

	 (unless (zpollset-poll items 1 timeout)
	   (loop-finish)) ;;  Interrupted

	 ;;  If we got a reply, process it
	 (cond ((zpollset-pollin items 0)
		(let ((msg (zmsg-recv client)))
		  (when verbose
		    (zclock-log "I: received reply:")
		    (zmsg-dump msg))

		  ;;  We would handle malformed replies better in real code
		  (assert (>= (zmsg-size msg) 3))

		  (let ((header (zmsg-pop msg)))
		    (assert (zframe-streq header *mdpc-client*))
		    (zframe-destroy header))

		  (let ((reply-service (zmsg-pop msg)))
		    (assert (zframe-streq reply-service service))
		    (zframe-destroy reply-service))

		  (zmsg-destroy request)
		  (return-from mdcli-send msg))) ;;  Success
	       ((plusp (decf retries-left))
		(when verbose
		  (zclock-log "W: no reply, reconnecting..."))
		(s-mdcli-connect-to-broker mdcli))
	       (t
		(when verbose
		  (zclock-log "W: permanent error, abandoning"))
		(loop-finish)))))) ;;  Give up
  (when (zctx-interrupted)
    (format t "W: interrupt received, killing client...~%"))
  (zmsg-destroy request)
  nil)
