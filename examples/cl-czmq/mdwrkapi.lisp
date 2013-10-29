;;  mdwrkapi class - Majordomo Protocol Worker API
;;  Implements the MDP/Worker spec at http://rfc.zeromq.org/spec:7.

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(load "mdp.lisp")

;;  Reliability parameters
(defparameter *heartbeat-liveness* 3) ;;  3-5 is reasonable

;;  .split worker class structure
;;  This is the structure of a worker API instance. We use a pseudo-OO
;;  approach in a lot of the C examples, as well as the CZMQ binding:

;;  Structure of our class
;;  We access these properties only via class methods

(defclass mdwrk ()
  ((ctx :initarg :ctx) ;;  Our context
   (broker :initarg :broker)
   (service :initarg :service)
   (worker :initarg :worker) ;;  Socket to broker
   (verbose :initarg :verbose) ;;  Print activity to stdout

   ;;  Heartbeat management
   (heartbeat-at :initarg :heartbeat-at) ;;  When to send HEARTBEAT
   (liveness :initarg :liveness) ;;  How many attempts left
   (heartbeat :initarg :heartbeat) ;;  Heartbeat delay, msecs
   (reconnect :initarg :reconnect) ;;  Reconnect delay, msecs

   (expect-reply :initarg :expect-reply) ;;  Zero only at start
   (reply-to :initarg :reply-to))) ;;  Return identity, if any

;;  .split utility functions
;;  We have two utility functions; to send a message to the broker and
;;  to (re)connect to the broker:

;;  Send message to broker
;;  If no msg is provided, creates one internally

(defun s-mdwrk-send-to-broker (mdwrk command option msg)
  (setf msg (if msg (zmsg-dup msg) (zmsg-new)))

  ;;  Stack protocol envelope to start of message
  (when option
    (zmsg-pushstr msg option))
  (zmsg-pushstr msg command)
  (zmsg-pushstr msg *mdpw-worker*)
  (zmsg-pushstr msg "")

  (with-slots (worker verbose) mdwrk
    (when verbose
      (zclock-log "I: sending ~S to broker" (mdps-command command))
      (zmsg-dump msg))
    (zmsg-send msg worker)))

;;  Connect or reconnect to broker

(defun s-mdwrk-connect-to-broker (mdwrk)
  (with-slots (worker ctx broker verbose service liveness heartbeat heartbeat-at) mdwrk
    (when worker
      (zsocket-destroy ctx worker))
    (setf worker (zsocket-new ctx :zmq-dealer))
    (zsocket-connect worker "~A" broker)
    (when verbose
      (zclock-log "I: connecting to broker at ~S..." broker))

    ;;  Register service with broker
    (s-mdwrk-send-to-broker mdwrk *mdpw-ready* service nil)

    ;;  If liveness hits zero, queue is considered disconnected
    (setf liveness *heartbeat-liveness*
	  heartbeat-at (+ (zclock-time) heartbeat)))
  nil)

;;  .split constructor and destructor
;;  Here we have the constructor and destructor for our mdwrk class:

;;  Constructor

(defun mdwrk-new (broker service verbose)
  (assert broker)
  (assert service)

  (let ((mdwrk
	 (make-instance 'mdwrk
			:ctx (zctx-new)
			:broker (copy-seq broker)
			:service (copy-seq service)
			:verbose verbose
			:heartbeat 2500 ;;  msecs
			:reconnect 2500 ;;  msecs
			:worker nil
			:expect-reply nil))) ;;  Zero only at start
    (s-mdwrk-connect-to-broker mdwrk)
    mdwrk))

;;  Destructor

(defun mdwrk-destroy (mdwrk)
  (when mdwrk
    (zctx-destroy (slot-value mdwrk 'ctx))))

;;  .split configure worker
;;  We provide two methods to configure the worker API. You can set the
;;  heartbeat interval and retries to match the expected network performance.

;;  Set heartbeat delay

(defun mdwrk-set-heartbeat (mdwrk heartbeat)
  (setf (slot-value mdwrk 'heartbeat) heartbeat))

;;  Set reconnect delay

(defun mdwrk-set-reconnect (mdwrk reconnect)
  (setf (slot-value mdwrk 'reconnect) reconnect))

;;  .split recv method
;;  This is the {{recv}} method; it's a little misnamed because it first sends
;;  any reply and then waits for a new request. If you have a better name
;;  for this, let me know.

;;  Send reply, if any, to broker and wait for next request.

(defun mdwrk-recv (mdwrk reply)
  ;;  Format and send the reply if we were provided one
  (with-slots (reconnect expect-reply reply-to
	       heartbeat heartbeat-at worker verbose liveness) mdwrk
    (assert (or reply (not expect-reply)))
    (when reply
      (assert reply-to)
      (zmsg-wrap reply reply-to)
      (s-mdwrk-send-to-broker mdwrk *mdpw-reply* nil reply)
      (zmsg-destroy reply))
    (setf expect-reply t)

    (loop do
	 (with-zpollset (items (worker :zmq-pollin))
	   (unless (zpollset-poll items 1 heartbeat)
	     (loop-finish)) ;;  Interrupted

	   (cond ((zpollset-pollin items 0)
		  (let ((msg (zmsg-recv worker)))
		    (unless msg
		      (loop-finish)) ;;  Interrupted
		    (when verbose
		      (zclock-log "I: received message from broker:")
		      (zmsg-dump msg))
		    (setf liveness *heartbeat-liveness*)

		    ;;  Don't try to handle errors, just assert noisily
		    (assert (>= (zmsg-size msg) 3))

		    (let ((empty (zmsg-pop msg)))
		      (assert (zframe-streq empty ""))
		      (zframe-destroy empty))

		    (let ((header (zmsg-pop msg)))
		      (assert (zframe-streq header *mdpw-worker*))
		      (zframe-destroy header))

		    (let ((command (zmsg-pop msg)))
		      (cond ((zframe-streq command *mdpw-request*)
			     ;;  We should pop and save as many addresses as there are
			     ;;  up to a null part, but for now, just save one...
			     (setf reply-to (zmsg-unwrap msg))
			     (zframe-destroy command)
			     ;;  .split process message
			     ;;  Here is where we actually have a message to process; we
			     ;;  return it to the caller application:

			     (return-from mdwrk-recv msg)) ;;  We have a request to process
			    ((zframe-streq command *mdpw-heartbeat*)
			     ;;  Do nothing for heartbeats
			     )
			    ((zframe-streq command *mdpw-disconnect*)
			     (s-mdwrk-connect-to-broker mdwrk))
			    (t
			     (zclock-log "E: invalid input message")
			     (zmsg-dump msg)))
		      (zframe-destroy command)
		      (zmsg-destroy msg))))
		 ((zerop (decf liveness))
		  (when verbose
		    (zclock-log "W: disconnected from broker - retrying..."))
		  (zclock-sleep reconnect)
		  (s-mdwrk-connect-to-broker mdwrk)))

	   ;;  Send HEARTBEAT if it's time
	   (when (> (zclock-time) heartbeat-at)
	     (s-mdwrk-send-to-broker mdwrk *mdpw-heartbeat* nil nil)
	     (setf heartbeat-at (+ (zclock-time) heartbeat))))
       finally
	 (when (zctx-interrupted)
	   (format t "W: interrupt received, killing worker...~%")))))
