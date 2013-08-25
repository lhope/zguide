;;  Paranoid Pirate worker

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defconstant +heartbeat-liveness+ 3) ;;  3-5 is reasonable
(defconstant +heartbeat-interval+ 1000) ;;  msecs
(defconstant +interval-init+ 1000) ;;  Initial reconnect
(defconstant +interval-max+ 32000) ;;  After exponential backoff

;;  Paranoid Pirate Protocol constants
(defparameter *ppp-ready* #(1))    ;;  Signals worker is ready
(defparameter *ppp-heartbeat* #(2)) ;;  Signals worker heartbeat

;;  Helper function that returns a new configured socket
;;  connected to the Paranoid Pirate queue

(defun s-worker-socket (ctx)
  ;; (note that zctx cleans up its sockets)
  (let ((worker (zsocket-new ctx :zmq-dealer)))
    (zsocket-connect worker "tcp://localhost:5556")

    ;;  Tell queue we're ready for work
    (format t "I: worker ready~%")
    (let ((frame (zframe-new *ppp-ready*)))
      (zframe-send frame worker))
    worker))

;;  .split main task
;;  We have a single task that implements the worker side of the
;;  Paranoid Pirate Protocol (PPP). The interesting parts here are
;;  the heartbeating, which lets the worker detect if the queue has
;;  died, and vice versa:

(defun main ()
  (with-zctx (ctx)
    (let ((worker (s-worker-socket ctx))

	  ;;  If liveness hits zero, queue is considered disconnected
	  (liveness +heartbeat-liveness+)
	  (interval +interval-init+)

	  ;;  Send out heartbeats at regular intervals
	  (heartbeat-at (+ (get-internal-real-time) +heartbeat-interval+)))

      (setf *random-state* (make-random-state t))
      (loop with cycles = 0 do
	   (with-zpollset (items (worker :zmq-pollin))
	     (unless (zpollset-poll items 1 +heartbeat-interval+)
	       (loop-finish)) ;;  Interrupted

	     (cond ((zpollset-events items 0)
		    ;;  Get message
		    ;;  - 3-part envelope + content -> request
		    ;;  - 1-part HEARTBEAT -> heartbeat
		    (let ((msg (zmsg-recv worker)))
		      (unless msg
			(loop-finish)) ;;  Interrupted

		      ;;  .split simulating problems
		      ;;  To test the robustness of the queue implementation we
		      ;;  simulate various typical problems, such as the worker
		      ;;  crashing or running very slowly. We do this after a few
		      ;;  cycles so that the architecture can get up and running
		      ;;  first:
		      (cond ((= 3 (zmsg-size msg))
			     (incf cycles)
			     (cond ((and (> cycles 3) (zerop (random 5)))
				    (format t "I: simulating a crash~%")
				    (zmsg-destroy msg)
				    (loop-finish))
				   ((and (> cycles 3) (zerop (random 5)))
				    (format t "I: simulating CPU overload~%")
				    (sleep 3)
				    (when (zctx-interrupted)
				      (loop-finish))))
			     (format t "I: normal reply~%")
			     (zmsg-send msg worker)
			     (setf liveness +heartbeat-liveness+)
			     (sleep 1) ;;  Do some heavy work
			     (when (zctx-interrupted)
			       (loop-finish)))
			    (;;  .split handle heartbeats
			     ;;  When we get a heartbeat message from the queue, it means the
			     ;;  queue was (recently) alive, so we must reset our liveness
			     ;;  indicator:
			     (= 1 (zmsg-size msg))
			     (let ((frame (zmsg-first msg)))
			       (if (equalp (zframe-data frame) *ppp-heartbeat*)
				   (setf liveness +heartbeat-liveness+)
				   (progn (format t "E: invalid message~%")
					  (zmsg-dump msg))))
			     (zmsg-destroy msg))
			    (t
			     (format t "E: invalid message~%")
			     (zmsg-dump msg)
			     ;; should destroy here?
			     ))
		      (setf interval +interval-init+)))
		   (;;  .split detecting a dead queue
		    ;;  If the queue hasn't sent us heartbeats in a while, destroy the
		    ;;  socket and reconnect. This is the simplest most brutal way of
		    ;;  discarding any messages we might have sent in the meantime:
		    (zerop (decf liveness))
		    (format t "W: heartbeat failure, can't reach queue~%")
		    (format t "W: reconnecting in ~d msec...~%" interval)
		    (sleep (* 0.001 interval))

		    (when (< interval +interval-max+)
		      (setf interval (* interval 2)))
		    (zsocket-destroy ctx worker)
		    (setf worker (s-worker-socket ctx)
			  liveness +heartbeat-liveness+)))
	     (when (> (get-internal-real-time) heartbeat-at)
	       (setf heartbeat-at (+ (get-internal-real-time) +heartbeat-interval+))
	       (format t "I: worker heartbeat~%")
	       (let ((frame (zframe-new *ppp-heartbeat*)))
		 (zframe-send frame worker)))))))
  0)
