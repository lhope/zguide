;;  Simple Pirate worker
;;  Connects REQ socket to tcp://*:5556
;;  Implements worker part of load-balancing

(ql:quickload "cl-czmq")
(use-package :cl-czmq)
(defparameter *worker-ready* #(1)) ;;  Signals worker is ready

(defun main ()
  (with-zctx (ctx)
    (with-zsockets ctx ((worker :zmq-req))
      ;;  Set random identity to make tracing easier
      (setf *random-state* (make-random-state t))
      (let ((identity (format nil "~4,'0x-~4,'0x" (random #x10000) (random #x10000))))
	(zsocket-set-identity worker identity)
	(zsocket-connect worker "tcp://localhost:5556")

	;;  Tell broker we're ready for work
	(format t "I: (~a) worker ready~%" identity)
	(let ((frame (zframe-new *worker-ready*)))
	  (zframe-send frame worker))

	(loop with cycles = 0 do
	     (let ((msg (zmsg-recv worker)))
	       (unless msg
		 (loop-finish)) ;;  Interrupted

	       ;;  Simulate various problems, after a few cycles
	       (incf cycles)
	       (cond ((and (> cycles 3) (zerop (random 5)))
		      (format t "I: (~a) simulating a crash~%" identity)
		      (zmsg-destroy msg)
		      (loop-finish))
		     ((and (> cycles 3) (zerop (random 5)))
		      (format t "I: (~a) simulating CPU overload~%" identity)
		      (sleep 3)
		      (when (zctx-interrupted)
			(loop-finish))))
	       (format t "I: (~a) normal reply~%" identity)
	       (sleep 1) ;;  Do some heavy work
	       (zmsg-send msg worker))))))
  0)
