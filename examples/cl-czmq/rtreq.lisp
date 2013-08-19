;;  ROUTER-to-REQ example

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defconstant +nbr-workers+ 10)

(defun s-set-id (socket)
  (let ((identity (format nil "~4,'0x-~4,'0x" (random #x10000) (random #x10000))))
    (zsocket-set-identity socket identity)))

(defun worker-task (&rest args)
  (declare (ignore args))
  (with-zctx (context)
    (with-zsockets context
	((worker :zmq-req))
      (s-set-id worker) ;;  Set a printable identity
      (zsocket-connect worker "tcp://localhost:5671")

      (loop with total = 0 do
	   ;;  Tell the broker we're ready for work
	 (zstr-send worker "Hi Boss")

	   ;;  Get workload from broker, until finished
	 (let ((workload (zstr-recv worker)))
	   (when (string= workload "Fired!")
	     (format t "Completed: ~d tasks~%" total)
	     (loop-finish)))
	 (incf total)

	 ;;  Do some random work
	 (sleep (random 0.5))))))

;;  .split main task
;;  While this example runs in a single process, that is only to make
;;  it easier to start and stop the example. Each thread has its own
;;  context and conceptually acts as a separate process.

(defun main ()
  (with-zctx (context)
    (with-zsockets context
	((broker :zmq-router))
      (zsocket-bind broker "tcp://*:5671")
      (setf *random-state* (make-random-state t))

      (loop repeat +nbr-workers+ do
	   (zthread-new #'worker-task))

      ;;  Run for five seconds and then tell workers to end
      (loop
	 with end-time = (+ (get-internal-real-time) 5000)
	 with workers-fired = 0
	 do
	   ;;  Next message gives us least recently used worker
	   (let ((identity (zstr-recv broker)))
	     (zstr-sendm broker identity))

	   (zstr-recv broker) ;;  Envelope delimiter
	   (zstr-recv broker) ;;  Response from worker
	   (zstr-sendm broker "")

	 ;;  Encourage workers until it's time to fire them
	 (if (< (get-internal-real-time) end-time)
	     (zstr-send broker "Work harder")
	     (progn
	       (zstr-send broker "Fired!")
	       (when (= (incf workers-fired) +nbr-workers+)
		 (loop-finish)))))))
  0)
