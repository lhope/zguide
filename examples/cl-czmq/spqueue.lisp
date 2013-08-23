;;  Simple Pirate broker
;;  This is identical to load-balancing pattern, with no reliability
;;  mechanisms. It depends on the client for recovery. Runs forever.

(ql:quickload '("cl-czmq" "lparallel"))
(use-package :cl-czmq)
(use-package :lparallel.queue)

(defparameter *worker-ready* #(1)) ;;  Signals worker is ready

(defun main ()
  (with-zctx (ctx)
    (with-zsockets ctx
	((frontend :zmq-router)
	 (backend :zmq-router))
      (zsocket-bind frontend "tcp://*:5555") ;;  For clients
      (zsocket-bind backend  "tcp://*:5556") ;;  For workers

      ;;  Queue of available workers
      (loop with workers = (make-queue)

	;;  The body of this example is exactly the same as lbbroker2.
	;;  .skip
	 do
	   (with-zpollset (items
			   (backend :zmq-pollin)
			   (frontend :zmq-pollin))
	     ;;  Poll frontend only if we have available workers
	     (unless (zpollset-poll items (if (plusp (queue-count workers)) 2 1) -1)
	       (loop-finish))

	     ;;  Handle worker activity on backend
	     (when (zpollset-pollin items 0)
	       ;;  Use worker identity for load-balancing
	       (let ((msg (zmsg-recv backend)))
		 (unless msg
		   (loop-finish)) ;;  Interrupted
		 (let ((identity (zmsg-unwrap msg)))
		   (push-queue identity workers))

		 ;;  Forward message to client if it's not a READY
		 (let ((frame (zmsg-first msg)))
		   (if (equalp (zframe-data frame) *worker-ready*)
		       (zmsg-destroy msg)
		       (zmsg-send msg frontend)))))
	     (when (zpollset-pollin items 1)
	       ;;  Get client request, route to first available worker
	       (let ((msg (zmsg-recv frontend)))
		 (when msg
		   (zmsg-wrap msg (pop-queue workers))
		   (zmsg-send msg backend)))))
	 finally ;;  When we're done, clean up properly
	   (loop while (plusp (queue-count workers)) do
		(let ((frame (pop-queue workers)))
		  (zframe-destroy frame))))))
  0)
