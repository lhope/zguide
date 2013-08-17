;;  Load-balancing broker
;;  Demonstrates use of the CZMQ API

(ql:quickload '("cl-czmq" "bordeaux-threads"))
(use-package :cl-czmq)

(defconstant +nbr-clients+ 10)
(defconstant +nbr-workers+ 3)
(defparameter *worker-ready* #(1))

;; simple queue
(defun make-queue ()
  (cons nil nil))

(defun enqueue (q obj)
  (if (null (car q))
      (setf (cdr q) (setf (car q) (list obj)))
      (setf (cdr (cdr q)) (list obj)
            (cdr q) (cdr (cdr q))))
  (car q))

(defun dequeue (q)
  (pop (car q)))

(defun queue-length (q)
  (length (car q)))

;;  Basic request-reply client using REQ socket
;;
(defun client-task (&rest args)
  (declare (ignore args))
  (with-zctx (ctx)
    (with-zsockets ctx
	((client :zmq-req))
      (zsocket-connect client "ipc://frontend.ipc")

      ;;  Send request, get reply
      (loop do
	   (zstr-send client "HELLO")
	   (let ((reply (zstr-recv client)))
	     (unless reply
	       (loop-finish)) ;;  Interrupted
	     (format t "Client: ~s~%" reply))
	   (sleep 1)))))

;;  Worker using REQ socket to do load-balancing
;;
(defun worker-task (&rest args)
  (declare (ignore args))
  (with-zctx (ctx)
    (with-zsockets ctx
	((worker :zmq-req))
      (zsocket-connect worker "ipc://backend.ipc")

      ;;  Tell broker we're ready for work
      (let ((frame (zframe-new *worker-ready*)))
	(zframe-send frame worker)

	;;  Process messages as they arrive
	(loop do
	   (let ((msg (zmsg-recv worker)))
	     (unless msg
	       (loop-finish)) ;;  Interrupted
	     (zframe-reset (zmsg-last msg) "OK")
	     (zmsg-send msg worker)))))))

;;  .split main task
;;  Now we come to the main task. This has the identical functionality to
;;  the previous {{lbbroker}} broker example, but uses CZMQ to start child
;;  threads, to hold the list of workers, and to read and send messages:

(defun main ()
  (with-zctx (ctx)
    (with-zsockets ctx
	((frontend :zmq-router)
	 (backend :zmq-router))
      (zsocket-bind frontend "ipc://frontend.ipc")
      (zsocket-bind backend "ipc://backend.ipc")

      (loop repeat +nbr-clients+ do
	   (bordeaux-threads:make-thread #'client-task))
      (loop repeat +nbr-workers+ do
	   (bordeaux-threads:make-thread #'worker-task))

      ;;  Queue of available workers
      (let ((workers (make-queue)))

	;;  .split main load-balancer loop
	;;  Here is the main loop for the load balancer. It works the same way
	;;  as the previous example, but is a lot shorter because CZMQ gives
	;;  us an API that does more with fewer calls:
	(loop do
	     (with-zpollset (items
			     (backend :zmq-pollin)
			     (frontend :zmq-pollin))
	     ;;  Poll frontend only if we have available workers
	       (unless (zpollset-poll items (if (plusp (queue-length workers)) 2 1) -1)
		 (loop-finish)) ;;  Interrupted

	     ;;  Handle worker activity on backend
	       (when (member :zmq-pollin (zpollset-events items 0))
		 ;;  Use worker identity for load-balancing
		 (let ((msg (zmsg-recv backend)))
		   (unless msg
		     (loop-finish)) ;;  Interrupted
		   (let ((identity (zmsg-unwrap msg)))
		     (enqueue workers identity))

		   ;;  Forward message to client if it's not a READY
		   (let ((frame (zmsg-first msg)))
		     (if (equalp (zframe-data frame) *worker-ready*)
			 (zmsg-destroy msg)
			 (zmsg-send msg frontend)))))
	       (when (member :zmq-pollin (zpollset-events items 1))
		 ;;  Get client request, route to first available worker
		 (let ((msg (zmsg-recv frontend)))
		   (when msg
		     (zmsg-wrap msg (dequeue workers))
		     (zmsg-send msg backend))))))


	;;  When we're done, clean up properly
	(loop while (plusp (queue-length workers)) do
	     (let ((frame (dequeue workers)))
	       (zframe-destroy frame))))))
  0)
