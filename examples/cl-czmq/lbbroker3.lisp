;;  Load-balancing broker
;;  Demonstrates use of the CZMQ API and reactor style
;;
;;  The client and worker tasks are identical from the previous example.


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

;;  Our load-balancer structure, passed to reactor handlers
(defstruct lbbroker
  frontend ;;  Listen to clients
  backend  ;;  Listen to workers
  workers) ;;  List of ready workers

;;  reactor design
;;  In the reactor design, each time a message arrives on a socket, the
;;  reactor passes it to a handler function. We have two handlers; one
;;  for the frontend, one for the backend:

;;  Handle input from client, on frontend
(defun s-handle-frontend (loop poller lbbroker)
  (declare (ignore poller))
  (let ((msg (zmsg-recv (lbbroker-frontend lbbroker))))
    (when msg
      (zmsg-wrap msg (dequeue (lbbroker-workers lbbroker)))
      (zmsg-send msg (lbbroker-backend lbbroker))

      ;;  Cancel reader on frontend if we went from 1 to 0 workers
      (when (zerop (queue-length (lbbroker-workers lbbroker)))
	(with-zpollset (poller ((lbbroker-frontend lbbroker) :zmq-pollin))
	  (zloop-poller-end loop poller)))))
  t)

;;  Handle input from worker, on backend
(defun s-handle-backend (loop poller lbbroker)
  (declare (ignore poller))
  ;;  Use worker identity for load-balancing
  (let ((msg (zmsg-recv (lbbroker-backend lbbroker))))
    (when msg
      (let ((identity (zmsg-unwrap msg)))
        (enqueue (lbbroker-workers lbbroker) identity)

        ;;  Enable reader on frontend if we went from 0 to 1 workers
        (when (= 1 (queue-length (lbbroker-workers lbbroker)))
	  (with-zpollset (poller ((lbbroker-frontend lbbroker) :zmq-pollin))
            (zloop-poller loop poller #'s-handle-frontend lbbroker)))
        ;;  Forward message to client if it's not a READY
	(let ((frame (zmsg-first msg)))
	  (if (equalp (zframe-data frame) *worker-ready*)
	      (zmsg-destroy msg)
	      (zmsg-send msg (lbbroker-frontend lbbroker)))))))
  t)

;;  main task
;;  And the main task now sets up child tasks, then starts its reactor.
;;  If you press Ctrl-C, the reactor exits and the main task shuts down.
;;  Because the reactor is a CZMQ class, this example may not translate
;;  into all languages equally well.

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

      (let ((lbbroker (make-lbbroker
		       :frontend frontend
		       :backend  backend
		       ;;  Queue of available workers
		       :workers (make-queue))))

	;;  Prepare reactor and fire it up
	(with-zloop (reactor)
	  (with-zpollset (poller ((lbbroker-backend lbbroker) :zmq-pollin))
	    (zloop-poller reactor poller #'s-handle-backend lbbroker))
	  (zloop-start reactor))

	;;  When we're done, clean up properly
	(loop for frame = (dequeue (lbbroker-workers lbbroker))
	   while frame do
	     (zframe-destroy frame)))))
0)
