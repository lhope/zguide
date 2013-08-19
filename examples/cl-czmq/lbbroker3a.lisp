;;  Load-balancing broker
;;  Demonstrates use of the CZMQ API and reactor style
;;
;;  The client and worker tasks are identical from the previous example.

;; This version uses cl-czmq's ability to pass multiple args to zloop-poller.

(ql:quickload "cl-czmq")
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

;; protect against interrupts caused by e.g. garbage collection.
(defmacro retry (&body body)
  (let ((g!val (gensym)))
    `(loop for ,g!val = (progn ,@body)
	when ,g!val return ,g!val)))


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
	   (let ((reply (retry (zstr-recv client))))
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
	   (let ((msg (retry (zmsg-recv worker))))
	     ;;  Get request, send reply

	     (zframe-reset (zmsg-last msg) "OK")
	     (zmsg-send msg worker)))))))

;;  reactor design
;;  In the reactor design, each time a message arrives on a socket, the
;;  reactor passes it to a handler function. We have two handlers; one
;;  for the frontend, one for the backend:

;;  Handle input from client, on frontend
(defun s-handle-frontend (loop poller frontend backend workers)
  (declare (ignore poller))
  (let ((msg (zmsg-recv frontend)))
    (when msg
      (zmsg-wrap msg (dequeue workers))
      (zmsg-send msg backend)

      ;;  Cancel reader on frontend if we went from 1 to 0 workers
      (when (zerop (queue-length workers))
	(with-zpollset (poller (frontend :zmq-pollin))
	  (zloop-poller-end loop poller)))))
  t)

;;  Handle input from worker, on backend
(defun s-handle-backend (loop poller frontend backend workers)
  (declare (ignore poller))
  ;;  Use worker identity for load-balancing
  (let ((msg (zmsg-recv backend)))
    (when msg
      (let ((identity (zmsg-unwrap msg)))
        (enqueue workers identity)

        ;;  Enable reader on frontend if we went from 0 to 1 workers
        (when (= 1 (queue-length workers))
	  (with-zpollset (poller (frontend :zmq-pollin))
            (zloop-poller loop poller #'s-handle-frontend frontend backend workers)))
        ;;  Forward message to client if it's not a READY
	(let ((frame (zmsg-first msg)))
	  (if (equalp (zframe-data frame) *worker-ready*)
	      (zmsg-destroy msg)
	      (zmsg-send msg frontend))))))
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
	   (zthread-new #'client-task))
      (loop repeat +nbr-workers+ do
	   (zthread-new #'worker-task))

      ;;  Queue of available workers
      (let ((workers (make-queue)))

	;;  Prepare reactor and fire it up
	(with-zloop (reactor)
	  ;; (zloop-set-verbose reactor t)
	  (with-zpollset (poller (backend :zmq-pollin))
	    (zloop-poller reactor poller #'s-handle-backend frontend backend workers))
	  (zloop-start reactor))

	;;  When we're done, clean up properly
	(loop for frame = (dequeue workers)
	   while frame do
	     (zframe-destroy frame)))))
0)
