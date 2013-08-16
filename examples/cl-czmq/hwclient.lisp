;;  Hello World client

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  (format t "Connecting to hello world server...~%")
  (with-zctx (context)
    (with-zsockets context
	((requester :zmq-req))
      (zsocket-connect requester "tcp://localhost:5555")

      (dotimes (request-nbr 10)
	(format t "Sending Hello ~d...~%" request-nbr)
	(zstr-send requester "Hello")
	(zstr-recv requester)
	(format t "Received World ~d~%" request-nbr))))
  0)


