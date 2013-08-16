;;  Hello World client
;;  Connects REQ socket to tcp://localhost:5559
;;  Sends "Hello" to server, expects "World" back

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  (with-zctx (context)

    ;;  Socket to talk to server
    (with-zsockets context
	((requester :zmq-req))
      (zsocket-connect requester "tcp://localhost:5559")

      (dotimes (request-nbr 10)
	(zstr-send requester "Hello")
        (let ((string (zstr-recv requester)))
	  (format t "Received reply ~d [~s]~%" request-nbr string)))))
  0)
