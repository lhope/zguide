;;  Hello World server

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  ;;  Socket to talk to clients
  (with-zctx (context)
    (with-zsockets context
	((responder :zmq-rep))
      (assert (= 5555 (zsocket-bind responder "tcp://*:5555")))
      (loop
	 (zstr-recv responder)
	 (format t "Received Hello~%")
	 (sleep 1)          ;;  Do some 'work'
	 (zstr-send responder "World")))))


