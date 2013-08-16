;;  Hello World worker
;;  Connects REP socket to tcp://*:5560
;;  Expects "Hello" from client, replies with "World"

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  (with-zctx (context)

    ;;  Socket to talk to clients
    (with-zsockets context
	((responder :zmq-rep))
      (zsocket-connect responder "tcp://localhost:5560")

      (loop do
	 ;;  Wait for next request from client
	 (let ((string (zstr-recv responder)))
	   (format t "Received request: [~s]~%" string))

	 ;;  Do some 'work'
	 (sleep 1)

	 ;;  Send reply back to client
	 (zstr-send responder "World"))))

  ;; We never get here (but clean up anyhow)
  0)
