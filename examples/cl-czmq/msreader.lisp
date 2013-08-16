;;  Reading from multiple sockets
;;  This version uses a simple recv loop

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  (with-zctx (context)
    (with-zsockets context
	((receiver :zmq-pull)
	 (subscriber :zmq-sub))
      ;;  Connect to task ventilator
      (zsocket-connect receiver "tcp://localhost:5557")

      ;;  Connect to weather server
      (zsocket-connect subscriber "tcp://localhost:5556")
      (zsocket-set-subscribe subscriber "10001 ")

      ;;  Process messages from both sockets
      ;;  We prioritize traffic from the task ventilator
      (loop do
	   (loop do
		(if (zstr-recv-nowait receiver)
		    nil ;;  Process task
		    (loop-finish)))
	   (loop do
		(if (zstr-recv-nowait subscriber)
		    nil ;;  Process weather update
		    (loop-finish)))
	   ;;  No activity, so sleep for 1 msec
	   (sleep 1))))
  0)
