;;  Reading from multiple sockets
;;  This version uses zmq_poll()

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  (with-zctx (context)
    (with-zsockets context
	((receiver :zmq-pull)
	 (subscriber :zmq-sub))
      ;; Connect to task ventilator
      (zsocket-connect receiver "tcp://localhost:5557")

      ;;  Connect to weather server
      (zsocket-connect subscriber "tcp://localhost:5556")
      (zsocket-set-subscribe subscriber "10001 ")

      ;;  Process messages from both sockets
      (with-zpollset (items
		      (receiver :zmq-pollin)
		      (subscriber :zmq-pollin))
	(loop do
	     (zpollset-poll items 2 -1)
	     (when (member :zmq-pollin (zpollset-events items 0))
	       (when (zstr-recv receiver)
		 ;;  Process task
		 ))
	     (when (member :zmq-pollin (zpollset-events items 1))
	       (when (zstr-recv subscriber)
		 ;;  Process weather update
		 ))))))
  0)
