;;  Task worker - design 2
;;  Adds pub-sub flow to receive and respond to kill signal


(ql:quickload "cl-czmq")
(use-package :cl-czmq)


(defun main ()
  (with-zctx (context)
    (with-zsockets context
	((receiver :zmq-pull)
	 (sender :zmq-push)
	 (controller :zmq-sub))
      ;;  Socket to receive messages on
      (zsocket-connect receiver "tcp://localhost:5557")

      ;;  Socket to send messages to
      (zsocket-connect sender "tcp://localhost:5558")

      ;;  Socket for control input
      (zsocket-connect controller "tcp://localhost:5559")
      (zsocket-set-subscribe controller "")

      ;;  Process messages from either socket

      (with-zpollset
	  (items (receiver :zmq-pollin)
		 (controller :zmq-pollin))

	(loop do
	     (zpollset-poll items 2 -1)
	     (when (member :zmq-pollin (zpollset-events items 0))
	       (let ((string (zstr-recv receiver)))
		 (format t "~a." string) ;;  Show progress
		 (finish-output)
		 (sleep (* 0.001 (parse-integer string)))) ;;  Do the work
	       (zstr-send sender "")) ;;  Send results to sink
	     (when (member :zmq-pollin (zpollset-events items 1))
	       (loop-finish)))))) ;;  Exit loop
  0)
