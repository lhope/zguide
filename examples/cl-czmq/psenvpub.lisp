;;  Pubsub envelope publisher
;;  Note that the zhelpers.h file also provides s_sendmore

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  ;;  Prepare our context and publisher
  (with-zctx (context)
    (with-zsockets context
	((publisher :zmq-pub))
      (zsocket-bind publisher "tcp://*:5563")

      (loop do
	   ;;  Write two messages, each with an envelope and content
	   (zstr-sendm publisher "A")
	   (zstr-send publisher "We don't want to see this")
	   (zstr-sendm publisher "B")
	   (zstr-send publisher "We would like to see this")
	   (sleep 1))))
  ;;  We never get here, but clean up anyhow
  0)
