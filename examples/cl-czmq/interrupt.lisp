;;  Shows how to handle Ctrl-C

(ql:quickload "cl-czmq")
(use-package :cl-czmq)


;;  Signal handling
;;
;;  Call s_catch_signals() in your application at startup, and then
;;  exit your main loop if s_interrupted is ever 1. Works especially
;;  well with zmq_poll.

(defun main ()
  (with-zctx (context)
    (with-zsockets context
	((socket :zmq-rep))
      (zsocket-bind socket "tcp://*:5555")

      (loop do
	   ;;  Blocking read will exit on a signal
	   (zstr-recv socket)
	   (when (zctx-interrupted)
	     (format t "W: interrupt received, killing server...~%")
	     (loop-finish)))))
  0)
