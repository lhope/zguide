;;  Synchronized publisher

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defconstant +subscribers-expected+
  10) ;;  We wait for 10 subscribers

(defun main ()
  (with-zctx (context)

    (with-zsockets context
	((publisher :zmq-pub)
	 (syncservice :zmq-rep))

      ;;  Socket to talk to clients
      (zsocket-set-sndhwm publisher 1100000)
      (zsocket-bind publisher "tcp://*:5561")

      ;;  Socket to receive signals
      (zsocket-bind syncservice "tcp://*:5562")

      ;;  Get synchronization from subscribers
      (format t "Waiting for subscribers~%")
      (loop repeat +subscribers-expected+ do
	   ;;  - wait for synchronization request
	 (zstr-recv syncservice)
	   ;;  - send synchronization reply
	 (zstr-send syncservice ""))

      ;;  Now broadcast exactly 1M updates followed by END
      (format t "Broadcasting messages~%")
      (dotimes (i 1000000)
	(zstr-send publisher "Rhubarb"))

      (zstr-send publisher "END")))
  0)
