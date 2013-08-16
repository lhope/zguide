;;  Synchronized subscriber

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  (with-zctx (context)
    (with-zsockets context
	((subscriber :zmq-sub)
	 (syncclient :zmq-req))
      ;;  First, connect our subscriber socket
      (zsocket-connect subscriber "tcp://localhost:5561")
      (zsocket-set-subscribe subscriber "")

      ;;  0MQ is so fast, we need to wait a while...
      (sleep 1)

      ;;  Second, synchronize with publisher
      (zsocket-connect syncclient "tcp://localhost:5562")

      ;;  - send a synchronization request
      (zstr-send syncclient "")

      ;;  - wait for synchronization reply
      (zstr-recv syncclient)

      ;;  Third, get our updates and report how many we got
      (let ((update-nbr 0))
	(loop do
	     (let ((string (zstr-recv subscriber)))
	       (when (string= string "END")
		 (loop-finish)))
	     (incf update-nbr))
	(format t "Received ~d updates~%" update-nbr))))
  0)
