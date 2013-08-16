;;  Pubsub envelope subscriber

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  ;;  Prepare our context and subscriber
  (with-zctx (context)
    (with-zsockets context
	((subscriber :zmq-sub))
      (zsocket-connect subscriber "tcp://localhost:5563")
      (zsocket-set-subscribe subscriber "B")

      (loop do
	   (let (;;  Read envelope with address
		 (address (zstr-recv subscriber))
		 ;;  Read message contents
		 (contents (zstr-recv subscriber)))
	     (format t "[~a] ~a~%" address contents)))))
  ;;  We never get here, but clean up anyhow
  0)
