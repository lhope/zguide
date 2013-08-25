;;  Task sink - design 2
;;  Adds pub-sub flow to send kill signal to workers

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  (with-zctx (context)
    (with-zsockets context
	((receiver :zmq-pull)
	 (controller :zmq-pub))
      ;;  Soacket to receive messages on
      (zsocket-bind receiver "tcp://*:5558")

      ;;  Socket for worker control
      (zsocket-bind controller "tcp://*:5559")

      ;;  Wait for start of batch
      (zstr-recv receiver)

      ;;  Start our clock now
      (let ((start-time (zclock-time)))

	;;  Process 100 confirmations
	(dotimes (task-nbr 100)
	  (zstr-recv receiver)
	  (if (zerop (mod task-nbr 100))
	      (format t ":")
	      (format t "."))
	  (finish-output))

	;;  Calculate and report duration of batch
	(format t "Total elapsed time: ~d msec~%"
		(- (zclock-time) start-time)))

      ;;  Send kill signal to workers
      (zstr-send controller "KILL")))

  0)
