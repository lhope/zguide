;;  Task sink
;;  Binds PULL socket to tcp://localhost:5558
;;  Collects results from workers via that socket

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  ;;  Prepare our context and socket
  (with-zctx (context)
    (with-zsockets context
	((receiver :zmq-pull))
      (zsocket-bind receiver "tcp://*:5558")

      ;;  Wait for start of batch
      (zstr-recv receiver)

      ;;  Start our clock now
      (let ((start-time (get-internal-real-time)))

	;; Process 100 confirmations
	(dotimes (task-nbr 100)
	  (zstr-recv receiver)
	  (if (zerop (mod task-nbr 10))
	      (format t ":")
	      (format t "."))
	  (finish-output))

	;;  Calculate and report duration of batch
	(format t "Total elapsed time: ~d msec~%"
		(- (get-internal-real-time) start-time)))))
  0)
