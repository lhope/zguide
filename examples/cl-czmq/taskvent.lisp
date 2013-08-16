;;  Task ventilator
;;  Binds PUSH socket to tcp://localhost:5557
;;  Sends batch of tasks to workers via that socket

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  (with-zctx (context)
    (with-zsockets context
	;; Socket to send messages on
	((sender :zmq-push)
	 (sink   :zmq-push))
      (zsocket-bind sender "tcp://*:5557")
      (zsocket-connect sink "tcp://localhost:5558")

      (format t "Press Enter when the workers are ready: ") (finish-output)
      (read-char)
      (format t "Sending tasks to workers...~%")

      ;;  The first message is "0" and signals start of batch
      (zstr-send sink "0")

      ;; Initialize random number generator
      (setf *random-state* (make-random-state t))

      ;; Send 100 tasks
      (let ((total-msec 0)) ;; Total expected cost in msecs
	(dotimes (task-nbr 100)
	  ;; Random workload from 1 to 100msecs
	  (let ((workload (1+ (random 100))))
	    (incf total-msec workload)
	    (zstr-send sender "~d" workload)))
	(format t "Total expected cost: ~d msec~%" total-msec))))
  0)
