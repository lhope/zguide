;;  Task worker
;;  Connects PULL socket to tcp://localhost:5557
;;  Collects workloads from ventilator via that socket
;;  Connects PUSH socket to tcp://localhost:5558
;;  Sends results to sink via that socket

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  (with-zctx (context)
    (with-zsockets context
	;; Socket to receive messages on
	((receiver :zmq-pull)
	 ;; Socket to send messages to
	 (sender :zmq-push))
      (zsocket-connect receiver "tcp://localhost:5557")
      (zsocket-connect sender "tcp://localhost:5558")

      ;; Process tasks forever
      (loop
	 (let ((string (zstr-recv receiver)))
	   (format t "~A." string) ;; Show progress
	   (finish-output)
	   (zclock-sleep (parse-integer string)) ;;  Do the work
	   (zstr-send sender ""))))) ;; Send results to sink
  0)
