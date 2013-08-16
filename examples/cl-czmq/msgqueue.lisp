;;  Simple message queuing broker
;;  Same as request-reply broker but using QUEUE device


(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  (with-zctx (context)
    (with-zsockets context
	((frontend :zmq-router)
	 (backend :zmq-dealer))
      ;;  Socket facing clients
      (assert (= 5559 (zsocket-bind frontend "tcp://*:5559")))

      ;;  Socket facing services
      (assert (= 5560 (zsocket-bind backend "tcp://*:5560")))

      ;;  Start the proxy
      (zsocket-proxy frontend backend)))

  ;;  We never get here...
  0)
