;;  Weather proxy device

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  (with-zctx (context)
    (with-zsockets context
	((frontend :zmq-xsub)
	 (backend  :zmq-xpub))
      ;;  This is where the weather server sits
      (zsocket-connect frontend "tcp://192.168.55.210:5556")

      ;;  This is our public endpoint for subscribers
      (zsocket-bind backend "tcp://10.1.1.0:8100")

      ;;  Run the proxy until the user interrupts us
      (zsocket-proxy frontend backend)))

  0)
