;;  Weather proxy device

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

;; And this one too...
(defun zmq-proxy (frontend backend)
  (cffi:foreign-funcall "zmq_proxy" :pointer frontend :pointer backend :pointer (cffi:null-pointer) :int))

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
      (zmq-proxy frontend backend)))

  0)
