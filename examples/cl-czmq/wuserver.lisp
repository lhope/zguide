;;  Weather update server
;;  Binds PUB socket to tcp://*:5556
;;  Publishes random weather updates

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  ;;  Prepare our context and publisher
  (with-zctx (context)
    (with-zsockets context
	((publisher :zmq-pub))
      (assert (eql 5556 (zsocket-bind publisher "tcp://*:5556")))
      (assert (zerop (zsocket-bind publisher "ipc://weather.ipc")))

      ;;  Initialize random number generator
      (setf *random-state* (make-random-state t))

      (loop
	 ;;  Get values that will fool the boss
	 (let ((zipcode (random 100000))
	       (temperature (- (random 215) 80))
	       (relhumidity (+ (random 50) 10)))
	   ;;  Send message to all subscribers
	   (zstr-send publisher "~5,'0d ~d ~d" zipcode temperature relhumidity)))))
  0)
