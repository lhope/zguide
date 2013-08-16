;;  Multithreaded Hello World server

(ql:quickload '("cl-czmq" "bordeaux-threads"))
(use-package :cl-czmq)

(defun worker-routine (context)
  ;;  Socket to talk to dispatcher
  (with-zsockets context
      ((receiver :zmq-rep))
    (zsocket-connect receiver "inproc://workers")

    (loop do
	 (let ((string (zstr-recv receiver)))
	   (format t "Received request: [~s]~%" string))
       ;;  Do some 'work'
	 (sleep 1)
       ;;  Send reply back to client
	 (zstr-send receiver "World"))))


(defun main ()
  (with-zctx (context)
    (with-zsockets context
	((clients :zmq-router)
	 (workers :zmq-dealer))
      ;;  Socket to talk to clients
      (zsocket-bind clients "tcp://*:5555")

      ;;  Socket to talk to workers
      (zsocket-bind workers "inproc://workers")

      ;;  Launch pool of worker threads
      (loop repeat 5
	 do
	   (bordeaux-threads:make-thread
	    (lambda () (worker-routine context))))
      ;;  Connect work threads to client threads via a queue proxy
      (zsocket-proxy clients workers)))

  ;;  We never get here, but clean up anyhow
  0)
