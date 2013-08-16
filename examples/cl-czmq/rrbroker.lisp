;;  Simple request-reply broker

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  ;;  Prepare our context and sockets
  (with-zctx (context)
    (with-zsockets context
	((frontend :zmq-router)
	 (backend :zmq-dealer))
      (zsocket-bind frontend "tcp://*:5559")
      (zsocket-bind backend  "tcp://*:5560")

      ;;  Initialize poll set
      (with-zpollset
	  (items (frontend :zmq-pollin)
		 (backend :zmq-pollin))

	;;  Switch messages between sockets
	(loop do
	   ;;        zmq_msg_t message;
	     (zpollset-poll items 2 -1)
	     (when (member :zmq-pollin (zpollset-events items 0))
	       (loop do
		    ;;  Process all parts of the message
		    (let* ((message (zframe-recv frontend))
			   (more (zframe-more message)))
		      (apply #'zframe-send message backend (when more '(:zframe-more)))
		      (unless more
			(loop-finish))))) ;; Last message part
	     (when (member :zmq-pollin (zpollset-events items 1))
	       (loop do
		  ;;  Process all parts of the message
		  (let* ((message (zframe-recv backend))
			 (more (zframe-more message)))
		    (apply #'zframe-send message frontend (when more '(:zframe-more)))
		    (unless more
		      (loop-finish))))))))) ;;  Last message part

  ;;  We never get here, but clean up anyhow
  0)
