;;  Broker peering simulation (part 1)
;;  Prototypes the state flow

(ql:quickload '("cl-czmq" "cl-launch"))
(use-package :cl-czmq)

(defun main (&optional (args cl-launch:*arguments*))
  ;;  First argument is this broker's name
  ;;  Other arguments are our peers' names
  ;;
  (when (< (length args) 1)
    (format t "syntax: peering1 me {you}...~%")
    (return-from main 0))

  (let ((self (first args)))
    (format t  "I: preparing broker at ~s...~%" self)

    (setf *random-state* (make-random-state t))

    (with-zctx (ctx)
      (with-zsockets ctx
	  ((statebe :zmq-pub)
	   (statefe :zmq-sub))

	;;  Bind state backend to endpoint
	(zsocket-bind statebe "ipc://~a-state.ipc" self)

	;;  Connect statefe to all peers
	(zsocket-set-subscribe statefe "")
	(dolist (peer (rest args))
	  (format t "I: connecting to state backend at ~s~%" peer)
	  (zsocket-connect statefe "ipc://~a-state.ipc" peer))

	;;  .split main loop
	;;  The main loop sends out status messages to peers, and collects
	;;  status messages back from peers. The zmq_poll timeout defines
	;;  our own heartbeat:

	(loop do
	   ;;  Poll for activity, or 1 second timeout
	     (with-zpollset (items (statefe :zmq-pollin))
	       (unless (zpollset-poll items 1 (* 1000 +zmq-poll-msec+))
		 (loop-finish)) ;;  Interrupted

	       ;;  Handle incoming status messages
	       (if (zpollset-pollin items)
		   (let ((peer-name (zstr-recv statefe))
			 (available (zstr-recv statefe)))
		     (format t "~a - ~A workers free~%" peer-name available))
		   ;;  Send random values for worker availability
		   (progn
		     (zstr-sendm statebe self)
		     (zstr-send  statebe "~d" (random 10)))))))))
  0)
