;;  Lazy Pirate client
;;  Use zmq_poll to do a safe request-reply
;;  To run, start lpserver and then randomly kill/restart it

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defconstant +request-timeout+ 2500) ;;  msecs, (> 1000!)
(defconstant +request-retries+ 3) ;;  Before we abandon
(defparameter *server-endpoint* "tcp://localhost:5555")

(defun main ()
  (with-zctx (ctx)
    (format t "I: connecting to server...~%")
    (with-zsockets ctx
	((client :zmq-req))
      (zsocket-connect client *server-endpoint*)

      (loop
	 with sequence = 0
	 with retries-left = +request-retries+
	 while (and (plusp retries-left)
		    (not (zctx-interrupted)))
	 with request
	 do
	   ;;  We send a request, then we work to get a reply
	   (setf request (format nil "~d" (incf sequence)))
	   (zstr-send client request)

	   (loop with expect-reply = t
	      while expect-reply do
		;;  Poll socket for a reply, with timeout
		(with-zpollset (items (client :zmq-pollin))
		  (unless (zpollset-poll items 1 (* +request-timeout+ +zmq-poll-msec+))
		    (loop-finish)) ;;  Interrupted

		  ;;  .split process server reply
		  ;;  Here we process a server reply and exit our loop if the
		  ;;  reply is valid. If we didn't a reply we close the client
		  ;;  socket and resend the request. We try a number of times
		  ;;  before finally abandoning:

		  (cond ((zpollset-events items 0)
			 ;;  We got a reply from the server, must match sequence
			 (let ((reply (zstr-recv client)))
			   (unless reply
			     (loop-finish)) ;;  Interrupted
			   (if (= (parse-integer reply) sequence)
			       (progn
				 (format t "I: server replied OK (~s)~%" reply)
				 (setf retries-left +request-retries+
				       expect-reply nil))
			       (format t "E: malformed reply from server: ~s~%" reply))))
			((zerop (decf retries-left))
			 (format t "E: server seems to be offline, abandoning~%"))
			(t
			 (format t "W: no response from server, retrying...~%")
			 ;;  Old socket is confused; close it and open a new one
			 (zsocket-destroy ctx client)
			 (format t "I: reconnecting to server...~%")
			 (setf client (zsocket-new ctx :zmq-req))
			 (zsocket-connect client *server-endpoint*)
			 ;;  Send request again, on new socket
			 (zstr-send client request))))))))
  0)
