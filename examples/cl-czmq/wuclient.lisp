;;  Weather update client
;;  Connects SUB socket to tcp://localhost:5556
;;  Collects weather updates and finds avg temp in zipcode

(ql:quickload '("cl-czmq" "cl-launch"))
(use-package :cl-czmq)

;; don't ever use this outside a toy example.
(defun read-3 (string)
  (with-input-from-string (var string)
    (values (read var) (read var) (read var))))

(defun main (&optional (argv cl-launch:*arguments*))
  ;;  Socket to talk to server
  (format t "Collecting updates from weather server...~%")
  (with-zctx (context)
    (with-zsockets context
	((subscriber :zmq-sub))
      (assert (zerop (zsocket-connect subscriber "tcp://localhost:5556")))

      ;;  Subscribe to zipcode, default is NYC, 10001
      (let ((filter (if argv (car argv) "10001 ")))
	(zsocket-set-subscribe subscriber filter)

	;;  Process 100 updates
	(loop with total-temp = 0
	   for update-nbr below 100
	   for string = (zstr-recv subscriber) do
	     (multiple-value-bind (zipcode temperature relhumidity)
		 (read-3 string)
	     (declare (ignore zipcode relhumidity))
	     (incf total-temp temperature))
	   finally
	     (format t "Average temperature for zipcode '~s' was ~df~%"
		     filter (round total-temp update-nbr))))))
  0)
