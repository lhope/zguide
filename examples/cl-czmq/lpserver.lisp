;;  Lazy Pirate server
;;  Binds REQ socket to tcp://*:5555
;;  Like hwserver except:
;;   - echoes request as-is
;;   - randomly runs slowly, or exits to simulate a crash.

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  (setf *random-state* (make-random-state t))

  (with-zctx (context)
    (with-zsockets context ((server :zmq-rep))
      (zsocket-bind server "tcp://*:5555")

      (loop with cycles = 0 do
	   (let ((request (zstr-recv server)))
	     (incf cycles)

	     ;;  Simulate various problems, after a few cycles
	     (cond ((and (> cycles 3) (zerop (random 3)))
		    (format t "I: simulating a crash~%")
		    (loop-finish))
		   ((and (> cycles 3) (zerop (random 3)))
		    (format t "I: simulating CPU overload~%")
		    (sleep 2)))
	     (format t "I: normal request (~s)~%" request)
	     (sleep 1) ;;  Do some heavy work
	     (zstr-send server request))))))
