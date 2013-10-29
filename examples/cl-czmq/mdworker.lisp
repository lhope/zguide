;;  Majordomo Protocol worker example
;;  Uses the mdwrk API to hide all MDP aspects

;;  Lets us build this source without creating a library

(ql:quickload '("cl-czmq" "cl-launch"))
(use-package :cl-czmq)

(load "mdwrkapi.lisp")

(defun main (&optional (args cl-launch:*arguments*))
  (let ((verbose (and (car args) (string= (car args) "-v"))))
    (let ((session (mdwrk-new "tcp://localhost:5555" "echo" verbose)))

      (loop with reply do
	   (let ((request (mdwrk-recv session reply)))
	     (unless request
	       (loop-finish)) ;;  Worker was interrupted
	     (setf reply request))) ;;  Echo is complex... :-)
      (mdwrk-destroy session)))
  0)
