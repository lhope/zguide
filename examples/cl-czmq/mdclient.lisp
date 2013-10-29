;;  Majordomo Protocol client example
;;  Uses the mdcli API to hide all MDP aspects

;;  Lets us build this source without creating a library

(ql:quickload '("cl-czmq" "cl-launch"))
(use-package :cl-czmq)

(load "mdcliapi.lisp")

(defun main (&optional (args cl-launch:*arguments*))
  (let ((verbose (and (car args) (string= (car args) "-v"))))
    (let ((session (mdcli-new "tcp://localhost:5555" verbose)))

      (loop for count below 100000
	 for request = (zmsg-new) do
	   (zmsg-pushstr request "Hello world")
	   (let ((reply (mdcli-send session "echo" request)))
	     (if reply
		 (zmsg-destroy reply)
		 (loop-finish))) ;;  Interrupt or failure

	 finally
	   (format t "~d requests/replies processed~%" count))
      (mdcli-destroy session)))
  0)
