;;  Report 0MQ version

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

;; we get the czmq version instead.
(defun main ()
  (format t "Current CZMQ version is ~d.~d.~d~%"
	  +czmq-version-major+
	  +czmq-version-minor+
	  +czmq-version-patch+)
  0)
