;;  Multithreaded relay

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun step1 (context)
  ;;  Connect to step2 and tell it we're ready
  (with-zsockets context
      ((xmitter :zmq-pair))
    (zsocket-connect xmitter "inproc://step2")
    (format t "Step 1 ready, signaling step 2~%")
    (zstr-send xmitter "READY")))

(defun step2 (context)
  ;;  Bind inproc socket before starting step1
  (with-zsockets context
      ((receiver :zmq-pair))
    (zsocket-bind receiver "inproc://step2")

    (zthread-new #'step1 context)

    ;;  Wait for signal and pass it on
    (zstr-recv receiver))

  ;;  Connect to step3 and tell it we're ready
  (with-zsockets context
      ((xmitter :zmq-pair))
    (zsocket-connect xmitter  "inproc://step3")
    (format t  "Step 2 ready, signaling step 3~%")
    (zstr-send xmitter "READY")))

(defun main ()
  (with-zctx (context)
    ;;  Bind inproc socket before starting step2
    (with-zsockets context
	((receiver :zmq-pair))
      (zsocket-bind receiver "inproc://step3")

      (zthread-new #'step2 context)

      ;;  Wait for signal
      (zstr-recv receiver))

    (format t "Test successful!~%")
    0))
