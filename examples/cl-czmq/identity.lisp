;;  Demonstrate request-reply identities

(ql:quickload "cl-czmq")
(use-package :cl-czmq)

(defun main ()
  (with-zctx (context)
    (with-zsockets context
	((sink :zmq-router)
	 (anonymous :zmq-req)
	 (identified :zmq-req))
      (zsocket-bind sink "inproc://example")

      ;;  First allow 0MQ to set the identity
      (zsocket-connect anonymous "inproc://example")
      (zstr-send anonymous "ROUTER uses a generated UUID")

      (let ((zmsg (zmsg-recv sink)))
	(zmsg-dump zmsg)
	(zmsg-destroy zmsg))

      ;;  Then set the identity ourselves
      (zsocket-set-identity identified "PEER2")
      (zsocket-connect identified "inproc://example")
      (zstr-send identified "ROUTER socket uses REQ's socket identity")

      (let ((zmsg (zmsg-recv sink)))
	(zmsg-dump zmsg)
	(zmsg-destroy zmsg))))

  0)
