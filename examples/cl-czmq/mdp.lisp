;;
;;  mdp.h
;;  Majordomo Protocol definitions
;;

;;  This is the version of MDP/Client we implement
(defparameter *mdpc-client* "MDPC01")

;;  This is the version of MDP/Worker we implement
(defparameter *mdpw-worker* "MDPW01")

;;  MDP/Server commands, as strings
(defparameter *mdpw-ready*      (string (code-char 1)))
(defparameter *mdpw-request*    (string (code-char 2)))
(defparameter *mdpw-reply*      (string (code-char 3)))
(defparameter *mdpw-heartbeat*  (string (code-char 4)))
(defparameter *mdpw-disconnect* (string (code-char 5)))

(defparameter *mdps-commands*
  #(nil "READY" "REQUEST" "REPLY" "HEARTBEAT" "DISCONNECT"))

(defun mdps-command (command)
  (let ((index (char-code (char command 0))))
    (aref *mdps-commands* index)))
