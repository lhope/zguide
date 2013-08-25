#!/bin/bash

./run ppqueue.lisp &
sleep 5
for i in 1 2 3 4; do
    ./run ppworker.lisp &
    sleep 5
done
./run lpclient.lisp &
