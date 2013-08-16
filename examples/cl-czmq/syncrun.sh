#!/bin/bash

echo "Starting subscribers..."
for ((a=0; a<10; a++)); do
    ./run syncsub.lisp &
    # This is a workaround for parallel compilation not working.
    sleep 5
done
echo "Starting publisher..."
./run syncpub.lisp
