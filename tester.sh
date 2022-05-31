#!/bin/bash

for i in {3..12} 
do
    port=$((i*500))

    echo "Testing port $port"
    python3 check.py $port

    echo ""
    echo "Sleeping for 5 sec"
    echo ""
    sleep 5
done