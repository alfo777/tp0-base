#!/bin/bash

while true
do
RESULT=$(echo "OK" | nc server 12345 2> /dev/null ) 

if [[ $RESULT == "OK" ]]; then
    echo "[SCRIPT] server up!" > result.txt
else
    echo "[SCRIPT] server down..." > result.txt
fi

sleep 5 
cat result.txt
done