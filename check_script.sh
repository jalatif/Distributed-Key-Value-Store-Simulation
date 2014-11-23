#!/bin/bash

for i in `seq 1 10`; do
   ./KVStoreGrader.sh 2> /dev/null | grep -i 'Test 5 Score' | head -1 | awk '{print $4}'
done
