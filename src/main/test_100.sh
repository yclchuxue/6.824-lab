#!bin/bash

for((i = 1; i <= 10; i++));
do
bash test-mr.sh > test.txt
done