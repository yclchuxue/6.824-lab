#!bin/zsh

for((i = 1; i <= 50; i++));
do
time=$(date "+%Y-%m-%d %H:%M:%S")
echo $i "${time}" 
bash test-mr.sh
done