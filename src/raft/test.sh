#!bin/bash
# rm test1.log
for((i = 1; i <= 10; i++));
do
# go test -run TestInitialElection2A >> test1.log
# go test -run TestReElection2A >> test1.log
# go test -run TestManyElections2A >> test1.log

VERBOSE=1 go test -run TestInitialElection2A > output.log
echo $i
if cat output.log | grep FAIL 
then
    python3 ./dslogs.py  output.log -c 7
    break
fi
done