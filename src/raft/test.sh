#!bin/bash
# rm test1.log
for((i = 1; i <= 1000; i++));
do
# go test -run TestInitialElection2A >> test1.log
# go test -run TestReElection2A >> test1.log
# go test -run TestManyElections2A >> test1.log

VERBOSE=1 go test -run 2A > output.log
echo $i
if cat output.log | grep FAIL 
then
    cat output.log >> output1.log
    # python3 ./dslogs.py  output.log -c 7
    # break
fi
done