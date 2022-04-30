#!bin/bash
# rm test1.log
for((i = 1; i <= 10; i++));
do
# go test -run TestInitialElection2A >> test1.log
# go test -run TestReElection2A >> test1.log
# go test -run TestManyElections2A >> test1.log
# 1 TestBasicAgree2B
# 2 TestRPCBytes2B
# 3 For2023TestFollowerFailure2B
# 4 For2023TestLeaderFailure2B
# 5 TestFailAgree2B
# 6 TestFailNoAgree2B       5
# 7 TestConcurrentStarts2B
# 8 TestRejoin2B
# 9 TestBackup2B
# 10 TestCount2B

VERBOSE=1 go test -run 2A > output.log
echo $i
if cat output.log | grep FAIL 
then
    # cat output.log >> output1.log
    python3 ./dslogs.py  output.log -c 3
    break
fi
done