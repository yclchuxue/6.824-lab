#!bin/bash
# rm test1.log
# for((i = 1; i <= 10; i++));
# do

# VERBOSE=1 go test  -run TestFigure8Unreliable2C > output.log
# echo $i
# if cat output.log | grep FAIL 
# then
#     # cat output.log >> output1.log
#     # python3 ./dslogs.py  output.log -c 5
#     echo "FAIL"
#     break
# fi
# done


# T2B=(TestBasicAgree2B TestRPCBytes2B For2023TestFollowerFailure2B For2023TestLeaderFailure2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B TestCount2B)

# T2=(TestPersist12C TestPersist22C TestPersist32C TestFigure82C TestUnreliableAgree2C TestFigure8Unreliable2C TestReliableChurn2C TestUnreliableChurn2C)

# python3 ./dstest.py TestInitialElection2A \
#                     TestReElection2A \
#                     TestManyElections2A\
#                     -r -p10 -n50 -o output.log

python3 ./dstest.py TestBasicAgree2B \
                    TestRPCBytes2B \
                    For2023TestFollowerFailure2B \
                    For2023TestLeaderFailure2B \
                    TestFailAgree2B \
                    TestFailNoAgree2B \
                    TestConcurrentStarts2B \
                    TestRejoin2B \
                    TestBackup2B \
                    TestCount2B \
                    -r -p10 -n50
                    #  -ooutput1.log

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
# 9 TestBackup2B 5
# 10 TestCount2B

# 1 TestPersist12C
# 2 TestPersist22C 5
# 3 TestPersist32C
# 4 TestFigure82C  5
# 5 TestUnreliableAgree2C 5
# 6 TestFigure8Unreliable2C 5
# 7 TestReliableChurn2C  5
# 8 TestUnreliableChurn2C  5
