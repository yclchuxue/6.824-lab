#!/usr/bin/env bash

trap 'exit 1' INT

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

# T2B=(TestBasicAgree2B TestRPCBytes2B For2023TestFollowerFailure2B For2023TestLeaderFailure2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B
# TestCount2B)
# python3 ./dslogs.py  output.log -c 5
T2=(TestPersist12C TestPersist22C TestPersist32C TestFigure82C TestUnreliableAgree2C TestFigure8Unreliable2C TestReliableChurn2C TestUnreliableChurn2C)

# for((i = 0; i < 8; i++));
# do
# python3 ./dstest.py $T2[i] 
# done
python3 ./dstest.py TestPersist12C \
                    TestPersist22C \
                    TestPersist32C \
                    TestFigure82C \
                    TestUnreliableAgree2C \
                    TestFigure8Unreliable2C \
                    TestReliableChurn2C \
                    TestUnreliableChurn2C \
                    -r -p10 -n1000
                    