#!/usr/bin/env bash

# python3 ./dslogs.py  out.log -c 5

python3 ./dstest.py TestUnreliable1 \
                     -p1 -n100


# TestStaticShards
# TestJoinLeave
# TestSnapshot
# TestMissChange
# TestConcurrent1
# TestConcurrent2
# TestConcurrent3
# TestUnreliable1
# TestUnreliable2
# TestUnreliable3
# TestChallenge1Delete
# TestChallenge2Unaffected
# TestChallenge2Partial


# scp -r 6.824-lab/ root@120.46.192.114:/home/yang/

# ssh root@120.46.192.114