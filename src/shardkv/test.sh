#!/usr/bin/env bash

# python3 ./dslogs.py  out.log -c 5

python3 ./dstest.py TestStaticShards \
                    -r -p6 -n1000


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