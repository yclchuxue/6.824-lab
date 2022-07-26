#!/usr/bin/env bash

# python3 ./dslogs.py  out.log -c 5

python3 ./dstest.py TestBasic \
                    TestMulti \
                    -r -p6 -n1000


# TestBasic
# TestMulti