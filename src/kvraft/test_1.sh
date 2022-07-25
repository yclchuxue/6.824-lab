#!bin/bash


python3 ./dstest.py TestBasic3A \
                    TestConcurrent3A \
                    TestUnreliable3A \
                    TestUnreliableOneKey3A \
                    TestOnePartition3A \
                    TestManyPartitionsOneClient3A \
                    TestManyPartitionsManyClients3A \
                    TestPersistConcurrent3A \
                    TestPersistConcurrentUnreliable3A \
                    TestPersistPartition3A \
                    TestPersistPartitionUnreliable3A \
                    TestPersistPartitionUnreliableLinearizable3A \
                    -r -p5 -n100

# python3 ./dstest.py TestSpeed3A \
#                     -r -p5 -n10

# python3 ./dslogs.py  out.log -c 5

# TestBasic3A
# TestSpeed3A
# TestConcurrent3A
# TestUnreliable3A
# TestUnreliableOneKey3A
# TestOnePartition3A
# TestManyPartitionsOneClient3A
# TestManyPartitionsManyClients3A
# TestPersistConcurrent3A
# TestPersistConcurrentUnreliable3A
# TestPersistPartition3A
# TestPersistPartitionUnreliable3A
# TestPersistPartitionUnreliableLinearizable3A
