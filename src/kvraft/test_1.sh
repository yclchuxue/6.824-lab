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


python3 ./dstest.py TestBasic3A \
                    -r -p10 -n


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
