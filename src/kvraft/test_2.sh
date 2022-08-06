#!bin/bash

python3 ./dstest.py TestSnapshotRPC3B \
                    TestSnapshotSize3B \
                    TestSpeed3B \
                    TestSnapshotRecover3B \
                    TestSnapshotRecoverManyClients3B \
                    TestSnapshotUnreliable3B \
                    TestSnapshotUnreliableRecover3B \
                    TestSnapshotUnreliableRecoverConcurrentPartition3B \
                    TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B \
                    -r -p5 -n100

# TestSnapshotRPC3B
# TestSnapshotSize3B
# TestSpeed3B
# TestSnapshotRecover3B
# TestSnapshotRecoverManyClients3B
# TestSnapshotUnreliable3B
# TestSnapshotUnreliableRecover3B
# TestSnapshotUnreliableRecoverConcurrentPartition3B
# TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B


	# //var start time.Time
	# //start = time.Now()

    # //ti := time.Since(start).Milliseconds()

# scp -r 6.824-lab/ root@120.46.192.114:/home/yang/

# ssh root@120.46.192.114