#!/usr/bin/env bash

# trap 'exit 1' INT

# TestSnapshotBasic2D \
# TestSnapshotInstall2D \
# TestSnapshotInstallUnreliable2D \
# TestSnapshotInstallCrash2D \
# TestSnapshotInstallUnCrash2D \
# TestSnapshotAllCrash2D \
# python3 ./dslogs.py  output.log -c 5

python3 ./dstest.py TestSnapshotBasic2D \
TestSnapshotInstall2D \
TestSnapshotInstallUnreliable2D \
TestSnapshotInstallCrash2D \
TestSnapshotInstallUnCrash2D \
TestSnapshotAllCrash2D \
                    -r -p6 -n100
