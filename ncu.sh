cd ncu

#ncu --set full --export test1GPU.ncu-rep -f --target-processes application-only --replay-mode application ../cmake-build-release/ltpmg --benchmark "TPCC_PART" --table_size 8 --batch_size 65536 --epoch_tp 3 --deviceIDs "0"

ncu --set full --export test2GPU.ncu-rep -f --target-processes application-only --replay-mode application ../cmake-build-release/ltpmg --benchmark "TPCC_PART" --table_size 8 --batch_size 65536 --epoch_tp 3 --deviceIDs "1,2"

#ncu --set full --export test4GPU.ncu-rep -f --target-processes application-only --replay-mode application ../cmake-build-release/ltpmg --benchmark "TPCC_PART" --table_size 8 --batch_size 65536 --epoch_tp 3 --deviceIDs "0,1,2,3"