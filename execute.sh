cd cmake-build-release

#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "unif" --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "5,7"
#
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "5,7"
#
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "unif" --deviceIDs "1"
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "unif" --deviceIDs "1"
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "unif" --deviceIDs "1"
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "unif" --deviceIDs "1"
#
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "1"
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "1"
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "1"
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "1"
#
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "1"
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "1"
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "1"
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "1"

./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "1"
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "1"
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "1"
./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "1"


#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_A" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_B" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_C" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_D" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "unif" --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.1 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 0.5 --deviceIDs "2,4,5,7"
#
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 8192  --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 16384 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 32768 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
#./ltpmg --benchmark "YCSB_E" --table_size 1 --neworder_percent 100 --batch_size 65536 --epoch_tp 10 --data_distribution "zipf" --zipf_config 1 --deviceIDs "2,4,5,7"
