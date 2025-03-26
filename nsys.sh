# bash compile.sh

cd nsys

nsys profile --stats=true --force-overwrite=true --output=test1GPU ../cmake-build-release/ltpmg --benchmark "YCSB_C" --table_size 8 --batch_size 16384 --epoch_tp 5 --deviceIDs "1"

#nsys profile --stats=true --force-overwrite=true --output=test2GPU ../cmake-build-release/ltpmg --benchmark "TPCC_PART" --table_size 8 --batch_size 65536 --epoch_tp 5 --deviceIDs "1,2"

#nsys profile --stats=true --force-overwrite=true --output=test4GPU ../cmake-build-release/ltpmg --benchmark "TPCC_PART" --table_size 8 --batch_size 65536 --epoch_tp 5 --deviceIDs "0,1,2,3"
