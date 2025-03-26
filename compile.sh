rm -rf ./build/*
rm -rf ./log/*
cd build
cmake ..
make -j
# ./ltpmg --benchmark "TEST" --warehouse_size 1024 --batch_size 8 --epoch_tp 10 --neworder_percent 50 --epoch_sync 1 --deviceIDs "6,7" 