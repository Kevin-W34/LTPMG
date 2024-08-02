cd build

./multi-ltpg --warehouse_size 16 --batch_size 65536 --epoch_tp 50 --neworder_percent  50 --epoch_sync 1 --deviceIDs "6,7"

./multi-ltpg --warehouse_size 16 --batch_size 65536 --epoch_tp 50 --neworder_percent  50 --epoch_sync 1 --deviceIDs "4,5,6,7"
