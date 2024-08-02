# bash compile.sh

cd nsys

# nsys profile --stats=true --force-overwrite=true --output=16-65536-50-50-1-7 ../build/multi-ltpg --warehouse_size 16 --batch_size 65536 --epoch_tp 50 --neworder_percent 50 --epoch_sync 1 --deviceIDs "7" 

# nsys profile --stats=true --force-overwrite=true --output=16-65536-50-50-1-67 ../build/multi-ltpg --warehouse_size 16 --batch_size 65536 --epoch_tp 50 --neworder_percent 50 --epoch_sync 1 --deviceIDs "6,7" 

# nsys profile --stats=true --force-overwrite=true --output=16-65536-50-50-1-4567 ../build/multi-ltpg --warehouse_size 16 --batch_size 65536 --epoch_tp 50 --neworder_percent 50 --epoch_sync 1 --deviceIDs "4,5,6,7"

# nsys profile --stats=true --force-overwrite=true --output=16-65536-50-100-1-7 ../build/multi-ltpg --warehouse_size 16 --batch_size 65536 --epoch_tp 50 --neworder_percent 100 --epoch_sync 1 --deviceIDs "7" 

# nsys profile --stats=true --force-overwrite=true --output=16-65536-50-100-1-67 ../build/multi-ltpg --warehouse_size 16 --batch_size 65536 --epoch_tp 50 --neworder_percent 100 --epoch_sync 1 --deviceIDs "6,7" 

# nsys profile --stats=true --force-overwrite=true --output=16-65536-50-100-1-4567 ../build/multi-ltpg --warehouse_size 16 --batch_size 65536 --epoch_tp 50 --neworder_percent 100 --epoch_sync 1 --deviceIDs "4,5,6,7"

# nsys profile --stats=true --force-overwrite=true --output=16-65536-50-0-1-7 ../build/multi-ltpg --warehouse_size 16 --batch_size 65536 --epoch_tp 50 --neworder_percent 0 --epoch_sync 1 --deviceIDs "7" 

# nsys profile --stats=true --force-overwrite=true --output=16-65536-50-0-1-67 ../build/multi-ltpg --warehouse_size 16 --batch_size 65536 --epoch_tp 50 --neworder_percent 0 --epoch_sync 1 --deviceIDs "6,7" 

# nsys profile --stats=true --force-overwrite=true --output=16-65536-50-0-1-4567 ../build/multi-ltpg --warehouse_size 16 --batch_size 65536 --epoch_tp 50 --neworder_percent 0 --epoch_sync 1 --deviceIDs "4,5,6,7"


# nsys profile --stats=true --force-overwrite=true --output=16-16384-50-50-1-4567 ../build/multi-ltpg --warehouse_size 16 --batch_size 16384 --epoch_tp 50 --neworder_percent 50 --epoch_sync 1 --deviceIDs "4,5,6,7"

# nsys profile --stats=true --force-overwrite=true --output=16-32768-50-50-1-4567 ../build/multi-ltpg --warehouse_size 16 --batch_size 32768 --epoch_tp 50 --neworder_percent 50 --epoch_sync 1 --deviceIDs "4,5,6,7"

nsys profile --stats=true --force-overwrite=true --output=16-65536-50-50-1-4567 ../build/multi-ltpg --warehouse_size 16 --batch_size 65536 --epoch_tp 50 --neworder_percent 50 --epoch_sync 1 --deviceIDs "4,5,6,7"

nsys profile --stats=true --force-overwrite=true --output=16-131072-50-50-1-4567 ../build/multi-ltpg --warehouse_size 16 --batch_size 131072 --epoch_tp 50 --neworder_percent 50 --epoch_sync 1 --deviceIDs "4,5,6,7"
