#include "../include/common.h"

DEFINE_string(benchmark, "YCSB_E", "TPCC_PART(default) or TPCC_ALL or YCSB_A/B/C/D/E or TEST benchmark.");
DEFINE_uint32(table_size, 1,
              "8(default) in TPC-C benchmark, the size of warehouse, table_size*1000000 in ycsb benchmark.");
DEFINE_uint32(batch_size, 8192, "65536(default) TPC-C benchmark, the size of a single Transaction batch.");
DEFINE_uint32(epoch_tp, 10, "10 (default) TPC-C benchmark, the size of execution epoch(default).");
DEFINE_uint32(epoch_sync, 1, "1 (default) TPC-C benchmark, the size of sync epoch(default).");
DEFINE_string(deviceIDs, "7", "'6,7' (default) GPU IDs, split by ','.");
DEFINE_uint32(neworder_percent, 50, "50 (default) The percentage of neworder transaction.");
DEFINE_string(log_path, "~/ltpmg/log", "path of log");
DEFINE_double(zipf_config, 1.0, "zipf config");
DEFINE_string(data_distribution, "unif", "unif(default) or zipf");

void initialize_dependency(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]); // 使用glog之前必须先初始化库，仅需执行一次，括号内为程序名
    FLAGS_alsologtostderr = true; // 是否将日志输出到文件和stderr
    FLAGS_colorlogtostderr = true; // 是否启用不同颜色显示

    gflags::ParseCommandLineFlags(&argc, &argv, true);

    gflags::ShutDownCommandLineFlags();

    FLAGS_log_dir = FLAGS_log_path;

    google::SetLogDestination(google::GLOG_INFO, "../log/INFO_"); // INFO级别的日志都存放到logs目录下且前缀为INFO_
    google::SetLogDestination(google::GLOG_WARNING, "../log/WARNING_"); // WARNING级别的日志都存放到logs目录下且前缀为WARNING_
    google::SetLogDestination(google::GLOG_ERROR, "../log/ERROR_"); // ERROR级别的日志都存放到logs目录下且前缀为ERROR_
    google::SetLogDestination(google::GLOG_FATAL, "../log/FATAL_"); // FATAL级别的日志都存放到logs目录下且前缀为FATAL_

    // LOG(INFO) << "benchmark: " << FLAGS_benchmark;
    // LOG(INFO) << "warehouse_size: " << FLAGS_warehouse_size;
    // LOG(INFO) << "batch_size: " << FLAGS_batch_size;
    // LOG(INFO) << "epoch_tp: " << FLAGS_epoch_tp;
    // LOG(INFO) << "epoch_sync: " << FLAGS_epoch_sync;
    // LOG(INFO) << "deviceIDs: " << FLAGS_deviceIDs;
    // LOG(INFO) << "neworder_percent: " << FLAGS_neworder_percent;

    // LOG(INFO) << "info";
    // LOG(WARNING) << "warning";
    // LOG(ERROR) << "error";
    // // LOG(FATAL) << "fatal.";
}

void free_dependency() {
    google::ShutdownGoogleLogging(); // 当要结束glog时必须关闭库，否则会内存溢出
}

long long current_time() {
    // get current time
    timespec time;
    clock_gettime(CLOCK_MONOTONIC, &time);
    long long time_t = time.tv_sec * 1000000 + time.tv_nsec / 1000;
    return time_t;
}

float duration(long long start_t, long long end_t) {
    // Computational time
    float time = ((float) (end_t - start_t)) / 1000000.0;
    return time;
}

void sleep(float time_t) {
    std::cout << "Sleep:" << time_t << "s." << std::endl;
    long long start_t = current_time();
    while (true) {
        long long end_t = current_time();
        float cost = duration(start_t, end_t);
        if (time_t <= cost) {
            break;
        }
    }
}
