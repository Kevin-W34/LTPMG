#pragma once

#ifndef LTPMG_DEFINE
#define LTPMG_DEFINE

#include <cstdint>
#include <cstring>
#include <iostream>
#include <fstream>
#include <sstream>
#include <tuple>
#include <vector>
#include <thread>
#include <cstdio>
#include <variant>
#include <memory>
#include <unordered_set>
#include <typeinfo>
#include <any>
#include <queue>
#include <functional>
#include <future>
#include <condition_variable>
#include <atomic>
#include <algorithm>
#include <mutex>
#include <cmath>
#include <random>
#include <nlohmann/json.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

typedef int8_t INT8;
typedef int16_t INT16;
typedef int32_t INT32;
typedef int64_t INT64;

typedef uint8_t UINT8;
typedef uint16_t UINT16;
typedef uint32_t UINT32;
typedef uint64_t UINT64;

typedef char CHAR;
typedef u_char UCHAR;

typedef float FLOAT;
typedef double DOUBLE;

typedef std::string STRING;

typedef bool BOOL;

typedef nlohmann::json json;

#endif
