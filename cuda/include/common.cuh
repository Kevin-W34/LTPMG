#pragma once

#include "stdio.h"
#include "stdlib.h"
#include "cuda_runtime.h"
#include "device_launch_parameters.h"
#include "cooperative_groups.h"
#include "nccl.h"

#include <thrust/execution_policy.h>
#include <thrust/device_vector.h>
#include <cub/block/block_reduce.cuh>
#include <cub/cub.cuh>
#include <cuda/atomic>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>
#include <ctime>
#include <memory>
#include <thread>


#ifndef PRINT_PARAM
#define PRINT_PARAM 1
#endif

#ifndef PRINT_DATABASE
// #define PRINT_DATABASE 1
#endif

#ifndef PRINT_QUERY
// #define PRINT_QUERY 1
#endif

#ifndef WARMUP
#define WARMUP 0.1
#endif

#ifndef MAKE_WILLDO
#define MAKE_WILLDO 1
#endif

#ifndef P2P
#define P2P 1
#endif

#ifndef MERGE
#define MERGE 1
#endif

#ifndef EXECUTE
#define EXECUTE 1
#endif

#ifndef EXECUTE_SERIAL
#define EXECUTE_SERIAL 1
#endif

#ifndef TEST_EXECUTE_WAREHOUSE
#define TEST_EXECUTE_WAREHOUSE 1
#endif

#ifndef TEST_EXECUTE_DISTRICT
#define TEST_EXECUTE_DISTRICT 1
#endif

#ifndef TEST_EXECUTE_CUSTOMER
#define TEST_EXECUTE_CUSTOMER 1
#endif

#ifndef TEST_EXECUTE_HISTORY
#define TEST_EXECUTE_HISTORY 1
#endif

#ifndef TEST_EXECUTE_NEWORDER
#define TEST_EXECUTE_NEWORDER 1
#endif

#ifndef TEST_EXECUTE_ORDER
#define TEST_EXECUTE_ORDER 1
#endif

#ifndef TEST_EXECUTE_ORDERLINE
#define TEST_EXECUTE_ORDERLINE 1
#endif

#ifndef TEST_EXECUTE_STOCK
#define TEST_EXECUTE_STOCK 1
#endif

#ifndef TEST_EXECUTE_ITEM
#define TEST_EXECUTE_ITEM 1
#endif

#define CHECK(res)                                                              \
    {                                                                           \
        if (res != cudaSuccess)                                                 \
        {                                                                       \
            printf("Error : %s:%d , ", __FILE__, __LINE__);                     \
            printf("code : %d , reason : %s \n", res, cudaGetErrorString(res)); \
            exit(-1);                                                           \
        }                                                                       \
    }

#define NCCLCHECK(cmd)                                           \
    {                                                            \
        ncclResult_t res = cmd;                                  \
        if (res != ncclSuccess)                                  \
        {                                                        \
            printf("Failed, NCCL error %s:%d '%s'\n",            \
                   __FILE__, __LINE__, ncclGetErrorString(res)); \
            exit(EXIT_FAILURE);                                  \
        }                                                        \
    }

long long current_time();

float duration(long long start_t, long long end_t);

void sleep(float time_t);

struct WAREHOUSE_ROW
{
    uint32_t W_ID;       // 0
    uint32_t W_TAX;      // 1
    uint32_t W_YTD;      // 2
    char W_NAME[11];     // 3
    char W_STREET_1[11]; // 4
    char W_STREET_2[11]; // 5
    char W_CITY[11];     // 6
    char W_STATE[11];    // 7
    char W_ZIP[11];      // 8
};

struct DISTRICT_ROW
{
    uint32_t D_ID;        // 0
    uint32_t D_W_ID;      // 1
    uint32_t D_TAX;       // 2
    uint32_t D_YTD;       // 3
    uint32_t D_NEXT_O_ID; // 4
    char D_NAME[11];      // 5
    char D_STREET_1[11];  // 6
    char D_STREET_2[11];  // 7
    char D_CITY[11];      // 8
    char D_STATE[11];     // 9
    char D_ZIP[11];       // 10
};

struct CUSTOMER_ROW /* 21 */
{
    uint32_t C_ID;           // 0
    uint32_t C_W_ID;         // 1
    uint32_t C_D_ID;         // 2
    uint32_t C_CREDIT_LIM;   // 3
    uint32_t C_DISCOUNT;     // 4
    uint32_t C_BALANCE;      // 5
    uint32_t C_YTD_PAYMENT;  // 6
    uint32_t C_PAYMENT_CNT;  // 7
    uint32_t C_DELIVERY_CNT; // 8
    uint32_t C_DATA;         // 9
    uint32_t C_CREDIT;       // 10
    char C_FIRST[11];        // 11
    char C_MIDDLE[11];       // 12
    char C_LAST[11];         // 13
    char C_STREET_1[11];     // 14
    char C_STREET_2[11];     // 15
    char C_CITY[11];         // 16
    char C_STATE[11];        // 17
    char C_ZIP[11];          // 18
    char C_PHONE[11];        // 19
    char C_SINCE[11];        // 20
};

struct HISTORY_ROW /* 5 */
{
    uint32_t H_C_ID;   // 0
    uint32_t H_C_D_ID; // 1
    uint32_t H_C_W_ID; // 2
    uint32_t H_D_ID;   // 3
    uint32_t H_W_ID;   // 4
};

struct NEWORDER_ROW /* 3 */
{
    uint32_t NO_O_ID; // 0
    uint32_t NO_D_ID; // 1
    uint32_t NO_W_ID; // 2
};

struct ORDER_ROW /* 8 */
{
    uint32_t O_ID;         // 0
    uint32_t O_D_ID;       // 1
    uint32_t O_W_ID;       // 2
    uint32_t O_C_ID;       // 3
    uint32_t O_ENTRY_D;    // 4
    uint32_t O_CARRIER_ID; // 5
    uint32_t O_OL_CNT;     // 6
    uint32_t O_ALL_LOCAL;  // 7
};

struct ORDERLINE_ROW /* 10 */
{
    uint32_t OL_O_ID;        // 0
    uint32_t OL_D_ID;        // 1
    uint32_t OL_W_ID;        // 2
    uint32_t OL_NUMBER;      // 3
    uint32_t OL_I_ID;        // 4
    uint32_t OL_SUPPLY_W_ID; // 5
    uint32_t OL_DELIVERY_D;  // 6
    uint32_t OL_QUANLITY;    // 7
    uint32_t OL_AMOUNT;      // 8
    uint32_t OL_DIST_INF;    // 9
};

struct STOCK_ROW /* 17 */
{
    uint32_t S_I_ID;       // 0
    uint32_t S_W_ID;       // 1
    uint32_t S_QUANTITY;   // 2
    uint32_t S_YTD;        // 3
    uint32_t S_ORDER_CNT;  // 4
    uint32_t S_REMOVE_CNT; // 5
    char S_DIST_01[11];    // 6
    char S_DIST_02[11];    // 7
    char S_DIST_03[11];    // 8
    char S_DIST_04[11];    // 9
    char S_DIST_05[11];    // 10
    char S_DIST_06[11];    // 11
    char S_DIST_07[11];    // 12
    char S_DIST_08[11];    // 13
    char S_DIST_09[11];    // 14
    char S_DIST_10[11];    // 15
    char S_DATA[11];       // 16
};

struct ITEM_ROW /* 5 */
{
    uint32_t I_ID;    // 0
    uint32_t I_IM_ID; // 1
    uint32_t I_PRICE; // 2
    char I_NAME[11];  // 3
    char I_DATA[11];  // 4
};

struct Snapshot
{
    WAREHOUSE_ROW *warehouse;
    DISTRICT_ROW *district;
    CUSTOMER_ROW *customer;
    HISTORY_ROW *history;
    NEWORDER_ROW *neworder;
    ORDER_ROW *order;
    ORDERLINE_ROW *orderline;
    STOCK_ROW *stock;
    ITEM_ROW *item;
    uint32_t *customer_name_index;
};

Snapshot *get_snapshot(uint32_t device_ID);

struct Result
{
    float cost;
    uint32_t epoch;
    uint32_t batch_size;
};
