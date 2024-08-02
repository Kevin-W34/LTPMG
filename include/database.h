#pragma once

#include "dependency.h"
#include "random.h"
#include "common.h"

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
    char OL_DIST_INF[11];    // 9
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

class Database
{
private:
    WAREHOUSE_ROW *warehouse_tbl;
    DISTRICT_ROW *district_tbl;
    CUSTOMER_ROW *customer_tbl;
    HISTORY_ROW *history_tbl;
    NEWORDER_ROW *neworder_tbl;
    ORDER_ROW *order_tbl;
    ORDERLINE_ROW *orderline_tbl;
    STOCK_ROW *stock_tbl;
    ITEM_ROW *item_tbl;
    uint32_t *customer_name_index;

    uint32_t warehouse_tbl_size;
    uint32_t district_tbl_size;
    uint32_t customer_tbl_size;
    uint32_t history_tbl_size;
    uint32_t neworder_tbl_size;
    uint32_t order_tbl_size;
    uint32_t orderline_tbl_size;
    uint32_t stock_tbl_size;
    uint32_t item_tbl_size;

    Random *random;

    uint32_t device_cnt;
    uint32_t *device_IDs;
    std::string device_IDs_str;

public:
    Database(std::shared_ptr<Param> param);
    ~Database();
    void initialize_warehouse_tbl(uint32_t thID);
    void initialize_district_tbl(uint32_t thID);
    void initialize_customer_tbl(uint32_t thID);
    void initialize_history_tbl(uint32_t thID);
    void initialize_neworder_tbl(uint32_t thID);
    void initialize_order_tbl(uint32_t thID);
    void initialize_orderline_tbl(uint32_t thID);
    void initialize_stock_tbl(uint32_t thID);
    void initialize_item_tbl(uint32_t thID);
    void initialize_tbl();
    void prepare_to_copy_to_gpu();
    uint32_t get_warehouse_size();
    uint32_t *get_device_IDs();
    uint32_t get_device_cnt();
    void analyse_device_IDs();
    WAREHOUSE_ROW *get_warehouse();
    DISTRICT_ROW *get_district();
    CUSTOMER_ROW *get_customer();
    HISTORY_ROW *get_history();
    NEWORDER_ROW *get_neworder();
    ORDER_ROW *get_order();
    ORDERLINE_ROW *get_orderline();
    STOCK_ROW *get_stock();
    ITEM_ROW *get_item();
    uint32_t *get_customer_name_index();

    void print_warehouse();
};

extern void copy_database_to_gpu(WAREHOUSE_ROW *warehouse, DISTRICT_ROW *district,
                                 CUSTOMER_ROW *customer, HISTORY_ROW *history,
                                 NEWORDER_ROW *neworder, ORDER_ROW *order,
                                 ORDERLINE_ROW *orderline, STOCK_ROW *stock,
                                 ITEM_ROW *item, uint32_t *customer_name_index);

extern void copy_database_to_cpu();

extern void initialize_gpudatabase(uint32_t warehouse_tbl_size, uint32_t device_cnt,
                                   uint32_t *device_IDs);

extern void release_gpudatabase();

extern void launchDatabaseKernel();