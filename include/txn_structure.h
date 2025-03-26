#pragma once
#ifndef LTPMG_TXN_STRUCTURE
#define LTPMG_TXN_STRUCTURE

#include "define.h"

struct Global_Txn_Exec_Param {
    // target platform
    UINT32 global_txn_info_size; // count of sub txn kinds
    UINT32 global_sub_txn_size;
    UINT32 batch_size;

    UINT32 target_platform; // 1 is GPU
    UINT32 target_GPU;
    UINT32 start;
    UINT32 size;
};

struct Global_Txn_Sub {
    // sub txn
    UINT32 TID = 0;
    UINT32 sub_txn_ID = 0;
    UINT32 ispopular = 0;
    UINT32 type = 'n'; // S:select s:scan
    UINT32 benchmark = 0;
    UINT32 table_ID = 0;
    UINT32 dest_Row_1 = 0;
    UINT32 dest_Row_2 = 0;
    UINT32 data = 0;
    UINT32 dest_device = 0;
    UINT32 cur_mark = 0;
    UINT32 isCommitted = 0;
};

struct Global_Txn_Result {
    // result
    UINT32 TID;
    UINT32 int_attributes[16];
    UINT32 string_attributes[16 * 8];
    // max char in a string is 32, 8bit for one char, 4 char in one UINT32, max UINT32 is 32/4=8
    DOUBLE double_attributes[16];
};

struct Global_Txn_Aux_Struct {
    /*  auxiliary struct bitmap and min_TID
        one row one aux_struct
        bitmap[x] means the bitmap of the xth row
    */
    uint32_t data_packet_cur = 0;
    uint32_t data_packet_size = 0;
    uint32_t bitmap_size;
    uint32_t min_TID_size;
    UINT32 *bitmap;
    UINT32 *bitmap_mark;
    UINT32 *min_TID; // table_size
    UINT32 *TID_cnt; // table_size
};

struct Global_Txn_Info {
    // subtxn info, what a sub txn contains
    UINT32 cur_subtxn_cnt = 0;
    UINT32 select_cnt = 0;
    UINT32 insert_cnt = 0;
    UINT32 update_cnt = 0;
    UINT32 scan_cnt = 0;
    UINT32 delete_cnt = 0;
};

struct Global_Txn {
    // subtxn
    UINT32 global_txn_info_ID = 0;
    UINT32 dest_device = 0;
    Global_Txn_Sub *subtxn; // 按预定义策略拆分事务为无数据竞争和语义竞争的子事务集合, 每个subtxn数组应为串行的子事务集合,max 32
};

struct Global_Txn_Exec {
    UINT32 select_cur = 0;
    UINT32 insert_cur = 0;
    UINT32 update_cur = 0;
    UINT32 scan_cur = 0;
    UINT32 delete_cur = 0;

    UINT32 *select_txn_mark; //31~0, 31:remote or local, 30~5:cur_txn, 4~0:cur_sub_txn
    UINT32 *insert_txn_mark;
    UINT32 *update_txn_mark;
    UINT32 *scan_txn_mark;
    UINT32 *delete_txn_mark;

    UINT32 *mark;
};

struct Global_Data_Packet {
    // data packet for p2p data transfer
    UINT32 mark = 0xffffffff;
};

struct Test_Query {
    UINT32 TID;
    UINT32 Row_0;
    UINT32 Row_1;
    UINT32 Row_2;
    UINT32 Row_3;
    UINT32 Row_4;
    UINT32 Row_5;
};

struct Test_Query_2 {
    UINT32 TID;
    UINT32 Row_0;
    UINT32 Row_1;
    UINT32 Row_2;
    UINT32 Row_3;
    UINT32 Row_4;
};

struct Neworder_Query {
    UINT32 TID;

    UINT32 W_ID;
    UINT32 D_ID;
    UINT32 C_ID;
    UINT32 O_ID;
    UINT32 N_O_ID;
    UINT32 O_OL_CNT;
    UINT32 O_OL_ID;

    struct Info {
        UINT32 OL_I_ID;
        UINT32 OL_SUPPLY_W_ID;
        UINT32 OL_QUANTITY;
    };

    Info INFO[15];
};

struct Payment_Query {
    UINT32 TID;

    UINT32 W_ID;
    UINT32 D_ID;
    UINT32 C_ID;
    UINT32 C_LAST;
    UINT32 isName; // 0,id; 1,name
    UINT32 C_D_ID;
    UINT32 C_W_ID;
    UINT32 H_AMOUNT;
    UINT32 H_ID;
};

struct Orderstatus_Query {
    UINT32 TID;

    UINT32 W_ID;
    UINT32 D_ID;
    UINT32 C_ID;
    UINT32 C_LAST;
    UINT32 O_ID;
    UINT32 OL_ID;
    UINT32 isName; // 0,id; 1,name
};

struct Delivery_Query {
    UINT32 TID;

    UINT32 NO_O_ID[10];
    UINT32 NO_W_ID[10];
    UINT32 NO_D_ID[10];
    UINT32 NO_C_ID[10];
};

struct Stocklevel_Query {
    UINT32 TID;

    // UINT32 query_cnt;
    UINT32 W_ID;
    UINT32 D_ID;
    UINT32 I_ID[10];
    UINT32 O_OL_ID[10];
};

struct YCSB_A_Query {
    UINT32 TID;

    UINT32 ROW_ID[10];
};

struct YCSB_B_Query {
    UINT32 TID;

    UINT32 ROW_ID[10];
};

struct YCSB_C_Query {
    UINT32 TID;

    UINT32 ROW_ID[10];
};

struct YCSB_D_Query {
    UINT32 TID;

    UINT32 ROW_ID[10];
};

struct YCSB_E_Query {
    UINT32 TID;

    UINT32 ROW_ID[10];
};

#endif
