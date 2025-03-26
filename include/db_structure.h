#pragma once
#ifndef LTPMG_DB_STRUCTURE
#define LTPMG_DB_STRUCTURE

#include "define.h"

struct Global_Table_Info
{
    UINT32 int_size = 0;
    UINT32 double_size = 0;
    UINT32 string_size = 0;
    UINT32 string_length = 8; // max char in a string is 32, 8bit for one char, 4 char in one UINT32, max UINT32 is 32/4=8
    UINT32 table_size = 0;
    UINT32 table_cnt = 0;
};

struct Global_Table
{
    INT32 *int_data;
    DOUBLE *double_data;
    UINT32 *string_data;
};

struct Global_Table_Index
{
    UINT32 *index;
};

struct Global_Table_Meta
{
    UINT32 row_start;
    UINT32 row_end;
    UINT32 table_slice_size;
    UINT32 table_size;
    UINT32 bitmap_row_slice_size;
};

struct Global_Table_Strategy
{
    UINT32 *int_target_GPU;
    UINT32 *int_target_GPU_platform;
    UINT32 *string_target_GPU;
    UINT32 *string_target_GPU_platform;
    UINT32 *double_target_GPU;
    UINT32 *double_target_GPU_platform;
};

#endif