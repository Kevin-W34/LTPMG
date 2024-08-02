#include "../include/database.h"

Database::Database(std::shared_ptr<Param> param) : warehouse_tbl_size(param->warehouse_size)
{
    this->district_tbl_size = this->warehouse_tbl_size * 10;
    this->customer_tbl_size = this->warehouse_tbl_size * 10 * 3000;
    this->history_tbl_size = this->warehouse_tbl_size * 10 * 3000;
    this->neworder_tbl_size = this->warehouse_tbl_size * 10 * 3000;
    this->order_tbl_size = this->warehouse_tbl_size * 10 * 3000;
    this->orderline_tbl_size = this->warehouse_tbl_size * 10 * 45000;
    this->stock_tbl_size = this->warehouse_tbl_size * 100000;
    this->item_tbl_size = 100000;
    this->device_IDs_str = param->deviceIDs;

    this->warehouse_tbl = new WAREHOUSE_ROW[warehouse_tbl_size];
    this->district_tbl = new DISTRICT_ROW[district_tbl_size];
    this->customer_tbl = new CUSTOMER_ROW[customer_tbl_size];
    this->history_tbl = new HISTORY_ROW[history_tbl_size];
    this->neworder_tbl = new NEWORDER_ROW[neworder_tbl_size];
    this->order_tbl = new ORDER_ROW[order_tbl_size];
    this->orderline_tbl = new ORDERLINE_ROW[orderline_tbl_size];
    this->stock_tbl = new STOCK_ROW[stock_tbl_size];
    this->item_tbl = new ITEM_ROW[item_tbl_size];
    this->customer_name_index = new uint32_t[customer_tbl_size];

    random = new Random();

    this->analyse_device_IDs();
    uint32_t row_cnt = 0;
    row_cnt += this->warehouse_tbl_size;
    row_cnt += this->district_tbl_size;
    row_cnt += this->customer_tbl_size;
    row_cnt += this->history_tbl_size;
    row_cnt += this->neworder_tbl_size;
    row_cnt += this->order_tbl_size;
    row_cnt += this->stock_tbl_size;
    row_cnt += this->warehouse_tbl_size;
    row_cnt += this->item_tbl_size;
    LOG(INFO) << "row_cnt:" << row_cnt << std::endl;
}

Database::~Database()
{
    delete warehouse_tbl;
    delete district_tbl;
    delete customer_tbl;
    delete history_tbl;
    delete neworder_tbl;
    delete order_tbl;
    delete orderline_tbl;
    delete stock_tbl;
    delete item_tbl;
    delete customer_name_index;

    delete device_IDs;
    delete random;
}

void Database::initialize_warehouse_tbl(uint32_t thID)
{
    uint32_t core_cnt = 1; // std::thread::hardware_concurrency()
    auto count = warehouse_tbl_size / core_cnt;
    if (thID < core_cnt)
    {
        for (size_t i = 0; i < count; i++)
        {
            uint32_t current = thID * count + i;
            warehouse_tbl[current].W_ID = current;
            warehouse_tbl[current].W_TAX = 1;
            warehouse_tbl[current].W_YTD = 1;
            strcpy(warehouse_tbl[current].W_NAME, "W_NAME");
            strcpy(warehouse_tbl[current].W_STREET_1, "No.1W Road");
            strcpy(warehouse_tbl[current].W_STREET_2, "No.2W Road");
            strcpy(warehouse_tbl[current].W_CITY, "New York");
            strcpy(warehouse_tbl[current].W_STATE, "New York");
            strcpy(warehouse_tbl[current].W_ZIP, "110011");
        }
    }
}

void Database::initialize_district_tbl(uint32_t thID)
{
    uint32_t core_cnt = 10; // std::thread::hardware_concurrency()
    auto count = district_tbl_size / core_cnt;
    if (thID < core_cnt)
    {
        for (size_t i = 0; i < count; i++)
        {
            uint32_t current = thID * count + i;
            district_tbl[current].D_ID = current % 10;
            district_tbl[current].D_W_ID = current / 10;
            district_tbl[current].D_TAX = 1;
            district_tbl[current].D_YTD = 1;
            district_tbl[current].D_NEXT_O_ID = 0;
            strcpy(district_tbl[current].D_NAME, "D_NAME");
            strcpy(district_tbl[current].D_STREET_1, "No.1D Road");
            strcpy(district_tbl[current].D_STREET_2, "No.2D Road");
            strcpy(district_tbl[current].D_CITY, "Los Angles");
            strcpy(district_tbl[current].D_STATE, "California");
            strcpy(district_tbl[current].D_ZIP, "110011-1");
        }
    }
}

void Database::initialize_customer_tbl(uint32_t thID)
{
    uint32_t core_cnt = 60; // std::thread::hardware_concurrency();
    auto count = customer_tbl_size / core_cnt;
    if (thID < core_cnt)
    {
        for (size_t i = 0; i < count; i++)
        {
            uint32_t current = thID * count + i;
            customer_tbl[current].C_ID = current % 3000;
            customer_tbl[current].C_W_ID = current / 30000;
            customer_tbl[current].C_D_ID = (current / 3000) % 10;
            customer_tbl[current].C_CREDIT_LIM = 100000;
            customer_tbl[current].C_DISCOUNT = 100000;
            customer_tbl[current].C_BALANCE = 100000;
            customer_tbl[current].C_YTD_PAYMENT = 100000;
            customer_tbl[current].C_PAYMENT_CNT = 100000;
            customer_tbl[current].C_DELIVERY_CNT = 100000;
            customer_tbl[current].C_DATA = 100000;
            customer_tbl[current].C_CREDIT = random->uniform_dist(0, 1); // 0 bad; 1 good
            strcpy(customer_tbl[current].C_FIRST, "C_FIRST");
            strcpy(customer_tbl[current].C_MIDDLE, "C_MIDDLE");
            strcpy(customer_tbl[current].C_LAST, random->rand_C_LAST().c_str());
            strcpy(customer_tbl[current].C_STREET_1, "C_STREET_1");
            strcpy(customer_tbl[current].C_STREET_2, "C_STREET_2");
            strcpy(customer_tbl[current].C_CITY, "C_CITY");
            strcpy(customer_tbl[current].C_STATE, "C_STATE");
            strcpy(customer_tbl[current].C_ZIP, "C_STATE");
            strcpy(customer_tbl[current].C_PHONE, "C_PHONE");
            strcpy(customer_tbl[current].C_SINCE, "C_SINCE");
            if (customer_tbl[current].C_LAST == "AAA")
            {
                customer_name_index[current] = 0;
            }
            else if (customer_tbl[current].C_LAST == "BBB")
            {
                customer_name_index[current] = 1;
            }
            else if (customer_tbl[current].C_LAST == "CCC")
            {
                customer_name_index[current] = 2;
            }
            else if (customer_tbl[current].C_LAST == "DDD")
            {
                customer_name_index[current] = 3;
            }
            else if (customer_tbl[current].C_LAST == "EEE")
            {
                customer_name_index[current] = 4;
            }
            else if (customer_tbl[current].C_LAST == "FFF")
            {
                customer_name_index[current] = 5;
            }
            else if (customer_tbl[current].C_LAST == "GGG")
            {
                customer_name_index[current] = 6;
            }
            else if (customer_tbl[current].C_LAST == "HHH")
            {
                customer_name_index[current] = 7;
            }
            else if (customer_tbl[current].C_LAST == "III")
            {
                customer_name_index[current] = 8;
            }
            else if (customer_tbl[current].C_LAST == "JJJ")
            {
                customer_name_index[current] = 9;
            }
            else if (customer_tbl[current].C_LAST == "KKK")
            {
                customer_name_index[current] = 10;
            }
            else if (customer_tbl[current].C_LAST == "LLL")
            {
                customer_name_index[current] = 11;
            }
            else if (customer_tbl[current].C_LAST == "MMM")
            {
                customer_name_index[current] = 12;
            }
            else if (customer_tbl[current].C_LAST == "NNN")
            {
                customer_name_index[current] = 13;
            }
            else if (customer_tbl[current].C_LAST == "OOO")
            {
                customer_name_index[current] = 14;
            }
        }
    }
}

void Database::initialize_history_tbl(uint32_t thID)
{
}

void Database::initialize_neworder_tbl(uint32_t thID)
{
}

void Database::initialize_order_tbl(uint32_t thID)
{
    uint32_t core_cnt = 100; // std::thread::hardware_concurrency();
    auto count = order_tbl_size / core_cnt;
    if (thID < core_cnt)
    {
        for (size_t i = 0; i < count; i++)
        {
            uint32_t current = thID * count + i;
            order_tbl[current].O_ID = current % 30000;
            order_tbl[current].O_D_ID = current / 3000;
            order_tbl[current].O_W_ID = current / 30000;
            order_tbl[current].O_C_ID = random->uniform_dist(0, 2999);
            order_tbl[current].O_ENTRY_D = 0;
            order_tbl[current].O_CARRIER_ID = 0;
            order_tbl[current].O_OL_CNT = 0;
            order_tbl[current].O_ALL_LOCAL = 0;
        }
    }
}

void Database::initialize_orderline_tbl(uint32_t thID)
{
    uint32_t core_cnt = 100; // std::thread::hardware_concurrency();
    auto count = orderline_tbl_size / core_cnt;
    if (thID < core_cnt)
    {
        for (size_t i = 0; i < count; i++)
        {
            uint32_t current = thID * count + i;
            orderline_tbl[current].OL_O_ID = current % 450000;
            orderline_tbl[current].OL_D_ID = current / 45000;
            orderline_tbl[current].OL_W_ID = current / 450000;
            orderline_tbl[current].OL_NUMBER = 0;
            orderline_tbl[current].OL_I_ID = random->uniform_dist(0, 99999);
            orderline_tbl[current].OL_SUPPLY_W_ID = orderline_tbl[current].OL_W_ID;
            orderline_tbl[current].OL_DELIVERY_D = 0;
            orderline_tbl[current].OL_QUANLITY = 0;
            orderline_tbl[current].OL_AMOUNT = 0;
            strcpy(orderline_tbl[current].OL_DIST_INF, "000000");
        }
    }
}

void Database::initialize_stock_tbl(uint32_t thID)
{
    uint32_t core_cnt = 100; // std::thread::hardware_concurrency();
    auto count = stock_tbl_size / core_cnt;
    if (thID < core_cnt)
    {
        for (size_t i = 0; i < count; i++)
        {
            uint32_t current = thID * count + i;
            stock_tbl[current].S_I_ID = current % 100000;
            stock_tbl[current].S_W_ID = current / 100000;
            stock_tbl[current].S_QUANTITY = 100000;
            stock_tbl[current].S_YTD = 100000;
            stock_tbl[current].S_ORDER_CNT = 0;
            stock_tbl[current].S_REMOVE_CNT = 0;
            strcpy(stock_tbl[current].S_DIST_01, "S_DIST_01");
            strcpy(stock_tbl[current].S_DIST_02, "S_DIST_02");
            strcpy(stock_tbl[current].S_DIST_03, "S_DIST_03");
            strcpy(stock_tbl[current].S_DIST_04, "S_DIST_04");
            strcpy(stock_tbl[current].S_DIST_05, "S_DIST_05");
            strcpy(stock_tbl[current].S_DIST_06, "S_DIST_06");
            strcpy(stock_tbl[current].S_DIST_07, "S_DIST_07");
            strcpy(stock_tbl[current].S_DIST_08, "S_DIST_08");
            strcpy(stock_tbl[current].S_DIST_09, "S_DIST_09");
            strcpy(stock_tbl[current].S_DIST_10, "S_DIST_10");
            strcpy(stock_tbl[current].S_DATA, "S_DATA");
        }
    }
}

void Database::initialize_item_tbl(uint32_t thID)
{
    uint32_t core_cnt = 100; // std::thread::hardware_concurrency();
    auto count = item_tbl_size / core_cnt;
    if (thID < core_cnt)
    {
        for (size_t i = 0; i < count; i++)
        {
            uint32_t current = thID * count + i;
            item_tbl[current].I_ID = current;
            item_tbl[current].I_IM_ID = 1000;
            item_tbl[current].I_PRICE = random->uniform_dist(1, 40000);
            strcpy(item_tbl[current].I_DATA, "I_DATA");
            strcpy(item_tbl[current].I_NAME, "I_NAME");
        }
    }
}

void Database::initialize_tbl()
{
    long long start_t = current_time();
    std::vector<std::thread> threads;
    uint32_t core_cnt = 100; // std::thread::hardware_concurrency();
    for (size_t i = 0; i < core_cnt; i++)
    {
        threads.emplace_back(std::thread(&Database::initialize_warehouse_tbl, this, i));
        threads.emplace_back(std::thread(&Database::initialize_district_tbl, this, i));
        threads.emplace_back(std::thread(&Database::initialize_customer_tbl, this, i));
        threads.emplace_back(std::thread(&Database::initialize_history_tbl, this, i));
        threads.emplace_back(std::thread(&Database::initialize_neworder_tbl, this, i));
        threads.emplace_back(std::thread(&Database::initialize_order_tbl, this, i));
        threads.emplace_back(std::thread(&Database::initialize_orderline_tbl, this, i));
        threads.emplace_back(std::thread(&Database::initialize_stock_tbl, this, i));
        threads.emplace_back(std::thread(&Database::initialize_item_tbl, this, i));
    }
    for (size_t i = 0; i < threads.size(); i++)
    {
        threads[i].join();
    }
    long long end_t = current_time();
    float cost = duration(start_t, end_t);
    LOG(INFO) << "initialize database table, cost " << cost << "s." << std::endl;
#ifdef PRINT_DATABASE
    this->print_warehouse();
#endif
}

void Database::prepare_to_copy_to_gpu()
{
}

uint32_t Database::get_warehouse_size()
{
    return warehouse_tbl_size;
}

uint32_t Database::get_device_cnt()
{
    return this->device_cnt;
}

uint32_t *Database::get_device_IDs()
{
    return this->device_IDs;
}

void Database::analyse_device_IDs()
{
    long long start_t = current_time();
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(this->device_IDs_str);
    while (std::getline(tokenStream, token, ','))
    {
        tokens.push_back(token);
    }
    this->device_cnt = tokens.size();
    this->device_IDs = new uint32_t[this->device_cnt];
    for (size_t i = 0; i < this->device_cnt; i++)
    {
        std::string token = tokens[i];
        this->device_IDs[i] = atoi(token.c_str());
    }
    long long end_t = current_time();
    float cost = duration(start_t, end_t);
    LOG(INFO) << "analyse device IDs, costs " << cost << "s." << std::endl;
}

WAREHOUSE_ROW *Database::get_warehouse()
{
    return this->warehouse_tbl;
}

DISTRICT_ROW *Database::get_district()
{
    return this->district_tbl;
}

CUSTOMER_ROW *Database::get_customer()
{
    return this->customer_tbl;
}

HISTORY_ROW *Database::get_history()
{
    return this->history_tbl;
}

NEWORDER_ROW *Database::get_neworder()
{
    return this->neworder_tbl;
}

ORDER_ROW *Database::get_order()
{
    return this->order_tbl;
}

ORDERLINE_ROW *Database::get_orderline()
{
    return this->orderline_tbl;
}

STOCK_ROW *Database::get_stock()
{
    return this->stock_tbl;
}

ITEM_ROW *Database::get_item()
{
    return this->item_tbl;
}

uint32_t *Database::get_customer_name_index()
{
    return this->customer_name_index;
}

void Database::print_warehouse()
{
    for (size_t i = 0; i < warehouse_tbl_size; i++)
    {
        std::cout << "warehouse_tbl:" << this->warehouse_tbl[i].W_ID << "->";
        std::cout << this->warehouse_tbl[i].W_TAX << "," << this->warehouse_tbl[i].W_YTD << ",";
        std::cout << this->warehouse_tbl[i].W_NAME << "," << this->warehouse_tbl[i].W_STREET_1 << ",";
        std::cout << this->warehouse_tbl[i].W_STREET_2 << "," << this->warehouse_tbl[i].W_CITY << ",";
        std::cout << this->warehouse_tbl[i].W_STATE << "," << this->warehouse_tbl[i].W_ZIP << std::endl;
    }
}