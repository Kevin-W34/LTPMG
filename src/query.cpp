#include "../include/query.h"

Query::Query(std::shared_ptr<Param> param) : warehouse_tbl_size(param->warehouse_size),
                                             neworder_percent(param->neworder_percent),
                                             batch_size(param->batch_size),
                                             epoch_tp(param->epoch_tp),
                                             epoch_sync(param->epoch_sync),
                                             device_IDs_str(param->deviceIDs)
{
    this->district_tbl_size = this->warehouse_tbl_size * 10;
    this->customer_tbl_size = this->warehouse_tbl_size * 10 * 3000;
    this->history_tbl_size = this->warehouse_tbl_size * 10 * 3000;
    this->neworder_tbl_size = this->warehouse_tbl_size * 10 * 3000;
    this->order_tbl_size = this->warehouse_tbl_size * 10 * 3000;
    this->orderline_tbl_size = this->warehouse_tbl_size * 10 * 45000;
    this->stock_tbl_size = this->warehouse_tbl_size * 100000;
    this->item_tbl_size = 100000;

    this->neworderquery_size = this->batch_size * this->neworder_percent / 100;
    this->paymentquery_size = this->batch_size - this->neworderquery_size;

    this->gen_neworderquery_size = this->neworderquery_size * this->epoch_tp; //
    this->gen_paymentquery_size = this->paymentquery_size * this->epoch_tp;   //

    this->neworderquery = new NeworderQuery[this->gen_neworderquery_size];
    this->paymentquery = new PaymentQuery[this->gen_paymentquery_size];

    this->transaction_ID.store(0);
    this->neworder_tbl_ID.store(0);
    this->order_tbl_ID.store(0);
    this->orderline_tbl_ID.store(0);
    this->history_tbl_ID.store(0);

    random = new Random();
    this->analyse_device_IDs();

    this->neworderquery_slice_size = this->neworderquery_size / this->device_cnt;
    this->paymentquery_slice_size = this->paymentquery_size / this->device_cnt;

    LOG(INFO) << "this->neworderquery_slice_size:" << this->neworderquery_slice_size << ",this->paymentquery_slice_size:" << this->paymentquery_slice_size << std::endl;
}

Query::~Query()
{
    delete this->neworderquery;
    delete this->paymentquery;
    delete this->device_IDs;
    delete random;
}

void Query::make_neworder(uint32_t thID, uint32_t core_cnt, uint32_t epoch_ID)
{
    auto count = this->neworderquery_size / core_cnt;
    if (thID < core_cnt)
    {
        for (size_t i = 0; i < count; ++i)
        {
            uint32_t current = thID * count + i + this->neworderquery_size * epoch_ID;
            if (current >= this->gen_neworderquery_size)
            {
                break;
            }

            uint32_t start_W_ID = 0;
            uint32_t end_W_ID = this->warehouse_tbl_size - 1;
            uint32_t slice = (current / this->neworderquery_slice_size) % this->device_cnt;
            start_W_ID = slice * this->warehouse_tbl_size / this->device_cnt;
            end_W_ID = start_W_ID + this->warehouse_tbl_size / this->device_cnt - 1;
            // printf("current:%d,slice:%d,start:%d,end:%d\n", current, slice, start_W_ID, end_W_ID);

            this->neworderquery[current].txn_ID = this->transaction_ID.fetch_add(1);

            // printf("N epoch_ID:%d,current:%d,thID:%d\n", epoch_ID, current, this->neworderquery[current].txn_ID);

            this->neworderquery[current].W_ID = (uint32_t)random->uniform_dist(start_W_ID, end_W_ID); // [0 , WAREHOUSE_SIZE - 1]
            this->neworderquery[current].D_ID = (uint32_t)random->uniform_dist(0, 9);                 // [0 , 9]
            this->neworderquery[current].C_ID = (uint32_t)random->uniform_dist(0, 2999);

            this->neworderquery[current].O_OL_CNT = (uint32_t)random->uniform_dist(5, 15); // [5 , 15]
            // this->neworderquery[current].O_OL_CNT = 15;
            // this->neworderquery[current].O_OL_CNT = 1;

            this->neworderquery[current].N_O_ID = this->neworder_tbl_ID.fetch_add(1) % 30000;                                               // this->neworder_tbl_size;    // this->neworderquery[current].O_OL_CNT
            this->neworderquery[current].O_ID = this->order_tbl_ID.fetch_add(1) % 30000;                                                    // this->order_tbl_size;            // this->neworderquery[current].O_OL_CNT
            this->neworderquery[current].O_OL_ID = this->orderline_tbl_ID.fetch_add(this->neworderquery[current].O_OL_CNT) % (450000 - 15); // this->orderline_tbl_size; // this->neworderquery[current].O_OL_CNT

            for (uint32_t i = 0; i < this->neworderquery[current].O_OL_CNT; i++)
            {
                // this->neworderquery[current].INFO[i].OL_I_ID = (uint32_t)random->non_uniform_distribution(8191, 1, 100000) - 1; //[0 , 99999];
                this->neworderquery[current].INFO[i].OL_I_ID = (uint32_t)random->uniform_dist(0, 99999);
                for (uint32_t k = 0; k < i; k++)
                {
                    while (this->neworderquery[current].INFO[k].OL_I_ID == this->neworderquery[current].INFO[i].OL_I_ID)
                    {
                        // this->neworderquery[current].INFO[i].OL_I_ID = (uint32_t)random->non_uniform_distribution(8191, 1, 100000) - 1;
                        this->neworderquery[current].INFO[i].OL_I_ID = (uint32_t)random->uniform_dist(0, 99999);
                    }
                }
                this->neworderquery[current].INFO[i].OL_SUPPLY_W_ID = this->neworderquery[current].W_ID; // Home Warehouse
                uint32_t isLocal = random->uniform_dist(1, 100);
                if (isLocal <= 15) // Remote Warehouse
                {
                    isLocal = random->uniform_dist(0, this->warehouse_tbl_size - 1);
                    while (isLocal == this->neworderquery[current].W_ID)
                    {
                        isLocal = random->uniform_dist(0, this->warehouse_tbl_size - 1);
                    }
                    this->neworderquery[current].INFO[i].OL_SUPPLY_W_ID = isLocal;
                }
                this->neworderquery[current].INFO[i].OL_QUANTITY = (uint32_t)random->uniform_dist(1, 2);
            }
        }
    }
}

void Query::make_payment(uint32_t thID, uint32_t core_cnt, uint32_t epoch_ID)
{
    auto count = this->paymentquery_size / core_cnt;
    if (thID < core_cnt)
    {
        for (size_t i = 0; i < count; ++i)
        {
            uint32_t current = thID * count + i + this->paymentquery_size * epoch_ID;
            if (current >= this->gen_paymentquery_size)
            {
                break;
            }
            uint32_t start_W_ID = 0;
            uint32_t end_W_ID = this->warehouse_tbl_size - 1;
            uint32_t slice = (current / this->paymentquery_slice_size) % this->device_cnt;
            start_W_ID = slice * this->warehouse_tbl_size / this->device_cnt;
            end_W_ID = start_W_ID + this->warehouse_tbl_size / this->device_cnt - 1;
            // printf("current:%d,slice:%d,start:%d,end:%d\n", current, slice, start_W_ID, end_W_ID);

            this->paymentquery[current].txn_ID = this->transaction_ID.fetch_add(1);
            // printf("P epoch_ID:%d,current:%d,thID:%d\n", epoch_ID, current, this->paymentquery[current].txn_ID);

            this->paymentquery[current].W_ID = random->uniform_dist(start_W_ID, end_W_ID);
            this->paymentquery[current].D_ID = random->uniform_dist(0, 9);
            uint32_t isName = random->uniform_dist(1, 100);
            if (isName < 21) // 21
            {
                this->paymentquery[current].isName = 1;
                this->paymentquery[current].C_ID = 0; // random->non_uniform_distribution(1023, 1, 3000) - 1;
                this->paymentquery[current].C_LAST = (random->uniform_dist(0, 2999) << 4) + random->uniform_dist(0, 14);
                // this->paymentquery[current].C_LAST = (random->uniform_dist(0, 19) << 8) + random->uniform_dist(0, 149);
            } /* C_LAST */
            else
            {
                this->paymentquery[current].isName = 0;
                this->paymentquery[current].C_ID = (uint32_t)random->uniform_dist(0, 2999);
                this->paymentquery[current].C_LAST = 0xffffffff;
                // std::cout << this->paymentquery[current].C_LAST << std::endl;
            } /* C_ID */

            uint32_t isLocal = random->uniform_dist(1, 100);
            if (isLocal <= 95) // 85
            {
                this->paymentquery[current].C_W_ID = this->paymentquery[current].W_ID;
            } /* Local */
            else
            {
                this->paymentquery[current].C_W_ID = random->uniform_dist(0, this->warehouse_tbl_size - 1);
                while (this->paymentquery[current].C_W_ID == this->paymentquery[current].W_ID)
                {
                    this->paymentquery[current].C_W_ID = random->uniform_dist(0, this->warehouse_tbl_size - 1);
                }
            } /* Remote */

            // this->paymentquery[current].C_W_ID = this->paymentquery[current].W_ID;
            this->paymentquery[current].C_D_ID = this->paymentquery[current].D_ID;
            this->paymentquery[current].H_AMOUNT = random->uniform_dist(1, 4);
            this->paymentquery[current].H_ID = this->history_tbl_ID.fetch_add(1) % 30000; // this->history_tbl_size;
        }
    }
}

void Query::make_Query()
{
    long long start_t = current_time();
    std::vector<std::thread> threads;
    uint32_t core_cnt = 64;
    for (uint32_t j = 0; j < this->epoch_tp; j++)
    {
        for (size_t i = 0; i < core_cnt; i++)
        {
            threads.emplace_back(std::thread(&Query::make_neworder, this, i, core_cnt, j));
            threads.emplace_back(std::thread(&Query::make_payment, this, i, core_cnt, j));
        }
        for (size_t i = 0; i < threads.size(); i++)
        {
            threads[i].join();
        }
        this->transaction_ID.store(0);
        this->history_tbl_ID.store(0);
        this->neworder_tbl_ID.store(0);
        this->order_tbl_ID.store(0);
        this->orderline_tbl_ID.store(0);
        threads.clear();
    }
    long long end_t = current_time();
    float cost = duration(start_t, end_t);
    LOG(INFO) << "initialize neworder query, cost:" << this->gen_neworderquery_size << "." << std::endl;
    LOG(INFO) << "initialize payment query, cost:" << this->gen_paymentquery_size << "." << std::endl;
    LOG(INFO) << "initialize query, cost:" << cost << "s." << std::endl;
#ifdef PRINT_QUERY
    this->print_neworder();
    // this->print_payment();
#endif
}

void Query::print_neworder()
{
    for (size_t i = 0; i < this->gen_neworderquery_size; i++)
    {
        std::cout << "neworderquery:" << i << "->" << this->neworderquery[i].W_ID << ",";
        std::cout << this->neworderquery[i].D_ID << "," << this->neworderquery[i].C_ID << ",";
        std::cout << this->neworderquery[i].O_ID << "," << this->neworderquery[i].N_O_ID << ",";
        std::cout << this->neworderquery[i].O_OL_CNT << "," << this->neworderquery[i].O_OL_ID << std::endl;
    }
}

void Query::print_payment()
{
    for (size_t i = 0; i < this->gen_paymentquery_size; i++)
    {
        std::cout << "paymentquery:" << i << "->" << this->paymentquery[i].W_ID << ",";
        std::cout << this->paymentquery[i].D_ID << "," << this->paymentquery[i].C_ID << ",";
        std::cout << this->paymentquery[i].C_LAST << "," << this->paymentquery[i].isName << ",";
        std::cout << this->paymentquery[i].C_D_ID << "," << this->paymentquery[i].C_W_ID << std::endl;
    }
}

NeworderQuery *Query::get_neworder_query()
{
    return this->neworderquery;
}

PaymentQuery *Query::get_payment_query()
{
    return this->paymentquery;
}

uint32_t Query::get_batch_size()
{
    return this->batch_size;
}

uint32_t Query::get_neworder_percent()
{
    return this->neworder_percent;
}

uint32_t Query::get_epoch_tp()
{
    return this->epoch_tp;
}

uint32_t Query::get_epoch_sync()
{
    return this->epoch_sync;
}

uint32_t Query::get_warehouse_size()
{
    return warehouse_tbl_size;
}

uint32_t Query::get_device_cnt()
{
    return this->device_cnt;
}

uint32_t *Query::get_device_IDs()
{
    return this->device_IDs;
}

void Query::analyse_device_IDs()
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