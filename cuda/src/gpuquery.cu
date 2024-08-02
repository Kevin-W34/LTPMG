#include "../include/gpuquery.cuh"

void initialize_gpuquery(uint32_t warehouse_tbl_size, uint32_t device_cnt,
                         uint32_t *device_IDs, uint32_t batch_size,
                         uint32_t neworder_percent, uint32_t epoch_tp,
                         uint32_t epoch_sync)
{
    long long start_cpy = current_time();
    gpuquery = new GPUquery(warehouse_tbl_size, device_cnt,
                            device_IDs, batch_size,
                            neworder_percent, epoch_tp,
                            epoch_sync);
    long long end_cpy = current_time();
    float cost_cpy = duration(start_cpy, end_cpy);
    std::cout << "initialize GPU query, cost:" << cost_cpy << "s." << std::endl;
}

void release_gpuquery()
{
    long long start_cpy = current_time();

    // release query,
    gpuquery->release_result();
    gpuquery->release_query();
    gpuquery->release_param();
    gpuquery->release_willdotable();

    delete gpuquery;
    long long end_cpy = current_time();
    float cost_cpy = duration(start_cpy, end_cpy);
    std::cout << "release GPU query, cost:" << cost_cpy << "s." << std::endl;
}

void copy_query_to_gpu(NeworderQuery *neworderquery, PaymentQuery *paymentquery)
{
    long long start_cpy = current_time();
    gpuquery->copy_query_to_gpu<NeworderQuery>(neworderquery, gpuquery->get_neworderquery(), gpuquery->get_neworderquery_size());
    gpuquery->copy_query_to_gpu<PaymentQuery>(paymentquery, gpuquery->get_paymentquery(), gpuquery->get_paymentquery_size());
    // gpuquery->copy_query_to_gpu(neworderquery, gpuquery->get_neworderquery(), gpuquery->get_neworderquery_size());
    // gpuquery->copy_query_to_gpu(paymentquery, gpuquery->get_paymentquery(), gpuquery->get_paymentquery_size());

    // initialize query, cudaMalloc
    gpuquery->initialize_result();
    gpuquery->initialize_query();
    gpuquery->initialize_param();
    gpuquery->initialize_willdotable();

    long long end_cpy = current_time();
    float cost_cpy = duration(start_cpy, end_cpy);
    std::cout << "copy query to gpu, cost:" << cost_cpy << "s." << std::endl;
#ifdef PRINT_QUERY
    gpuquery->print_neworder();
#endif
}

void copy_query_to_cpu()
{
    long long start_cpy = current_time();

    long long end_cpy = current_time();
    float cost_cpy = duration(start_cpy, end_cpy);
    std::cout << "copy query to cpu, cost:" << cost_cpy << "s." << std::endl;
}

GPUquery::GPUquery(uint32_t warehouse_tbl_size, uint32_t device_cnt,
                   uint32_t *device_IDs, uint32_t batch_size,
                   uint32_t neworder_percent, uint32_t epoch_tp,
                   uint32_t epoch_sync)
{
    this->warehouse_tbl_size = warehouse_tbl_size;
    this->district_tbl_size = this->warehouse_tbl_size * 10;
    this->customer_tbl_size = this->warehouse_tbl_size * 10 * 3000;
    this->history_tbl_size = this->warehouse_tbl_size * 10 * 3000;
    this->neworder_tbl_size = this->warehouse_tbl_size * 10 * 3000;
    this->order_tbl_size = this->warehouse_tbl_size * 10 * 3000;
    this->orderline_tbl_size = this->warehouse_tbl_size * 10 * 45000;
    this->stock_tbl_size = this->warehouse_tbl_size * 100000;
    this->item_tbl_size = 100000;

    this->device_cnt = device_cnt;
    this->device_IDs = new uint32_t[this->device_cnt];

    this->batch_size = batch_size;
    this->neworder_percent = neworder_percent;
    this->epoch_tp = epoch_tp;
    this->epoch_sync = epoch_sync;

    this->neworderquery_size = this->batch_size * this->neworder_percent / 100;
    this->paymentquery_size = this->batch_size - this->neworderquery_size;
    this->neworderquery_slice_size = this->neworderquery_size / this->device_cnt;
    this->paymentquery_slice_size = this->paymentquery_size / this->device_cnt;
    this->query_slice_size = this->neworderquery_size + this->paymentquery_size;

    // std::cout << "batch size:" << this->batch_size << std::endl;
    // std::cout << "query_slice_size:" << this->query_slice_size << std::endl;

    this->gen_neworderquery_size = this->neworderquery_size * this->epoch_tp;
    this->gen_paymentquery_size = this->paymentquery_size * this->epoch_tp;

    this->buffer_size = this->neworderquery_slice_size * 3 * this->epoch_sync + this->paymentquery_slice_size / 8 * this->epoch_sync;
    std::cout << "this->buffer_size:" << this->buffer_size << std::endl;

    for (auto i = 0; i < this->device_cnt; i++)
    {
        this->device_IDs[i] = device_IDs[i];
        // std::cout << this->device_IDs[i] << " ";
    }
    // std::cout << std::endl;
    CHECK(cudaSetDevice(this->device_IDs[0]));
    CHECK(cudaMallocHost((void **)&this->neworderquery, sizeof(NeworderQuery) * this->gen_neworderquery_size));
    CHECK(cudaMallocHost((void **)&this->paymentquery, sizeof(PaymentQuery) * this->gen_paymentquery_size));
    print_malloc_size();
}

GPUquery::~GPUquery()
{
    delete this->device_IDs;
    CHECK(cudaFreeHost(this->neworderquery));
    CHECK(cudaFreeHost(this->paymentquery));
}

template <typename Query>
void GPUquery::copy_query_to_gpu(Query *query_c, Query *query_g,
                                 uint32_t size_of_q)
{
    CHECK(cudaMemcpy(query_g, query_c, sizeof(Query) * size_of_q, cudaMemcpyHostToHost));
}

NeworderQuery *GPUquery::get_neworderquery()
{
    return this->neworderquery;
}

uint32_t GPUquery::get_neworderquery_size()
{
    return this->gen_neworderquery_size;
}

PaymentQuery *GPUquery::get_paymentquery()
{
    return this->paymentquery;
}

uint32_t GPUquery::get_paymentquery_size()
{
    return this->gen_paymentquery_size;
}

uint32_t GPUquery::get_epoch_tp()
{
    return this->epoch_tp;
}

uint32_t GPUquery::get_epoch_sync()
{
    return this->epoch_sync;
}

uint32_t GPUquery::get_buffer_size()
{
    return this->buffer_size;
}

uint32_t GPUquery::get_batch_size()
{
    return this->batch_size;
}

void GPUquery::print_neworder()
{
    for (uint32_t i = 0; i < this->gen_neworderquery_size; i++)
    {
        std::cout << "neworderquery:" << i << "->" << this->neworderquery[i].W_ID << ",";
        std::cout << this->neworderquery[i].D_ID << "," << this->neworderquery[i].C_ID << ",";
        std::cout << this->neworderquery[i].O_ID << "," << this->neworderquery[i].N_O_ID << ",";
        std::cout << this->neworderquery[i].O_OL_CNT << "," << this->neworderquery[i].O_OL_ID << std::endl;
    }
}

void GPUquery::print_payment()
{
    for (uint32_t i = 0; i < this->gen_paymentquery_size; i++)
    {
        std::cout << "paymentquery:" << i << "->" << this->paymentquery[i].W_ID << ",";
        std::cout << this->paymentquery[i].D_ID << "," << this->paymentquery[i].C_ID << ",";
        std::cout << this->paymentquery[i].C_LAST << "," << this->paymentquery[i].isName << ",";
        std::cout << this->paymentquery[i].C_D_ID << "," << this->paymentquery[i].C_W_ID << std::endl;
    }
}

void GPUquery::malloc_result(NeworderQueryResult *&neworderqueryresult_c, NeworderQueryResult *&neworderqueryresult_g,
                             PaymentQueryResult *&paymentqueryresult_c, PaymentQueryResult *&paymentqueryresult_g,
                             uint32_t device_ID)
{
    bool isExisted = false;
    for (uint32_t i = 0; i < this->device_cnt; i++)
    {
        if (device_ID == this->device_IDs[i])
        {
            isExisted = true;
        }
    }
    if (isExisted)
    {
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMallocHost((void **)&neworderqueryresult_c, sizeof(NeworderQueryResult) * this->query_slice_size));
        CHECK(cudaMallocHost((void **)&paymentqueryresult_c, sizeof(PaymentQueryResult) * this->query_slice_size));
        CHECK(cudaMalloc((void **)&neworderqueryresult_g, sizeof(NeworderQueryResult) * this->query_slice_size));
        CHECK(cudaMalloc((void **)&paymentqueryresult_g, sizeof(PaymentQueryResult) * this->query_slice_size));
        CHECK(cudaMemset(neworderqueryresult_g, 0, sizeof(NeworderQueryResult) * this->query_slice_size));
        CHECK(cudaMemset(paymentqueryresult_g, 0, sizeof(PaymentQueryResult) * this->query_slice_size));
    }
}

void GPUquery::initialize_result()
{
    this->malloc_result(this->neworderqueryresult_0, this->neworderqueryresult_d_0, this->paymentqueryresult_0, this->paymentqueryresult_d_0, 0);
    this->malloc_result(this->neworderqueryresult_1, this->neworderqueryresult_d_1, this->paymentqueryresult_1, this->paymentqueryresult_d_1, 1);
    this->malloc_result(this->neworderqueryresult_2, this->neworderqueryresult_d_2, this->paymentqueryresult_2, this->paymentqueryresult_d_2, 2);
    this->malloc_result(this->neworderqueryresult_3, this->neworderqueryresult_d_3, this->paymentqueryresult_3, this->paymentqueryresult_d_3, 3);
    this->malloc_result(this->neworderqueryresult_4, this->neworderqueryresult_d_4, this->paymentqueryresult_4, this->paymentqueryresult_d_4, 4);
    this->malloc_result(this->neworderqueryresult_5, this->neworderqueryresult_d_5, this->paymentqueryresult_5, this->paymentqueryresult_d_5, 5);
    this->malloc_result(this->neworderqueryresult_6, this->neworderqueryresult_d_6, this->paymentqueryresult_6, this->paymentqueryresult_d_6, 6);
    this->malloc_result(this->neworderqueryresult_7, this->neworderqueryresult_d_7, this->paymentqueryresult_7, this->paymentqueryresult_d_7, 7);
}

void GPUquery::free_result(NeworderQueryResult *&neworderqueryresult_c, NeworderQueryResult *&neworderqueryresult_g,
                           PaymentQueryResult *&paymentqueryresult_c, PaymentQueryResult *&paymentqueryresult_g,
                           uint32_t device_ID)
{
    bool isExisted = false;
    for (uint32_t i = 0; i < this->device_cnt; i++)
    {
        if (device_ID == this->device_IDs[i])
        {
            isExisted = true;
            break;
        }
    }
    if (isExisted)
    {
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaFreeHost(neworderqueryresult_c));
        CHECK(cudaFreeHost(paymentqueryresult_c));
        CHECK(cudaFree(neworderqueryresult_g));
        CHECK(cudaFree(paymentqueryresult_g));
    }
}

void GPUquery::release_result()
{
    this->free_result(this->neworderqueryresult_0, this->neworderqueryresult_d_0, this->paymentqueryresult_0, this->paymentqueryresult_d_0, 0);
    this->free_result(this->neworderqueryresult_1, this->neworderqueryresult_d_1, this->paymentqueryresult_1, this->paymentqueryresult_d_1, 1);
    this->free_result(this->neworderqueryresult_2, this->neworderqueryresult_d_2, this->paymentqueryresult_2, this->paymentqueryresult_d_2, 2);
    this->free_result(this->neworderqueryresult_3, this->neworderqueryresult_d_3, this->paymentqueryresult_3, this->paymentqueryresult_d_3, 3);
    this->free_result(this->neworderqueryresult_4, this->neworderqueryresult_d_4, this->paymentqueryresult_4, this->paymentqueryresult_d_4, 4);
    this->free_result(this->neworderqueryresult_5, this->neworderqueryresult_d_5, this->paymentqueryresult_5, this->paymentqueryresult_d_5, 5);
    this->free_result(this->neworderqueryresult_6, this->neworderqueryresult_d_6, this->paymentqueryresult_6, this->paymentqueryresult_d_6, 6);
    this->free_result(this->neworderqueryresult_7, this->neworderqueryresult_d_7, this->paymentqueryresult_7, this->paymentqueryresult_d_7, 7);
}

void GPUquery::copy_result(uint32_t device_ID, cudaStream_t *stream, uint32_t stream_ID)
{
    switch (device_ID)
    {
    case 0:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemcpyAsync(this->neworderqueryresult_0, this->neworderqueryresult_d_0, sizeof(NeworderQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
        CHECK(cudaMemcpyAsync(this->paymentqueryresult_0, this->paymentqueryresult_d_0, sizeof(PaymentQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
    case 1:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemcpyAsync(this->neworderqueryresult_1, this->neworderqueryresult_d_1, sizeof(NeworderQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
        CHECK(cudaMemcpyAsync(this->paymentqueryresult_1, this->paymentqueryresult_d_1, sizeof(PaymentQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
    case 2:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemcpyAsync(this->neworderqueryresult_2, this->neworderqueryresult_d_2, sizeof(NeworderQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
        CHECK(cudaMemcpyAsync(this->paymentqueryresult_2, this->paymentqueryresult_d_2, sizeof(PaymentQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
    case 3:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemcpyAsync(this->neworderqueryresult_3, this->neworderqueryresult_d_3, sizeof(NeworderQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
        CHECK(cudaMemcpyAsync(this->paymentqueryresult_3, this->paymentqueryresult_d_3, sizeof(PaymentQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
    case 4:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemcpyAsync(this->neworderqueryresult_4, this->neworderqueryresult_d_4, sizeof(NeworderQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
        CHECK(cudaMemcpyAsync(this->paymentqueryresult_4, this->paymentqueryresult_d_4, sizeof(PaymentQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
    case 5:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemcpyAsync(this->neworderqueryresult_5, this->neworderqueryresult_d_5, sizeof(NeworderQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
        CHECK(cudaMemcpyAsync(this->paymentqueryresult_5, this->paymentqueryresult_d_5, sizeof(PaymentQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
    case 6:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemcpyAsync(this->neworderqueryresult_6, this->neworderqueryresult_d_6, sizeof(NeworderQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
        CHECK(cudaMemcpyAsync(this->paymentqueryresult_6, this->paymentqueryresult_d_6, sizeof(PaymentQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
    case 7:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemcpyAsync(this->neworderqueryresult_7, this->neworderqueryresult_d_7, sizeof(NeworderQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
        CHECK(cudaMemcpyAsync(this->paymentqueryresult_7, this->paymentqueryresult_d_7, sizeof(PaymentQueryResult) * this->query_slice_size, cudaMemcpyDeviceToHost, stream[stream_ID]));
    default:
        return;
    }
}

void GPUquery::reset_result(uint32_t device_ID, cudaStream_t *stream, uint32_t stream_ID)
{
    // uint32_t location = 0;
    // for (uint32_t i = 0; i < this->device_cnt; i++)
    // {
    //     if (device_ID == this->device_IDs[i])
    //     {
    //         isExisted = true;
    //         location = i;
    //     }
    // }
    switch (device_ID)
    {
    case 0:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemsetAsync(this->neworderqueryresult_d_0, 0, sizeof(NeworderQueryResult) * this->query_slice_size, stream[stream_ID]));
        CHECK(cudaMemsetAsync(this->paymentqueryresult_d_0, 0, sizeof(PaymentQueryResult) * this->query_slice_size, stream[stream_ID]));
    case 1:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemsetAsync(this->neworderqueryresult_d_1, 0, sizeof(NeworderQueryResult) * this->query_slice_size, stream[stream_ID]));
        CHECK(cudaMemsetAsync(this->paymentqueryresult_d_1, 0, sizeof(PaymentQueryResult) * this->query_slice_size, stream[stream_ID]));
    case 2:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemsetAsync(this->neworderqueryresult_d_2, 0, sizeof(NeworderQueryResult) * this->query_slice_size, stream[stream_ID]));
        CHECK(cudaMemsetAsync(this->paymentqueryresult_d_2, 0, sizeof(PaymentQueryResult) * this->query_slice_size, stream[stream_ID]));
    case 3:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemsetAsync(this->neworderqueryresult_d_3, 0, sizeof(NeworderQueryResult) * this->query_slice_size, stream[stream_ID]));
        CHECK(cudaMemsetAsync(this->paymentqueryresult_d_3, 0, sizeof(PaymentQueryResult) * this->query_slice_size, stream[stream_ID]));
    case 4:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemsetAsync(this->neworderqueryresult_d_4, 0, sizeof(NeworderQueryResult) * this->query_slice_size, stream[stream_ID]));
        CHECK(cudaMemsetAsync(this->paymentqueryresult_d_4, 0, sizeof(PaymentQueryResult) * this->query_slice_size, stream[stream_ID]));
    case 5:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemsetAsync(this->neworderqueryresult_d_5, 0, sizeof(NeworderQueryResult) * this->query_slice_size, stream[stream_ID]));
        CHECK(cudaMemsetAsync(this->paymentqueryresult_d_5, 0, sizeof(PaymentQueryResult) * this->query_slice_size, stream[stream_ID]));
    case 6:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemsetAsync(this->neworderqueryresult_d_6, 0, sizeof(NeworderQueryResult) * this->query_slice_size, stream[stream_ID]));
        CHECK(cudaMemsetAsync(this->paymentqueryresult_d_6, 0, sizeof(PaymentQueryResult) * this->query_slice_size, stream[stream_ID]));
    case 7:
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemsetAsync(this->neworderqueryresult_d_7, 0, sizeof(NeworderQueryResult) * this->query_slice_size, stream[stream_ID]));
        CHECK(cudaMemsetAsync(this->paymentqueryresult_d_7, 0, sizeof(PaymentQueryResult) * this->query_slice_size, stream[stream_ID]));
    default:
        return;
    }
}

void GPUquery::malloc_query(NeworderQuery *&neworderquery, PaymentQuery *&paymentquery,
                            uint32_t device_ID)
{
    bool isExisted = false;
    for (uint32_t i = 0; i < this->device_cnt; i++)
    {
        if (device_ID == this->device_IDs[i])
        {
            isExisted = true;
            break;
        }
    }
    if (isExisted)
    {
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMalloc((void **)&neworderquery, sizeof(NeworderQuery) * this->neworderquery_slice_size));
        CHECK(cudaMalloc((void **)&paymentquery, sizeof(PaymentQuery) * this->paymentquery_slice_size));
        // std::cout << "device_ID:" << device_ID << std::endl;
        // std::cout << "neworderquery:" << neworderquery << std::endl;
        // std::cout << "paymentquery:" << paymentquery << std::endl;
    }
}

void GPUquery::initialize_query()
{
    this->malloc_query(this->neworderquery_d_0, this->paymentquery_d_0, 0);
    this->malloc_query(this->neworderquery_d_1, this->paymentquery_d_1, 1);
    this->malloc_query(this->neworderquery_d_2, this->paymentquery_d_2, 2);
    this->malloc_query(this->neworderquery_d_3, this->paymentquery_d_3, 3);
    this->malloc_query(this->neworderquery_d_4, this->paymentquery_d_4, 4);
    this->malloc_query(this->neworderquery_d_5, this->paymentquery_d_5, 5);
    this->malloc_query(this->neworderquery_d_6, this->paymentquery_d_6, 6);
    this->malloc_query(this->neworderquery_d_7, this->paymentquery_d_7, 7);
}

void GPUquery::free_query(NeworderQuery *&neworderquery, PaymentQuery *&paymentquery,
                          uint32_t device_ID)
{
    bool isExisted = false;
    for (uint32_t i = 0; i < this->device_cnt; i++)
    {
        if (device_ID == this->device_IDs[i])
        {
            isExisted = true;
            break;
        }
    }
    if (isExisted)
    {
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaFree(neworderquery));
        CHECK(cudaFree(paymentquery));
    }
}

void GPUquery::release_query()
{
    free_query(this->neworderquery_d_0, this->paymentquery_d_0, 0);
    free_query(this->neworderquery_d_1, this->paymentquery_d_1, 1);
    free_query(this->neworderquery_d_2, this->paymentquery_d_2, 2);
    free_query(this->neworderquery_d_3, this->paymentquery_d_3, 3);
    free_query(this->neworderquery_d_4, this->paymentquery_d_4, 4);
    free_query(this->neworderquery_d_5, this->paymentquery_d_5, 5);
    free_query(this->neworderquery_d_6, this->paymentquery_d_6, 6);
    free_query(this->neworderquery_d_7, this->paymentquery_d_7, 7);
}

void GPUquery::copy_query(NeworderQuery *&neworderquery_d, PaymentQuery *&paymentquery_d,
                          uint32_t epoch_ID, uint32_t device_ID, cudaStream_t *stream)
{
    bool isExisted = false;
    uint32_t location = 0;
    for (uint32_t i = 0; i < this->device_cnt; i++)
    {
        if (device_ID == this->device_IDs[i])
        {
            isExisted = true;
            location = i;
            break;
        }
    }
    if (isExisted)
    {
        CHECK(cudaSetDevice(device_ID));
        uint32_t start_offset = location * neworderquery_slice_size + epoch_ID * this->neworderquery_size;
        // std::cout << "location:" << location << ",neworderquery_slice_size:" << neworderquery_slice_size << ",start_offset:" << start_offset << std::endl;
        CHECK(cudaMemcpyAsync(neworderquery_d, this->neworderquery + start_offset, sizeof(NeworderQuery) * this->neworderquery_slice_size, cudaMemcpyHostToDevice, stream[location]));
        start_offset = location * paymentquery_slice_size + epoch_ID * this->paymentquery_size;
        // std::cout << "location:" << location << ",paymentquery_slice_size:" << paymentquery_slice_size << ",start_offset:" << start_offset << std::endl;
        CHECK(cudaMemcpyAsync(paymentquery_d, this->paymentquery + start_offset, sizeof(PaymentQuery) * this->paymentquery_slice_size, cudaMemcpyHostToDevice, stream[location]));
        // std::cout << "device_ID:" << device_ID << std::endl;
        // std::cout << "neworderquery:" << neworderquery << std::endl;
        // std::cout << "paymentquery:" << paymentquery << std::endl;
    }
}

void GPUquery::copy_query(uint32_t epoch_ID, cudaStream_t *stream)
{
    copy_query(this->neworderquery_d_0, this->paymentquery_d_0, epoch_ID, 0, stream);
    copy_query(this->neworderquery_d_1, this->paymentquery_d_1, epoch_ID, 1, stream);
    copy_query(this->neworderquery_d_2, this->paymentquery_d_2, epoch_ID, 2, stream);
    copy_query(this->neworderquery_d_3, this->paymentquery_d_3, epoch_ID, 3, stream);
    copy_query(this->neworderquery_d_4, this->paymentquery_d_4, epoch_ID, 4, stream);
    copy_query(this->neworderquery_d_5, this->paymentquery_d_5, epoch_ID, 5, stream);
    copy_query(this->neworderquery_d_6, this->paymentquery_d_6, epoch_ID, 6, stream);
    copy_query(this->neworderquery_d_7, this->paymentquery_d_7, epoch_ID, 7, stream);
}

NeworderQuery *GPUquery::get_neworderquery_d(uint32_t device_ID)
{
    switch (device_ID)
    {
    case 0:
        return this->neworderquery_d_0;
    case 1:
        return this->neworderquery_d_1;
    case 2:
        return this->neworderquery_d_2;
    case 3:
        return this->neworderquery_d_3;
    case 4:
        return this->neworderquery_d_4;
    case 5:
        return this->neworderquery_d_5;
    case 6:
        return this->neworderquery_d_6;
    case 7:
        return this->neworderquery_d_7;
    default:
        return nullptr;
    }
}

PaymentQuery *GPUquery::get_paymentquery_d(uint32_t device_ID)
{
    switch (device_ID)
    {
    case 0:
        return this->paymentquery_d_0;
    case 1:
        return this->paymentquery_d_1;
    case 2:
        return this->paymentquery_d_2;
    case 3:
        return this->paymentquery_d_3;
    case 4:
        return this->paymentquery_d_4;
    case 5:
        return this->paymentquery_d_5;
    case 6:
        return this->paymentquery_d_6;
    case 7:
        return this->paymentquery_d_7;
    default:
        return nullptr;
    }
}

uint32_t *GPUquery::get_device_IDs()
{
    return this->device_IDs;
}

uint32_t GPUquery::get_device_cnt()
{
    return this->device_cnt;
}

void GPUquery::initialize_param()
{
    this->malloc_param(this->paramquery_d_0, this->paramquery_0, 0);
    this->malloc_param(this->paramquery_d_1, this->paramquery_1, 1);
    this->malloc_param(this->paramquery_d_2, this->paramquery_2, 2);
    this->malloc_param(this->paramquery_d_3, this->paramquery_3, 3);
    this->malloc_param(this->paramquery_d_4, this->paramquery_4, 4);
    this->malloc_param(this->paramquery_d_5, this->paramquery_5, 5);
    this->malloc_param(this->paramquery_d_6, this->paramquery_6, 6);
    this->malloc_param(this->paramquery_d_7, this->paramquery_7, 7);
}

void GPUquery::malloc_param(ParamQuery *&param_g, ParamQuery &param_c, uint32_t device_ID)
{
    bool isExisted = false;
    uint32_t location = 0;
    for (uint32_t i = 0; i < this->device_cnt; i++)
    {
        if (device_ID == this->device_IDs[i])
        {
            isExisted = true;
            location = i;
        }
    }
    if (isExisted)
    {
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMalloc((void **)&param_g, sizeof(ParamQuery)));
        param_c.device_cnt = this->device_cnt;
        param_c.device_ID = location;
        param_c.epoch_sync = this->epoch_sync;

        param_c.neworderquery_slice_size = this->neworderquery_slice_size;
        param_c.paymentquery_slice_size = this->paymentquery_slice_size;
        param_c.query_slice_size = param_c.neworderquery_slice_size + param_c.paymentquery_slice_size;

        param_c.warehouse_tbl_size = this->warehouse_tbl_size / this->device_cnt;
        param_c.district_tbl_size = this->district_tbl_size / this->device_cnt;
        param_c.customer_tbl_size = this->customer_tbl_size / this->device_cnt;
        param_c.history_tbl_size = this->history_tbl_size / this->device_cnt;
        param_c.neworder_tbl_size = this->neworder_tbl_size / this->device_cnt;
        param_c.order_tbl_size = this->order_tbl_size / this->device_cnt;
        param_c.orderline_tbl_size = this->orderline_tbl_size / this->device_cnt;
        param_c.stock_tbl_size = this->stock_tbl_size / this->device_cnt;
        param_c.item_tbl_size = this->item_tbl_size;

        param_c.buffer_size = this->buffer_size;

#ifdef PRINT_PARAM
        std::cout << "device_ID:" << device_ID << std::endl;
        std::cout << "param_c.device_cnt:" << param_c.device_cnt << std::endl;
        std::cout << "param_c.device_ID:" << param_c.device_ID << std::endl;
        std::cout << "param_c.epoch_sync:" << param_c.epoch_sync << std::endl;
        std::cout << "param_c.neworderquery_slice_size:" << param_c.neworderquery_slice_size << std::endl;
        std::cout << "param_c.paymentquery_slice_size:" << param_c.paymentquery_slice_size << std::endl;
        std::cout << "param_c.query_slice_size:" << param_c.query_slice_size << std::endl;
        std::cout << "param_c.warehouse_tbl_size:" << param_c.warehouse_tbl_size << std::endl;
        std::cout << "param_c.district_tbl_size:" << param_c.district_tbl_size << std::endl;
        std::cout << "param_c.customer_tbl_size:" << param_c.customer_tbl_size << std::endl;
        std::cout << "param_c.history_tbl_size:" << param_c.history_tbl_size << std::endl;
        std::cout << "param_c.neworder_tbl_size:" << param_c.neworder_tbl_size << std::endl;
        std::cout << "param_c.order_tbl_size:" << param_c.order_tbl_size << std::endl;
        std::cout << "param_c.orderline_tbl_size:" << param_c.orderline_tbl_size << std::endl;
        std::cout << "param_c.stock_tbl_size:" << param_c.stock_tbl_size << std::endl;
        std::cout << "param_c.item_tbl_size:" << param_c.item_tbl_size << std::endl;
        std::cout << "param_c.buffer_size:" << param_c.buffer_size << std::endl;
#endif
    }
}

void GPUquery::copy_param(uint32_t epoch_ID, cudaStream_t *stream)
{
    this->copy_param(this->paramquery_d_0, this->paramquery_0, 0, stream);
    this->copy_param(this->paramquery_d_1, this->paramquery_1, 1, stream);
    this->copy_param(this->paramquery_d_2, this->paramquery_2, 2, stream);
    this->copy_param(this->paramquery_d_3, this->paramquery_3, 3, stream);
    this->copy_param(this->paramquery_d_4, this->paramquery_4, 4, stream);
    this->copy_param(this->paramquery_d_5, this->paramquery_5, 5, stream);
    this->copy_param(this->paramquery_d_6, this->paramquery_6, 6, stream);
    this->copy_param(this->paramquery_d_7, this->paramquery_7, 7, stream);
}

void GPUquery::copy_param(ParamQuery *&param_g, ParamQuery &param_c, uint32_t device_ID, cudaStream_t *stream)
{
    bool isExisted = false;
    uint32_t location = 0;
    for (uint32_t i = 0; i < this->device_cnt; i++)
    {
        if (device_ID == this->device_IDs[i])
        {
            isExisted = true;
            location = i;
            break;
        }
    }
    if (isExisted)
    {
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMemcpyAsync(param_g, &param_c, sizeof(ParamQuery), cudaMemcpyHostToDevice, stream[location]));
    }
}

void GPUquery::release_param()
{
    free_param(this->paramquery_d_0, this->paramquery_0, 0);
    free_param(this->paramquery_d_1, this->paramquery_1, 1);
    free_param(this->paramquery_d_2, this->paramquery_2, 2);
    free_param(this->paramquery_d_3, this->paramquery_3, 3);
    free_param(this->paramquery_d_4, this->paramquery_4, 4);
    free_param(this->paramquery_d_5, this->paramquery_5, 5);
    free_param(this->paramquery_d_6, this->paramquery_6, 6);
    free_param(this->paramquery_d_7, this->paramquery_7, 7);
}

void GPUquery::free_param(ParamQuery *&param_g, ParamQuery &param_c, uint32_t device_ID)
{
    bool isExisted = false;
    for (uint32_t i = 0; i < this->device_cnt; i++)
    {
        if (device_ID == this->device_IDs[i])
        {
            isExisted = true;
            break;
        }
    }
    if (isExisted)
    {
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaFree(param_g));
    }
}

ParamQuery *GPUquery::get_paramquery_d(uint32_t device_ID)
{
    switch (device_ID)
    {
    case 0:
        return this->paramquery_d_0;
    case 1:
        return this->paramquery_d_1;
    case 2:
        return this->paramquery_d_2;
    case 3:
        return this->paramquery_d_3;
    case 4:
        return this->paramquery_d_4;
    case 5:
        return this->paramquery_d_5;
    case 6:
        return this->paramquery_d_6;
    case 7:
        return this->paramquery_d_7;
    default:
        return nullptr;
    }
}

ParamQuery *GPUquery::get_paramquery(uint32_t device_ID)
{
    switch (device_ID)
    {
    case 0:
        return &this->paramquery_0;
    case 1:
        return &this->paramquery_1;
    case 2:
        return &this->paramquery_2;
    case 3:
        return &this->paramquery_3;
    case 4:
        return &this->paramquery_4;
    case 5:
        return &this->paramquery_5;
    case 6:
        return &this->paramquery_6;
    case 7:
        return &this->paramquery_7;
    default:
        return nullptr;
    }
}

void GPUquery::initialize_willdotable()
{
    this->malloc_willdotable(this->willdotable_d_0, this->willdotable_0, 0);
    this->malloc_willdotable(this->willdotable_d_1, this->willdotable_1, 1);
    this->malloc_willdotable(this->willdotable_d_2, this->willdotable_2, 2);
    this->malloc_willdotable(this->willdotable_d_3, this->willdotable_3, 3);
    this->malloc_willdotable(this->willdotable_d_4, this->willdotable_4, 4);
    this->malloc_willdotable(this->willdotable_d_5, this->willdotable_5, 5);
    this->malloc_willdotable(this->willdotable_d_6, this->willdotable_6, 6);
    this->malloc_willdotable(this->willdotable_d_7, this->willdotable_7, 7);
}

void GPUquery::malloc_willdotable(WilldoTable *&willdotable_g, WilldoTable &willdotable_c, uint32_t device_ID)
{
    bool isExisted = false;
    uint32_t location = 0;
    for (uint32_t i = 0; i < this->device_cnt; i++)
    {
        if (device_ID == this->device_IDs[i])
        {
            isExisted = true;
            location = i;
            break;
        }
    }
    if (isExisted)
    {
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaMalloc((void **)&willdotable_g, sizeof(WilldoTable)));
        willdotable_c.warehouse_log_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync;
        willdotable_c.district_log_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync;
        willdotable_c.customer_log_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync + this->get_paramquery(device_ID)->paymentquery_slice_size * this->device_cnt;
        willdotable_c.history_log_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync;
        willdotable_c.neworder_log_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync;
        willdotable_c.order_log_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync;
        willdotable_c.orderline_log_size = (this->get_paramquery(device_ID)->neworderquery_slice_size * this->get_paramquery(device_ID)->epoch_sync) * (15 + this->device_cnt);
        willdotable_c.stock_log_size = (this->get_paramquery(device_ID)->neworderquery_slice_size * this->get_paramquery(device_ID)->epoch_sync) * (15 + this->device_cnt);
        willdotable_c.item_log_size = (this->get_paramquery(device_ID)->neworderquery_slice_size * this->get_paramquery(device_ID)->epoch_sync) * (15 + this->device_cnt);

        CHECK(cudaMalloc((void **)&willdotable_c.warehouse_log, sizeof(Willdo) * willdotable_c.warehouse_log_size));
        CHECK(cudaMalloc((void **)&willdotable_c.district_log, sizeof(Willdo) * willdotable_c.district_log_size));
        CHECK(cudaMalloc((void **)&willdotable_c.customer_log, sizeof(Willdo) * willdotable_c.customer_log_size));
        CHECK(cudaMalloc((void **)&willdotable_c.history_log, sizeof(Willdo) * willdotable_c.history_log_size));
        CHECK(cudaMalloc((void **)&willdotable_c.neworder_log, sizeof(Willdo) * willdotable_c.neworder_log_size));
        CHECK(cudaMalloc((void **)&willdotable_c.order_log, sizeof(Willdo) * willdotable_c.order_log_size));
        CHECK(cudaMalloc((void **)&willdotable_c.orderline_log, sizeof(Willdo) * willdotable_c.orderline_log_size));
        CHECK(cudaMalloc((void **)&willdotable_c.stock_log, sizeof(Willdo) * willdotable_c.stock_log_size));
        CHECK(cudaMalloc((void **)&willdotable_c.item_log, sizeof(Willdo) * willdotable_c.item_log_size));
        CHECK(cudaMemset(willdotable_c.warehouse_log, 0, sizeof(Willdo) * willdotable_c.warehouse_log_size));
        CHECK(cudaMemset(willdotable_c.district_log, 0, sizeof(Willdo) * willdotable_c.district_log_size));
        CHECK(cudaMemset(willdotable_c.customer_log, 0, sizeof(Willdo) * willdotable_c.customer_log_size));
        CHECK(cudaMemset(willdotable_c.history_log, 0, sizeof(Willdo) * willdotable_c.history_log_size));
        CHECK(cudaMemset(willdotable_c.neworder_log, 0, sizeof(Willdo) * willdotable_c.neworder_log_size));
        CHECK(cudaMemset(willdotable_c.order_log, 0, sizeof(Willdo) * willdotable_c.order_log_size));
        CHECK(cudaMemset(willdotable_c.orderline_log, 0, sizeof(Willdo) * willdotable_c.orderline_log_size));
        CHECK(cudaMemset(willdotable_c.stock_log, 0, sizeof(Willdo) * willdotable_c.stock_log_size));
        CHECK(cudaMemset(willdotable_c.item_log, 0, sizeof(Willdo) * willdotable_c.item_log_size));

        willdotable_c.txn_ID = 0;
        willdotable_c.warehouse_cur = 0;
        willdotable_c.district_cur = 0;
        willdotable_c.customer_cur = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync;
        willdotable_c.history_cur = 0;
        willdotable_c.neworder_cur = 0;
        willdotable_c.order_cur = 0;
        willdotable_c.orderline_cur = ((this->get_paramquery(device_ID)->neworderquery_slice_size) * this->get_paramquery(device_ID)->epoch_sync) * 15;
        willdotable_c.stock_cur = ((this->get_paramquery(device_ID)->neworderquery_slice_size) * this->get_paramquery(device_ID)->epoch_sync) * 15;
        willdotable_c.item_cur = ((this->get_paramquery(device_ID)->neworderquery_slice_size) * this->get_paramquery(device_ID)->epoch_sync) * 15;
        willdotable_c.cur_sendbuffer_offset = 0;

        willdotable_c.warehouse_bitmap_size = this->get_paramquery(device_ID)->device_cnt * (this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->warehouse_tbl_size * this->get_paramquery(device_ID)->query_slice_size) >> 5;
        willdotable_c.district_bitmap_size = this->get_paramquery(device_ID)->device_cnt * (this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->district_tbl_size * this->get_paramquery(device_ID)->query_slice_size) >> 5;
        willdotable_c.customer_bitmap_size = this->get_paramquery(device_ID)->customer_tbl_size;
        willdotable_c.history_bitmap_size = this->get_paramquery(device_ID)->history_tbl_size;
        willdotable_c.neworder_bitmap_size = this->get_paramquery(device_ID)->neworder_tbl_size;
        willdotable_c.order_bitmap_size = this->get_paramquery(device_ID)->order_tbl_size;
        willdotable_c.orderline_bitmap_size = this->get_paramquery(device_ID)->orderline_tbl_size;
        willdotable_c.stock_bitmap_size = this->get_paramquery(device_ID)->stock_tbl_size;
        willdotable_c.item_bitmap_size = this->get_paramquery(device_ID)->item_tbl_size;

        CHECK(cudaMalloc((void **)&willdotable_c.warehouse_bitmap, sizeof(uint32_t) * willdotable_c.warehouse_bitmap_size));
        CHECK(cudaMalloc((void **)&willdotable_c.district_bitmap, sizeof(uint32_t) * willdotable_c.district_bitmap_size));
        // CHECK(cudaMalloc((void **)&willdotable_c.customer_bitmap, sizeof(uint32_t) * willdotable_c.customer_bitmap_size));
        // CHECK(cudaMalloc((void **)&willdotable_c.history_bitmap, sizeof(uint32_t) * willdotable_c.history_bitmap_size));
        // CHECK(cudaMalloc((void **)&willdotable_c.neworder_bitmap, sizeof(uint32_t) * willdotable_c.neworder_bitmap_size));
        // CHECK(cudaMalloc((void **)&willdotable_c.order_bitmap, sizeof(uint32_t) * willdotable_c.order_bitmap_size));
        // CHECK(cudaMalloc((void **)&willdotable_c.orderline_bitmap, sizeof(uint32_t) * willdotable_c.orderline_bitmap_size));
        // CHECK(cudaMalloc((void **)&willdotable_c.stock_bitmap, sizeof(uint32_t) * willdotable_c.stock_bitmap_size));
        // CHECK(cudaMalloc((void **)&willdotable_c.item_bitmap, sizeof(uint32_t) * willdotable_c.item_bitmap_size));
        CHECK(cudaMemset(willdotable_c.warehouse_bitmap, 0, sizeof(uint32_t) * willdotable_c.warehouse_bitmap_size));
        CHECK(cudaMemset(willdotable_c.district_bitmap, 0, sizeof(uint32_t) * willdotable_c.district_bitmap_size));
        // CHECK(cudaMemset(willdotable_c.customer_bitmap, 0xff, sizeof(uint32_t) * willdotable_c.customer_bitmap_size));
        // CHECK(cudaMemset(willdotable_c.history_bitmap, 0xff, sizeof(uint32_t) * willdotable_c.history_bitmap_size));
        // CHECK(cudaMemset(willdotable_c.neworder_bitmap, 0xff, sizeof(uint32_t) * willdotable_c.neworder_bitmap_size));
        // CHECK(cudaMemset(willdotable_c.order_bitmap, 0xff, sizeof(uint32_t) * willdotable_c.order_bitmap_size));
        // CHECK(cudaMemset(willdotable_c.orderline_bitmap, 0xff, sizeof(uint32_t) * willdotable_c.orderline_bitmap_size));
        // CHECK(cudaMemset(willdotable_c.stock_bitmap, 0xff, sizeof(uint32_t) * willdotable_c.stock_bitmap_size));
        // CHECK(cudaMemset(willdotable_c.item_bitmap, 0xff, sizeof(uint32_t) * willdotable_c.item_bitmap_size));

        willdotable_c.warehouse_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt;
        willdotable_c.district_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt;
        willdotable_c.customer_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt;
        willdotable_c.history_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt;
        willdotable_c.neworder_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt;
        willdotable_c.order_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt;
        willdotable_c.orderline_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt * 17;
        willdotable_c.stock_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt * 17;
        willdotable_c.item_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt * 17;

        CHECK(cudaMalloc((void **)&willdotable_c.warehouse_access_control_txn_ID, sizeof(uint32_t) * willdotable_c.warehouse_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.district_access_control_txn_ID, sizeof(uint32_t) * willdotable_c.district_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.customer_access_control_txn_ID, sizeof(uint32_t) * willdotable_c.customer_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.history_access_control_txn_ID, sizeof(uint32_t) * willdotable_c.history_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.neworder_access_control_txn_ID, sizeof(uint32_t) * willdotable_c.neworder_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.order_access_control_txn_ID, sizeof(uint32_t) * willdotable_c.order_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.orderline_access_control_txn_ID, sizeof(uint32_t) * willdotable_c.orderline_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.stock_access_control_txn_ID, sizeof(uint32_t) * willdotable_c.stock_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.item_access_control_txn_ID, sizeof(uint32_t) * willdotable_c.item_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.warehouse_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.warehouse_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.district_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.district_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.customer_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.customer_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.history_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.history_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.neworder_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.neworder_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.order_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.order_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.orderline_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.orderline_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.stock_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.stock_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.item_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.item_access_control_txn_and_row_size));

        CHECK(cudaMalloc((void **)&willdotable_c.warehouse_access_control_row_ID, sizeof(uint32_t) * willdotable_c.warehouse_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.district_access_control_row_ID, sizeof(uint32_t) * willdotable_c.district_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.customer_access_control_row_ID, sizeof(uint32_t) * willdotable_c.customer_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.history_access_control_row_ID, sizeof(uint32_t) * willdotable_c.history_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.neworder_access_control_row_ID, sizeof(uint32_t) * willdotable_c.neworder_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.order_access_control_row_ID, sizeof(uint32_t) * willdotable_c.order_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.orderline_access_control_row_ID, sizeof(uint32_t) * willdotable_c.orderline_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.stock_access_control_row_ID, sizeof(uint32_t) * willdotable_c.stock_access_control_txn_and_row_size));
        CHECK(cudaMalloc((void **)&willdotable_c.item_access_control_row_ID, sizeof(uint32_t) * willdotable_c.item_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.warehouse_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.warehouse_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.district_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.district_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.customer_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.customer_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.history_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.history_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.neworder_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.neworder_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.order_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.order_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.orderline_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.orderline_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.stock_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.stock_access_control_txn_and_row_size));
        CHECK(cudaMemset(willdotable_c.item_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.item_access_control_txn_and_row_size));

        willdotable_c.warehouse_access_control_size = this->get_paramquery(device_ID)->warehouse_tbl_size;
        willdotable_c.district_access_control_size = this->get_paramquery(device_ID)->district_tbl_size;
        willdotable_c.customer_access_control_size = this->get_paramquery(device_ID)->customer_tbl_size;
        willdotable_c.history_access_control_size = this->get_paramquery(device_ID)->history_tbl_size;
        willdotable_c.neworder_access_control_size = this->get_paramquery(device_ID)->neworder_tbl_size;
        willdotable_c.order_access_control_size = this->get_paramquery(device_ID)->order_tbl_size;
        willdotable_c.orderline_access_control_size = this->get_paramquery(device_ID)->orderline_tbl_size;
        willdotable_c.stock_access_control_size = this->get_paramquery(device_ID)->stock_tbl_size;
        willdotable_c.item_access_control_size = this->get_paramquery(device_ID)->item_tbl_size;
        CHECK(cudaMalloc((void **)&willdotable_c.warehouse_access_control, sizeof(uint32_t) * willdotable_c.warehouse_access_control_size));
        CHECK(cudaMalloc((void **)&willdotable_c.district_access_control, sizeof(uint32_t) * willdotable_c.district_access_control_size));
        CHECK(cudaMalloc((void **)&willdotable_c.customer_access_control, sizeof(uint32_t) * willdotable_c.customer_access_control_size));
        CHECK(cudaMalloc((void **)&willdotable_c.history_access_control, sizeof(uint32_t) * willdotable_c.history_access_control_size));
        CHECK(cudaMalloc((void **)&willdotable_c.neworder_access_control, sizeof(uint32_t) * willdotable_c.neworder_access_control_size));
        CHECK(cudaMalloc((void **)&willdotable_c.order_access_control, sizeof(uint32_t) * willdotable_c.order_access_control_size));
        CHECK(cudaMalloc((void **)&willdotable_c.orderline_access_control, sizeof(uint32_t) * willdotable_c.orderline_access_control_size));
        CHECK(cudaMalloc((void **)&willdotable_c.stock_access_control, sizeof(uint32_t) * willdotable_c.stock_access_control_size));
        CHECK(cudaMalloc((void **)&willdotable_c.item_access_control, sizeof(uint32_t) * willdotable_c.item_access_control_size));
        CHECK(cudaMemset(willdotable_c.warehouse_access_control, 0xff, sizeof(uint32_t) * willdotable_c.warehouse_access_control_size));
        CHECK(cudaMemset(willdotable_c.district_access_control, 0xff, sizeof(uint32_t) * willdotable_c.district_access_control_size));
        CHECK(cudaMemset(willdotable_c.customer_access_control, 0xff, sizeof(uint32_t) * willdotable_c.customer_access_control_size));
        CHECK(cudaMemset(willdotable_c.history_access_control, 0xff, sizeof(uint32_t) * willdotable_c.history_access_control_size));
        CHECK(cudaMemset(willdotable_c.neworder_access_control, 0xff, sizeof(uint32_t) * willdotable_c.neworder_access_control_size));
        CHECK(cudaMemset(willdotable_c.order_access_control, 0xff, sizeof(uint32_t) * willdotable_c.order_access_control_size));
        CHECK(cudaMemset(willdotable_c.orderline_access_control, 0xff, sizeof(uint32_t) * willdotable_c.orderline_access_control_size));
        CHECK(cudaMemset(willdotable_c.stock_access_control, 0xff, sizeof(uint32_t) * willdotable_c.stock_access_control_size));
        CHECK(cudaMemset(willdotable_c.item_access_control, 0xff, sizeof(uint32_t) * willdotable_c.item_access_control_size));

        willdotable_c.warehouse_access_control_offset = 0U;
        willdotable_c.district_access_control_offset = 0U;
        willdotable_c.customer_access_control_offset = 0U;
        willdotable_c.history_access_control_offset = 0U;
        willdotable_c.neworder_access_control_offset = 0U;
        willdotable_c.order_access_control_offset = 0U;
        willdotable_c.orderline_access_control_offset = 0U;
        willdotable_c.stock_access_control_offset = 0U;
        willdotable_c.item_access_control_offset = 0U;

        // CHECK(cudaMalloc((void **)&willdotable_c.warehouse_txn_ID_to_log_offset, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        // CHECK(cudaMalloc((void **)&willdotable_c.district_txn_ID_to_log_offset, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        CHECK(cudaMalloc((void **)&willdotable_c.customer_txn_ID_to_log_offset, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        CHECK(cudaMalloc((void **)&willdotable_c.history_txn_ID_to_log_offset, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        CHECK(cudaMalloc((void **)&willdotable_c.neworder_txn_ID_to_log_offset, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        CHECK(cudaMalloc((void **)&willdotable_c.order_txn_ID_to_log_offset, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        CHECK(cudaMalloc((void **)&willdotable_c.orderline_txn_ID_to_log_offset, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt * 15));
        CHECK(cudaMalloc((void **)&willdotable_c.stock_txn_ID_to_log_offset, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt * 15));

        // CHECK(cudaMemset(willdotable_c.warehouse_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        // CHECK(cudaMemset(willdotable_c.district_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        CHECK(cudaMemset(willdotable_c.customer_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        CHECK(cudaMemset(willdotable_c.history_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        CHECK(cudaMemset(willdotable_c.neworder_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        CHECK(cudaMemset(willdotable_c.order_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        CHECK(cudaMemset(willdotable_c.orderline_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt * 15));
        CHECK(cudaMemset(willdotable_c.stock_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt * 15));
    }
}

void GPUquery::release_willdotable()
{
    this->free_willdotable(this->willdotable_d_0, this->willdotable_0, 0);
    this->free_willdotable(this->willdotable_d_1, this->willdotable_1, 1);
    this->free_willdotable(this->willdotable_d_2, this->willdotable_2, 2);
    this->free_willdotable(this->willdotable_d_3, this->willdotable_3, 3);
    this->free_willdotable(this->willdotable_d_4, this->willdotable_4, 4);
    this->free_willdotable(this->willdotable_d_5, this->willdotable_5, 5);
    this->free_willdotable(this->willdotable_d_6, this->willdotable_6, 6);
    this->free_willdotable(this->willdotable_d_7, this->willdotable_7, 7);
}

void GPUquery::free_willdotable(WilldoTable *&willdotable_g, WilldoTable &willdotable_c, uint32_t device_ID)
{
    bool isExisted = false;
    for (uint32_t i = 0; i < this->device_cnt; i++)
    {
        if (device_ID == this->device_IDs[i])
        {
            isExisted = true;
        }
    }
    if (isExisted)
    {
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaFree(willdotable_g));
        CHECK(cudaFree(willdotable_c.warehouse_log));
        CHECK(cudaFree(willdotable_c.district_log));
        CHECK(cudaFree(willdotable_c.customer_log));
        CHECK(cudaFree(willdotable_c.history_log));
        CHECK(cudaFree(willdotable_c.neworder_log));
        CHECK(cudaFree(willdotable_c.order_log));
        CHECK(cudaFree(willdotable_c.orderline_log));
        CHECK(cudaFree(willdotable_c.stock_log));
        CHECK(cudaFree(willdotable_c.item_log));

        CHECK(cudaFree(willdotable_c.warehouse_bitmap));
        CHECK(cudaFree(willdotable_c.district_bitmap));
        // CHECK(cudaFree(willdotable_c.customer_bitmap));
        // CHECK(cudaFree(willdotable_c.history_bitmap));
        // CHECK(cudaFree(willdotable_c.neworder_bitmap));
        // CHECK(cudaFree(willdotable_c.order_bitmap));
        // CHECK(cudaFree(willdotable_c.orderline_bitmap));
        // CHECK(cudaFree(willdotable_c.stock_bitmap));
        // CHECK(cudaFree(willdotable_c.item_bitmap));

        CHECK(cudaFree(willdotable_c.warehouse_access_control_txn_ID));
        CHECK(cudaFree(willdotable_c.district_access_control_txn_ID));
        CHECK(cudaFree(willdotable_c.customer_access_control_txn_ID));
        CHECK(cudaFree(willdotable_c.history_access_control_txn_ID));
        CHECK(cudaFree(willdotable_c.neworder_access_control_txn_ID));
        CHECK(cudaFree(willdotable_c.order_access_control_txn_ID));
        CHECK(cudaFree(willdotable_c.orderline_access_control_txn_ID));
        CHECK(cudaFree(willdotable_c.stock_access_control_txn_ID));
        CHECK(cudaFree(willdotable_c.item_access_control_txn_ID));

        CHECK(cudaFree(willdotable_c.warehouse_access_control_row_ID));
        CHECK(cudaFree(willdotable_c.district_access_control_row_ID));
        CHECK(cudaFree(willdotable_c.customer_access_control_row_ID));
        CHECK(cudaFree(willdotable_c.history_access_control_row_ID));
        CHECK(cudaFree(willdotable_c.neworder_access_control_row_ID));
        CHECK(cudaFree(willdotable_c.order_access_control_row_ID));
        CHECK(cudaFree(willdotable_c.orderline_access_control_row_ID));
        CHECK(cudaFree(willdotable_c.stock_access_control_row_ID));
        CHECK(cudaFree(willdotable_c.item_access_control_row_ID));

        CHECK(cudaFree(willdotable_c.warehouse_access_control));
        CHECK(cudaFree(willdotable_c.district_access_control));
        CHECK(cudaFree(willdotable_c.customer_access_control));
        CHECK(cudaFree(willdotable_c.history_access_control));
        CHECK(cudaFree(willdotable_c.neworder_access_control));
        CHECK(cudaFree(willdotable_c.order_access_control));
        CHECK(cudaFree(willdotable_c.orderline_access_control));
        CHECK(cudaFree(willdotable_c.stock_access_control));
        CHECK(cudaFree(willdotable_c.item_access_control));

        // CHECK(cudaFree(willdotable_c.warehouse_txn_ID_to_log_offset));
        // CHECK(cudaFree(willdotable_c.district_txn_ID_to_log_offset));
        CHECK(cudaFree(willdotable_c.customer_txn_ID_to_log_offset));
        CHECK(cudaFree(willdotable_c.history_txn_ID_to_log_offset));
        CHECK(cudaFree(willdotable_c.neworder_txn_ID_to_log_offset));
        CHECK(cudaFree(willdotable_c.order_txn_ID_to_log_offset));
        CHECK(cudaFree(willdotable_c.orderline_txn_ID_to_log_offset));
        CHECK(cudaFree(willdotable_c.stock_txn_ID_to_log_offset));
    }
}

void GPUquery::copy_willdotable(WilldoTable *&willdotable_g, WilldoTable &willdotable_c, uint32_t device_ID, cudaStream_t *stream)
{
    bool isExisted = false;
    uint32_t location = 0;
    for (uint32_t i = 0; i < this->device_cnt; i++)
    {
        if (device_ID == this->device_IDs[i])
        {
            isExisted = true;
            location = i;
            break;
        }
    }
    if (isExisted)
    {
        CHECK(cudaSetDevice(device_ID));
        willdotable_c.warehouse_log_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync;
        willdotable_c.district_log_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync;
        willdotable_c.customer_log_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync + this->get_paramquery(device_ID)->paymentquery_slice_size * this->device_cnt;
        willdotable_c.history_log_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync;
        willdotable_c.neworder_log_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync;
        willdotable_c.order_log_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync;
        willdotable_c.orderline_log_size = (this->get_paramquery(device_ID)->neworderquery_slice_size * this->get_paramquery(device_ID)->epoch_sync) * (15 + this->device_cnt);
        willdotable_c.stock_log_size = (this->get_paramquery(device_ID)->neworderquery_slice_size * this->get_paramquery(device_ID)->epoch_sync) * (15 + this->device_cnt);
        willdotable_c.item_log_size = (this->get_paramquery(device_ID)->neworderquery_slice_size * this->get_paramquery(device_ID)->epoch_sync) * (15 + this->device_cnt);
        CHECK(cudaMemsetAsync(willdotable_c.warehouse_log, 0, sizeof(Willdo) * willdotable_c.warehouse_log_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.district_log, 0, sizeof(Willdo) * willdotable_c.district_log_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.customer_log, 0, sizeof(Willdo) * willdotable_c.customer_log_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.history_log, 0, sizeof(Willdo) * willdotable_c.history_log_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.neworder_log, 0, sizeof(Willdo) * willdotable_c.neworder_log_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.order_log, 0, sizeof(Willdo) * willdotable_c.order_log_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.orderline_log, 0, sizeof(Willdo) * willdotable_c.orderline_log_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.stock_log, 0, sizeof(Willdo) * willdotable_c.stock_log_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.item_log, 0, sizeof(Willdo) * willdotable_c.item_log_size, stream[location]));

        willdotable_c.txn_ID = 0;
        willdotable_c.warehouse_cur = 0;
        willdotable_c.district_cur = 0;
        willdotable_c.customer_cur = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync;
        willdotable_c.history_cur = 0;
        willdotable_c.neworder_cur = 0;
        willdotable_c.order_cur = 0;
        willdotable_c.orderline_cur = ((this->get_paramquery(device_ID)->neworderquery_slice_size) * this->get_paramquery(device_ID)->epoch_sync) * 15;
        willdotable_c.stock_cur = ((this->get_paramquery(device_ID)->neworderquery_slice_size) * this->get_paramquery(device_ID)->epoch_sync) * 15;
        willdotable_c.item_cur = ((this->get_paramquery(device_ID)->neworderquery_slice_size) * this->get_paramquery(device_ID)->epoch_sync) * 15;
        willdotable_c.cur_sendbuffer_offset = 0;
        willdotable_c.warehouse_bitmap_size = this->get_paramquery(device_ID)->device_cnt * (this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->warehouse_tbl_size * (this->get_paramquery(device_ID)->query_slice_size)) >> 5;
        willdotable_c.district_bitmap_size = this->get_paramquery(device_ID)->device_cnt * (this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->district_tbl_size * (this->get_paramquery(device_ID)->query_slice_size)) >> 5;
        // willdotable_c.customer_bitmap_size = this->get_paramquery(device_ID)->customer_tbl_size;
        // willdotable_c.history_bitmap_size = this->get_paramquery(device_ID)->history_tbl_size;
        // willdotable_c.neworder_bitmap_size = this->get_paramquery(device_ID)->neworder_tbl_size;
        // willdotable_c.order_bitmap_size = this->get_paramquery(device_ID)->order_tbl_size;
        // willdotable_c.orderline_bitmap_size = this->get_paramquery(device_ID)->orderline_tbl_size;
        // willdotable_c.stock_bitmap_size = this->get_paramquery(device_ID)->stock_tbl_size;
        // willdotable_c.item_bitmap_size = this->get_paramquery(device_ID)->item_tbl_size;
        CHECK(cudaMemsetAsync(willdotable_c.warehouse_bitmap, 0, sizeof(uint32_t) * willdotable_c.warehouse_bitmap_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.district_bitmap, 0, sizeof(uint32_t) * willdotable_c.district_bitmap_size, stream[location]));
        // CHECK(cudaMemsetAsync(willdotable_c.customer_bitmap, 0, sizeof(uint32_t) * willdotable_c.customer_bitmap_size, stream[location]));
        // CHECK(cudaMemsetAsync(willdotable_c.history_bitmap, 0, sizeof(uint32_t) * willdotable_c.history_bitmap_size, stream[location]));
        // CHECK(cudaMemsetAsync(willdotable_c.neworder_bitmap, 0, sizeof(uint32_t) * willdotable_c.neworder_bitmap_size, stream[location]));
        // CHECK(cudaMemsetAsync(willdotable_c.order_bitmap, 0, sizeof(uint32_t) * willdotable_c.order_bitmap_size, stream[location]));
        // CHECK(cudaMemsetAsync(willdotable_c.orderline_bitmap, 0, sizeof(uint32_t) * willdotable_c.orderline_bitmap_size, stream[location]));
        // CHECK(cudaMemsetAsync(willdotable_c.stock_bitmap, 0, sizeof(uint32_t) * willdotable_c.stock_bitmap_size, stream[location]));
        // CHECK(cudaMemsetAsync(willdotable_c.item_bitmap, 0, sizeof(uint32_t) * willdotable_c.item_bitmap_size, stream[location]));

        willdotable_c.warehouse_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt;
        willdotable_c.district_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt;
        willdotable_c.customer_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt;
        willdotable_c.history_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt;
        willdotable_c.neworder_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt;
        willdotable_c.order_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt;
        willdotable_c.orderline_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt * 17;
        willdotable_c.stock_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt * 17;
        willdotable_c.item_access_control_txn_and_row_size = this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt * 17;

        CHECK(cudaMemsetAsync(willdotable_c.warehouse_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.warehouse_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.district_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.district_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.customer_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.customer_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.history_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.history_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.neworder_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.neworder_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.order_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.order_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.orderline_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.orderline_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.stock_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.stock_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.item_access_control_txn_ID, 0, sizeof(uint32_t) * willdotable_c.item_access_control_txn_and_row_size, stream[location]));

        CHECK(cudaMemsetAsync(willdotable_c.warehouse_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.warehouse_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.district_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.district_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.customer_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.customer_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.history_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.history_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.neworder_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.neworder_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.order_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.order_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.orderline_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.orderline_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.stock_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.stock_access_control_txn_and_row_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.item_access_control_row_ID, 0, sizeof(uint32_t) * willdotable_c.item_access_control_txn_and_row_size, stream[location]));

        willdotable_c.warehouse_access_control_size = this->get_paramquery(device_ID)->warehouse_tbl_size;
        willdotable_c.district_access_control_size = this->get_paramquery(device_ID)->district_tbl_size;
        willdotable_c.customer_access_control_size = this->get_paramquery(device_ID)->customer_tbl_size;
        willdotable_c.history_access_control_size = this->get_paramquery(device_ID)->history_tbl_size;
        willdotable_c.neworder_access_control_size = this->get_paramquery(device_ID)->neworder_tbl_size;
        willdotable_c.order_access_control_size = this->get_paramquery(device_ID)->order_tbl_size;
        willdotable_c.orderline_access_control_size = this->get_paramquery(device_ID)->orderline_tbl_size;
        willdotable_c.stock_access_control_size = this->get_paramquery(device_ID)->stock_tbl_size;
        willdotable_c.item_access_control_size = this->get_paramquery(device_ID)->item_tbl_size;

        CHECK(cudaMemsetAsync(willdotable_c.warehouse_access_control, 0xff, sizeof(uint32_t) * willdotable_c.warehouse_access_control_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.district_access_control, 0xff, sizeof(uint32_t) * willdotable_c.district_access_control_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.customer_access_control, 0xff, sizeof(uint32_t) * willdotable_c.customer_access_control_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.history_access_control, 0xff, sizeof(uint32_t) * willdotable_c.history_access_control_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.neworder_access_control, 0xff, sizeof(uint32_t) * willdotable_c.neworder_access_control_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.order_access_control, 0xff, sizeof(uint32_t) * willdotable_c.order_access_control_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.orderline_access_control, 0xff, sizeof(uint32_t) * willdotable_c.orderline_access_control_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.stock_access_control, 0xff, sizeof(uint32_t) * willdotable_c.stock_access_control_size, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.item_access_control, 0xff, sizeof(uint32_t) * willdotable_c.item_access_control_size, stream[location]));

        willdotable_c.warehouse_access_control_offset = 0U;
        willdotable_c.district_access_control_offset = 0U;
        willdotable_c.customer_access_control_offset = 0U;
        willdotable_c.history_access_control_offset = 0U;
        willdotable_c.neworder_access_control_offset = 0U;
        willdotable_c.order_access_control_offset = 0U;
        willdotable_c.orderline_access_control_offset = 0U;
        willdotable_c.stock_access_control_offset = 0U;
        willdotable_c.item_access_control_offset = 0U;

        // CHECK(cudaMemset(willdotable_c.warehouse_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        // CHECK(cudaMemset(willdotable_c.district_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt));
        CHECK(cudaMemsetAsync(willdotable_c.customer_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.history_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.neworder_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.order_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.orderline_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt * 15, stream[location]));
        CHECK(cudaMemsetAsync(willdotable_c.stock_txn_ID_to_log_offset, 0, sizeof(uint32_t) * this->get_paramquery(device_ID)->query_slice_size * this->get_paramquery(device_ID)->epoch_sync * this->get_paramquery(device_ID)->device_cnt * 15, stream[location]));

        CHECK(cudaMemcpyAsync(willdotable_g, &willdotable_c, sizeof(WilldoTable), cudaMemcpyHostToDevice, stream[location]));

        // std::cout << "copy_willdotable " << device_ID << std::endl;
    }
}

void GPUquery::copy_willdotable(uint32_t epoch_ID, cudaStream_t *stream)
{

    this->copy_willdotable(this->willdotable_d_0, this->willdotable_0, 0, stream);
    this->copy_willdotable(this->willdotable_d_1, this->willdotable_1, 1, stream);
    this->copy_willdotable(this->willdotable_d_2, this->willdotable_2, 2, stream);
    this->copy_willdotable(this->willdotable_d_3, this->willdotable_3, 3, stream);
    this->copy_willdotable(this->willdotable_d_4, this->willdotable_4, 4, stream);
    this->copy_willdotable(this->willdotable_d_5, this->willdotable_5, 5, stream);
    this->copy_willdotable(this->willdotable_d_6, this->willdotable_6, 6, stream);
    this->copy_willdotable(this->willdotable_d_7, this->willdotable_7, 7, stream);
}

WilldoTable *GPUquery::get_willdotable_d(uint32_t device_ID)
{
    switch (device_ID)
    {
    case 0:
        return this->willdotable_d_0;
    case 1:
        return this->willdotable_d_1;
    case 2:
        return this->willdotable_d_2;
    case 3:
        return this->willdotable_d_3;
    case 4:
        return this->willdotable_d_4;
    case 5:
        return this->willdotable_d_5;
    case 6:
        return this->willdotable_d_6;
    case 7:
        return this->willdotable_d_7;
    default:
        return nullptr;
    }
}

WilldoTable *GPUquery::get_willdotable(uint32_t device_ID)
{
    switch (device_ID)
    {
    case 0:
        return &this->willdotable_0;
    case 1:
        return &this->willdotable_1;
    case 2:
        return &this->willdotable_2;
    case 3:
        return &this->willdotable_3;
    case 4:
        return &this->willdotable_4;
    case 5:
        return &this->willdotable_5;
    case 6:
        return &this->willdotable_6;
    case 7:
        return &this->willdotable_7;
    default:
        return nullptr;
    }
}

NeworderQueryResult *GPUquery::get_neworderqueryresult(uint32_t device_ID)
{
    switch (device_ID)
    {
    case 0:
        return this->neworderqueryresult_0;
    case 1:
        return this->neworderqueryresult_1;
    case 2:
        return this->neworderqueryresult_2;
    case 3:
        return this->neworderqueryresult_3;
    case 4:
        return this->neworderqueryresult_4;
    case 5:
        return this->neworderqueryresult_5;
    case 6:
        return this->neworderqueryresult_6;
    case 7:
        return this->neworderqueryresult_7;
    default:
        return nullptr;
    }
}

PaymentQueryResult *GPUquery::get_paymentqueryresult(uint32_t device_ID)
{
    switch (device_ID)
    {
    case 0:
        return this->paymentqueryresult_0;
    case 1:
        return this->paymentqueryresult_1;
    case 2:
        return this->paymentqueryresult_2;
    case 3:
        return this->paymentqueryresult_3;
    case 4:
        return this->paymentqueryresult_4;
    case 5:
        return this->paymentqueryresult_5;
    case 6:
        return this->paymentqueryresult_6;
    case 7:
        return this->paymentqueryresult_7;
    default:
        return nullptr;
    }
}

NeworderQueryResult *GPUquery::get_neworderqueryresult_d(uint32_t device_ID)
{
    switch (device_ID)
    {
    case 0:
        return this->neworderqueryresult_d_0;
    case 1:
        return this->neworderqueryresult_d_1;
    case 2:
        return this->neworderqueryresult_d_2;
    case 3:
        return this->neworderqueryresult_d_3;
    case 4:
        return this->neworderqueryresult_d_4;
    case 5:
        return this->neworderqueryresult_d_5;
    case 6:
        return this->neworderqueryresult_d_6;
    case 7:
        return this->neworderqueryresult_d_7;
    default:
        return nullptr;
    }
}

PaymentQueryResult *GPUquery::get_paymentqueryresult_d(uint32_t device_ID)
{
    switch (device_ID)
    {
    case 0:
        return this->paymentqueryresult_d_0;
    case 1:
        return this->paymentqueryresult_d_1;
    case 2:
        return this->paymentqueryresult_d_2;
    case 3:
        return this->paymentqueryresult_d_3;
    case 4:
        return this->paymentqueryresult_d_4;
    case 5:
        return this->paymentqueryresult_d_5;
    case 6:
        return this->paymentqueryresult_d_6;
    case 7:
        return this->paymentqueryresult_d_7;
    default:
        return nullptr;
    }
}

void GPUquery::print_malloc_size()
{
    uint64_t malloc_size = 0;
    malloc_size += sizeof(Willdo) * (this->neworderquery_slice_size + this->paymentquery_slice_size) * this->epoch_sync;
    malloc_size += sizeof(Willdo) * (this->neworderquery_slice_size + this->paymentquery_slice_size) * this->epoch_sync;
    malloc_size += sizeof(Willdo) * (this->neworderquery_slice_size + this->paymentquery_slice_size) * this->epoch_sync;
    malloc_size += sizeof(Willdo) * (this->neworderquery_slice_size + this->paymentquery_slice_size) * this->epoch_sync;
    malloc_size += sizeof(Willdo) * (this->neworderquery_slice_size + this->paymentquery_slice_size) * this->epoch_sync;
    malloc_size += sizeof(Willdo) * (this->neworderquery_slice_size + this->paymentquery_slice_size) * this->epoch_sync;
    malloc_size += (sizeof(Willdo) * (this->neworderquery_slice_size + this->paymentquery_slice_size) * this->epoch_sync) << 4;
    malloc_size += (sizeof(Willdo) * (this->neworderquery_slice_size + this->paymentquery_slice_size) * this->epoch_sync) << 4;
    malloc_size += (sizeof(Willdo) * (this->neworderquery_slice_size + this->paymentquery_slice_size) * this->epoch_sync) << 4;
    malloc_size += sizeof(WilldoTable);

    uint64_t bitmap_size = 0;
    bitmap_size += (sizeof(uint32_t) * this->epoch_sync * (this->neworderquery_slice_size + this->paymentquery_slice_size) * this->warehouse_tbl_size) >> 5;
    bitmap_size += (sizeof(uint32_t) * this->epoch_sync * (this->neworderquery_slice_size + this->paymentquery_slice_size) * this->district_tbl_size) >> 5;
    bitmap_size += (sizeof(uint32_t) * this->epoch_sync * this->customer_tbl_size * 4);
    bitmap_size += (sizeof(uint32_t) * this->epoch_sync * this->history_tbl_size * 4);
    bitmap_size += (sizeof(uint32_t) * this->epoch_sync * this->neworder_tbl_size * 4);
    bitmap_size += (sizeof(uint32_t) * this->epoch_sync * this->order_tbl_size * 4);
    bitmap_size += (sizeof(uint32_t) * this->epoch_sync * this->orderline_tbl_size * 4);
    bitmap_size += (sizeof(uint32_t) * this->epoch_sync * this->stock_tbl_size * 4);
    bitmap_size += (sizeof(uint32_t) * this->epoch_sync * this->item_tbl_size * 4);
    std::cout << "cuda heap malloc_size:" << (malloc_size >>= 10) << "KB." << std::endl;
    std::cout << "cuda heap bitmap_size:" << (bitmap_size >>= 10) << "KB." << std::endl;
}

__global__ void Kernel(NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                       ParamQuery *paramquery, uint32_t device_ID)
{
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    if (thID < paramquery->neworderquery_slice_size)
        printf("deviceID:%d,thID:%d,neworder_W_ID:%d,payment_W_ID:%d,param_deviceID:%d\n", device_ID, thID, neworderquery[thID].W_ID, paymentquery[thID].W_ID, paramquery->device_ID);
    // char tmp[11] = "AbCdEfGhIj";
    // char tmpchar = (char)0;
    // if (tmpchar == 0)
    //     printf("tmp=%s, tmpchar=%c,tmpchar=%d\n", tmp, tmp[tmpchar], tmpchar + 1);
}

__global__ void reset_willdo(ParamQuery *paramquery, WilldoTable *willdotables)
{
}

__global__ void make_willdo(Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                            NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                            ParamQuery *paramquery, WilldoTable *willdotables,
                            uint32_t epoch_ID, Willdo *willdo_send)
{
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t offset = threadIdx.x + blockDim.x * (blockIdx.x / 8);
    uint32_t flag = blockIdx.x % 8;
    uint32_t step = gridDim.x / 8;
    if (flag == 0)
    {
        while (offset < paramquery->neworderquery_slice_size)
        {
            make_neworder_willdo_warehouse(offset, snapshot, neworderquery, neworderqueryresult, paramquery, willdotables, epoch_ID, willdo_send);
            offset += blockDim.x * step;
        }
    }
    else if (flag == 1)
    {
        while (offset < paramquery->neworderquery_slice_size)
        {
            make_neworder_willdo_district(offset, snapshot, neworderquery, neworderqueryresult, paramquery, willdotables, epoch_ID, willdo_send);
            offset += blockDim.x * step;
        }
    }
    else if (flag == 2)
    {
        while (offset < paramquery->neworderquery_slice_size)
        {
            make_neworder_willdo_customer(offset, snapshot, neworderquery, neworderqueryresult, paramquery, willdotables, epoch_ID, willdo_send);
            offset += blockDim.x * step;
        }
    }
    else if (flag == 3)
    {
        while (offset < paramquery->neworderquery_slice_size)
        {
            make_neworder_willdo_neworder(offset, snapshot, neworderquery, neworderqueryresult, paramquery, willdotables, epoch_ID, willdo_send);
            offset += blockDim.x * step;
        }
    }
    else if (flag == 4)
    {
        while (offset < paramquery->neworderquery_slice_size)
        {
            make_neworder_willdo_order(offset, snapshot, neworderquery, neworderqueryresult, paramquery, willdotables, epoch_ID, willdo_send);
            offset += blockDim.x * step;
        }
    }
    else if (flag == 5)
    {
#ifdef TEST_EXECUTE_ORDERLINE
        while (offset < paramquery->neworderquery_slice_size)
        {
            make_neworder_willdo_orderline(offset, snapshot, neworderquery, neworderqueryresult, paramquery, willdotables, epoch_ID, willdo_send);
            offset += blockDim.x * step;
        }
#endif
    }
    else if (flag == 6)
    {
#ifdef TEST_EXECUTE_STOCK
        while (offset < paramquery->neworderquery_slice_size)
        {
            make_neworder_willdo_stock(offset, snapshot, neworderquery, neworderqueryresult, paramquery, willdotables, epoch_ID, willdo_send);
            offset += blockDim.x * step;
        }
#endif
    }
    else if (flag == 7)
    {
#ifdef TEST_EXECUTE_ITEM
        while (offset < paramquery->neworderquery_slice_size)
        {
            make_neworder_willdo_item(offset, snapshot, neworderquery, neworderqueryresult, paramquery, willdotables, epoch_ID, willdo_send);
            offset += blockDim.x * step;
        }
#endif
    }

    __syncthreads();
    offset = threadIdx.x + blockDim.x * (blockIdx.x / 4);
    flag = blockIdx.x % 4;
    step = gridDim.x / 4;
    if (flag == 0)
    {
        while (offset < paramquery->paymentquery_slice_size)
        {
            make_payment_willdo_warehouse(offset, snapshot, paymentquery, paymentqueryresult, paramquery, willdotables, epoch_ID, willdo_send);
            offset += blockDim.x * step;
        }
    }
    else if (flag == 1)
    {
        while (offset < paramquery->paymentquery_slice_size)
        {
            make_payment_willdo_district(offset, snapshot, paymentquery, paymentqueryresult, paramquery, willdotables, epoch_ID, willdo_send);
            offset += blockDim.x * step;
        }
    }
    else if (flag == 2)
    {
        while (offset < paramquery->paymentquery_slice_size)
        {
            make_payment_willdo_customer(offset, snapshot, paymentquery, paymentqueryresult, paramquery, willdotables, epoch_ID, willdo_send);
            offset += blockDim.x * step;
        }
    }
    else if (flag == 3)
    {
        while (offset < paramquery->paymentquery_slice_size)
        {
            make_payment_willdo_history(offset, snapshot, paymentquery, paymentqueryresult, paramquery, willdotables, epoch_ID, willdo_send);
            offset += blockDim.x * step;
        }
    }
}

__device__ void make_neworder_willdo_warehouse(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                               ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send)
{
    if (query_offset < paramquery->neworderquery_slice_size)
    {
        uint32_t loc = 0;
        uint32_t row_ID = 0;
        uint32_t bitmap_offset = 0;
        uint32_t txn_ID = neworderquery[query_offset].txn_ID;

        // warehouse
        loc = query_offset;
        row_ID = neworderquery[query_offset].W_ID % paramquery->warehouse_tbl_size;
        willdotables->warehouse_log[loc].device_ID = paramquery->device_ID;
        willdotables->warehouse_log[loc].epoch_ID = epoch_ID;
        willdotables->warehouse_log[loc].row_ID = row_ID;
        willdotables->warehouse_log[loc].txn_ID = txn_ID;
        willdotables->warehouse_log[loc].data = 0U;
        willdotables->warehouse_log[loc].txnType = 'N';
        willdotables->warehouse_log[loc].tblType = 'W';
        willdotables->warehouse_log[loc].isused = true;
        willdotables->warehouse_log[loc].isexecuted = false;
        neworderqueryresult[txn_ID].W_ID = neworderquery[query_offset].W_ID;
        neworderqueryresult[txn_ID].W_TAX = snapshot->warehouse[row_ID].W_TAX;

        // bitmap_offset = (epoch_ID % paramquery->epoch_sync) * willdotables->warehouse_bitmap_size / paramquery->epoch_sync + ((row_ID * (paramquery->query_slice_size)) >> 5) + (txn_ID >> 5);
        // atomicOr(&willdotables->warehouse_bitmap[bitmap_offset], 1U << (txn_ID & 31));

        // printf("row_ID:%d,txn_ID:%d,txnType:%c\n", row_ID, txn_ID, 'N');
    }
}

__device__ void make_neworder_willdo_district(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                              ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send)
{
    if (query_offset < paramquery->neworderquery_slice_size)
    {
        uint32_t loc = 0;
        uint32_t row_ID = 0;
        uint32_t bitmap_offset = 0;
        uint32_t txn_ID = neworderquery[query_offset].txn_ID;
        // printf("neworder txn_ID:%d\n", txn_ID);

        // district
        loc = query_offset;
        row_ID = (neworderquery[query_offset].W_ID % paramquery->warehouse_tbl_size) * 10 + neworderquery[query_offset].D_ID;
        row_ID = row_ID % paramquery->district_tbl_size;
        willdotables->district_log[loc].device_ID = paramquery->device_ID;
        willdotables->district_log[loc].epoch_ID = epoch_ID;
        willdotables->district_log[loc].row_ID = row_ID;
        willdotables->district_log[loc].txn_ID = txn_ID;
        willdotables->district_log[loc].data = 0U;
        willdotables->district_log[loc].txnType = 'N';
        willdotables->district_log[loc].tblType = 'D';
        willdotables->district_log[loc].isused = true;
        willdotables->district_log[loc].isexecuted = false;
        neworderqueryresult[txn_ID].D_ID = neworderquery[query_offset].D_ID;
        neworderqueryresult[txn_ID].D_TAX = snapshot->district[row_ID].D_TAX;

        // bitmap_offset = (epoch_ID % paramquery->epoch_sync) * willdotables->district_bitmap_size / paramquery->epoch_sync + ((row_ID * (paramquery->query_slice_size)) >> 5) + (txn_ID >> 5);
        // atomicOr(&willdotables->district_bitmap[bitmap_offset], 1U << (txn_ID & 31));

        // printf("row_ID:%d,txn_ID:%d,txnType:%c\n", row_ID, txn_ID, 'N');
    }
}

__device__ void make_neworder_willdo_customer(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                              ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send)
{
    if (query_offset < paramquery->neworderquery_slice_size)
    {
        uint32_t loc = 0;
        uint32_t row_ID = 0;
        uint32_t txn_ID = neworderquery[query_offset].txn_ID;
        // printf("neworder txn_ID:%d\n", txn_ID);

        // customer
        loc = query_offset;
        row_ID = ((neworderquery[query_offset].W_ID % paramquery->warehouse_tbl_size) * 10 + neworderquery[query_offset].D_ID) * 3000 + neworderquery[query_offset].C_ID;
        row_ID = row_ID % paramquery->customer_tbl_size;
        willdotables->customer_log[loc].device_ID = paramquery->device_ID;
        willdotables->customer_log[loc].epoch_ID = epoch_ID;
        willdotables->customer_log[loc].row_ID = row_ID;
        willdotables->customer_log[loc].txn_ID = txn_ID;
        willdotables->customer_log[loc].txnType = 'N';
        willdotables->customer_log[loc].tblType = 'C';
        willdotables->customer_log[loc].isused = true;
        willdotables->customer_log[loc].data = 0U;
        willdotables->customer_log[loc].isexecuted = false;
        willdotables->customer_log[loc].same_row_next_txn_ID = 0xffffffff;
        willdotables->customer_txn_ID_to_log_offset[txn_ID] = loc;
        neworderqueryresult[txn_ID].C_ID = neworderquery[query_offset].C_ID;
        neworderqueryresult[txn_ID].C_CREDIT = snapshot->customer[row_ID].C_CREDIT;
        atomicMin(&willdotables->customer_access_control[row_ID], txn_ID);

        // atomicAdd(&willdotables->customer_access_control[row_ID], 1);
    }
}

__device__ void make_neworder_willdo_neworder(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                              ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send)
{
    if (query_offset < paramquery->neworderquery_slice_size)
    {
        uint32_t loc = 0;
        uint32_t row_ID = 0;
        uint32_t txn_ID = neworderquery[query_offset].txn_ID;
        // printf("neworder txn_ID:%d\n", txn_ID);

        // neworder
        loc = query_offset;
        row_ID = (neworderquery[query_offset].W_ID % paramquery->warehouse_tbl_size) * 30000 + neworderquery[query_offset].N_O_ID;
        row_ID = row_ID % paramquery->neworder_tbl_size;
        willdotables->neworder_log[loc].device_ID = paramquery->device_ID;
        willdotables->neworder_log[loc].epoch_ID = epoch_ID;
        willdotables->neworder_log[loc].row_ID = row_ID;
        willdotables->neworder_log[loc].txn_ID = txn_ID;
        willdotables->neworder_log[loc].data = 0U;
        willdotables->neworder_log[loc].txnType = 'N';
        willdotables->neworder_log[loc].tblType = 'N';
        willdotables->neworder_log[loc].isused = true;
        willdotables->neworder_log[loc].isexecuted = false;
        willdotables->neworder_log[loc].same_row_next_txn_ID = 0xffffffff;
        willdotables->neworder_txn_ID_to_log_offset[txn_ID] = loc;

        atomicMin(&willdotables->neworder_access_control[row_ID], txn_ID);
        // atomicAdd(&willdotables->neworder_access_control[row_ID], 1);
    }
}

__device__ void make_neworder_willdo_order(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                           ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send)
{
    if (query_offset < paramquery->neworderquery_slice_size)
    {
        uint32_t loc = 0;
        uint32_t row_ID = 0;
        uint32_t txn_ID = neworderquery[query_offset].txn_ID;
        // printf("neworder txn_ID:%d\n", txn_ID);

        // order
        loc = query_offset;
        row_ID = (neworderquery[query_offset].W_ID % paramquery->warehouse_tbl_size) * 30000 + neworderquery[query_offset].O_ID;
        row_ID = row_ID % paramquery->order_tbl_size;
        willdotables->order_log[loc].device_ID = paramquery->device_ID;
        willdotables->order_log[loc].epoch_ID = epoch_ID;
        willdotables->order_log[loc].row_ID = row_ID;
        willdotables->order_log[loc].txn_ID = txn_ID;
        willdotables->order_log[loc].txnType = 'N';
        willdotables->order_log[loc].tblType = 'O';
        willdotables->order_log[loc].isused = true;
        willdotables->order_log[loc].isexecuted = false;
        willdotables->order_log[loc].same_row_next_txn_ID = 0xffffffff;
        willdotables->order_txn_ID_to_log_offset[txn_ID] = loc;

        atomicMin(&willdotables->order_access_control[row_ID], txn_ID);
        // atomicAdd(&willdotables->order_access_control[row_ID], 1);
    }
}

__device__ void make_neworder_willdo_orderline(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                               ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send)
{
    if (query_offset < paramquery->neworderquery_slice_size)
    {
        uint32_t loc = 0;
        uint32_t row_ID = 0;
        uint32_t ol_supply_w_id = 0;
        uint32_t buffer_loc = 0;
        uint32_t txn_ID = neworderquery[query_offset].txn_ID * 15;
        uint32_t ol_i_id = 0;

        // orderline
        uint32_t o_ol_cnt = neworderquery[query_offset].O_OL_CNT;
        loc = query_offset * 15;
        for (uint32_t j = 0; j < o_ol_cnt; j++)
        {
            ol_supply_w_id = neworderquery[query_offset].INFO[j].OL_SUPPLY_W_ID;
            ol_i_id = neworderquery[query_offset].INFO[j].OL_I_ID % paramquery->item_tbl_size;
            row_ID = (ol_supply_w_id % paramquery->warehouse_tbl_size) * 450000 + ol_i_id + j;
            row_ID = row_ID % paramquery->orderline_tbl_size;
            if (ol_supply_w_id < paramquery->warehouse_tbl_size * paramquery->device_ID || ol_supply_w_id >= paramquery->warehouse_tbl_size * (paramquery->device_ID + 1))
            {
                buffer_loc = atomicAdd(&willdotables->cur_sendbuffer_offset, 1);
                willdo_send[buffer_loc].device_ID = paramquery->device_ID;
                willdo_send[buffer_loc].epoch_ID = epoch_ID;
                willdo_send[buffer_loc].row_ID = row_ID;
                willdo_send[buffer_loc].txn_ID = txn_ID + j;
                willdo_send[buffer_loc].txnType = 'N';
                willdo_send[buffer_loc].tblType = 'L';
                willdo_send[buffer_loc].isused = true;
                willdo_send[buffer_loc].data = ol_supply_w_id;
                willdo_send[buffer_loc].isexecuted = false;
                willdo_send[buffer_loc].same_row_next_txn_ID = 0xffffffff;
                willdotables->orderline_log[loc + j].isused = false;
            }
            else
            {
                willdotables->orderline_log[loc + j].device_ID = paramquery->device_ID;
                willdotables->orderline_log[loc + j].epoch_ID = epoch_ID;
                willdotables->orderline_log[loc + j].row_ID = row_ID;
                willdotables->orderline_log[loc + j].txn_ID = txn_ID + j;
                willdotables->orderline_log[loc + j].txnType = 'N';
                willdotables->orderline_log[loc + j].tblType = 'L';
                willdotables->orderline_log[loc + j].isused = true;
                willdotables->orderline_log[loc + j].data = 0U;
                willdotables->orderline_log[loc + j].isexecuted = false;
                willdotables->orderline_log[loc + j].same_row_next_txn_ID = 0xffffffff;
                willdotables->orderline_txn_ID_to_log_offset[txn_ID + j] = loc + j;
                atomicMin(&willdotables->orderline_access_control[row_ID], txn_ID + j);
            }
        }
    }
}

__device__ void make_neworder_willdo_stock(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                           ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send)
{
    if (query_offset < paramquery->neworderquery_slice_size)
    {
        uint32_t loc = 0;
        uint32_t row_ID = 0;
        uint32_t ol_supply_w_id = 0;
        uint32_t txn_ID = neworderquery[query_offset].txn_ID * 15;

        // stock
        uint32_t o_ol_cnt = neworderquery[query_offset].O_OL_CNT;
        uint32_t buffer_loc = 0;
        uint32_t ol_i_id = 0;
        loc = query_offset * 15;
        for (uint32_t j = 0; j < o_ol_cnt; ++j)
        {
            ol_supply_w_id = neworderquery[query_offset].INFO[j].OL_SUPPLY_W_ID;
            ol_i_id = neworderquery[query_offset].INFO[j].OL_I_ID % paramquery->item_tbl_size;
            row_ID = ol_i_id + (ol_supply_w_id % paramquery->warehouse_tbl_size) * 100000;
            row_ID = row_ID % paramquery->stock_tbl_size;
            if (ol_supply_w_id < paramquery->warehouse_tbl_size * paramquery->device_ID || ol_supply_w_id >= paramquery->warehouse_tbl_size * (paramquery->device_ID + 1))
            {
                buffer_loc = atomicAdd(&willdotables->cur_sendbuffer_offset, 1);
                willdo_send[buffer_loc].device_ID = paramquery->device_ID;
                willdo_send[buffer_loc].epoch_ID = epoch_ID;
                willdo_send[buffer_loc].row_ID = row_ID;
                willdo_send[buffer_loc].txn_ID = txn_ID + j;
                willdo_send[buffer_loc].txnType = 'N';
                willdo_send[buffer_loc].tblType = 'S';
                willdo_send[buffer_loc].isused = true;
                willdo_send[buffer_loc].data = ol_supply_w_id;
                willdo_send[buffer_loc].isexecuted = false;
                willdo_send[buffer_loc].same_row_next_txn_ID = 0xffffffff;
                willdotables->stock_log[loc + j].isused = false;
            }
            else
            {
                willdotables->stock_log[loc + j].device_ID = paramquery->device_ID;
                willdotables->stock_log[loc + j].epoch_ID = epoch_ID;
                willdotables->stock_log[loc + j].row_ID = row_ID;
                willdotables->stock_log[loc + j].txn_ID = txn_ID + j;
                willdotables->stock_log[loc + j].txnType = 'N';
                willdotables->stock_log[loc + j].tblType = 'S';
                willdotables->stock_log[loc + j].isused = true;
                willdotables->stock_log[loc + j].data = 0U;
                willdotables->stock_log[loc + j].isexecuted = false;
                willdotables->stock_log[loc + j].same_row_next_txn_ID = 0xffffffff;
                willdotables->stock_txn_ID_to_log_offset[txn_ID + j] = loc + j;
                atomicMin(&willdotables->stock_access_control[row_ID], txn_ID + j);
            }
        }
    }
}

__device__ void make_neworder_willdo_item(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                          ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send)
{
    if (query_offset < paramquery->neworderquery_slice_size)
    {
        uint32_t loc = 0;
        uint32_t row_ID = 0;
        uint32_t txn_ID = neworderquery[query_offset].txn_ID;
        // printf("neworder txn_ID:%d\n", txn_ID);

        // item
        uint32_t o_ol_cnt = neworderquery[query_offset].O_OL_CNT;
        loc = query_offset * 15;
        uint32_t result = 0;
        for (uint32_t j = 0; j < o_ol_cnt; ++j)
        {
            row_ID = neworderquery[query_offset].INFO[j].OL_I_ID % paramquery->item_tbl_size;
            willdotables->item_log[loc + j].device_ID = paramquery->device_ID;
            willdotables->item_log[loc + j].epoch_ID = epoch_ID;
            willdotables->item_log[loc + j].row_ID = row_ID;
            willdotables->item_log[loc + j].txn_ID = txn_ID;
            willdotables->item_log[loc + j].txnType = 'N';
            willdotables->item_log[loc + j].tblType = 'I';
            willdotables->item_log[loc + j].isused = true;
            willdotables->item_log[loc + j].isexecuted = false;
            willdotables->item_log[loc].same_row_next_txn_ID = 0xffffffff;
            result += snapshot->item[row_ID].I_PRICE * neworderquery[query_offset].INFO[j].OL_QUANTITY;
            // read only
        }
        neworderqueryresult[neworderquery[query_offset].txn_ID].FIANL_PRICE = result;
    }
}

__device__ void make_payment_willdo_warehouse(uint32_t query_offset, Snapshot *snapshot, PaymentQuery *paymentquery, PaymentQueryResult *paymentqueryresult,
                                              ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send)
{
    if (query_offset < paramquery->paymentquery_slice_size)
    {
        uint32_t loc = 0;
        uint32_t row_ID = 0;
        uint32_t bitmap_offset = 0;
        uint32_t txn_ID = paymentquery[query_offset].txn_ID;

        // warehouse
        loc = query_offset + paramquery->neworderquery_slice_size;
        row_ID = paymentquery[query_offset].W_ID % paramquery->warehouse_tbl_size;
        willdotables->warehouse_log[loc].device_ID = paramquery->device_ID;
        willdotables->warehouse_log[loc].epoch_ID = epoch_ID;
        willdotables->warehouse_log[loc].row_ID = row_ID;
        willdotables->warehouse_log[loc].txn_ID = txn_ID;
        willdotables->warehouse_log[loc].txnType = 'P';
        willdotables->warehouse_log[loc].tblType = 'W';
        willdotables->warehouse_log[loc].isused = true;
        willdotables->warehouse_log[loc].isexecuted = false;
        willdotables->warehouse_log[loc].data = paymentquery[query_offset].H_AMOUNT;
        paymentqueryresult[txn_ID].W_ID = snapshot->warehouse[row_ID].W_ID;
        bitmap_offset = (epoch_ID % paramquery->epoch_sync) * willdotables->warehouse_bitmap_size / paramquery->epoch_sync + ((row_ID * (paramquery->query_slice_size)) >> 5) + (txn_ID >> 5);
        atomicOr(&willdotables->warehouse_bitmap[bitmap_offset], 1U << (txn_ID & 31));
        // printf("row_ID:%d,txn_ID:%d,txnType:%c\n", row_ID, txn_ID, 'P');
    }
}

__device__ void make_payment_willdo_district(uint32_t query_offset, Snapshot *snapshot, PaymentQuery *paymentquery, PaymentQueryResult *paymentqueryresult,
                                             ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send)
{
    if (query_offset < paramquery->paymentquery_slice_size)
    {
        uint32_t loc = 0;
        uint32_t row_ID = 0;
        uint32_t bitmap_offset = 0;
        uint32_t txn_ID = paymentquery[query_offset].txn_ID;

        // printf("payment txn_ID:%d\n", txn_ID);
        // district
        loc = query_offset + paramquery->neworderquery_slice_size;
        row_ID = (paymentquery[query_offset].W_ID % paramquery->warehouse_tbl_size) * 10 + paymentquery[query_offset].D_ID;
        row_ID = row_ID % paramquery->district_tbl_size;
        willdotables->district_log[loc].device_ID = paramquery->device_ID;
        willdotables->district_log[loc].epoch_ID = epoch_ID;
        willdotables->district_log[loc].row_ID = row_ID;
        willdotables->district_log[loc].txn_ID = txn_ID;
        willdotables->district_log[loc].txnType = 'P';
        willdotables->district_log[loc].tblType = 'D';
        willdotables->district_log[loc].isused = true;
        willdotables->district_log[loc].isexecuted = false;
        willdotables->district_log[loc].data = paymentquery[query_offset].H_AMOUNT;
        paymentqueryresult[txn_ID].D_ID = snapshot->district[row_ID].D_ID;
        paymentqueryresult[txn_ID].FIANL_PAYMENT = paymentquery[query_offset].H_AMOUNT;
        bitmap_offset = (epoch_ID % paramquery->epoch_sync) * willdotables->district_bitmap_size / paramquery->epoch_sync + ((row_ID * paramquery->query_slice_size) >> 5) + (txn_ID >> 5);
        atomicOr(&willdotables->district_bitmap[bitmap_offset], 1U << (txn_ID & 31));
        // printf("row_ID:%d,txn_ID:%d,txnType:%c\n", row_ID, txn_ID, 'P');
    }
}

__device__ void make_payment_willdo_customer(uint32_t query_offset, Snapshot *snapshot, PaymentQuery *paymentquery, PaymentQueryResult *paymentqueryresult,
                                             ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send)
{
    if (query_offset < paramquery->paymentquery_slice_size)
    {
        uint32_t loc = 0;
        uint32_t row_ID = 0;
        uint32_t txn_ID = paymentquery[query_offset].txn_ID;
        uint32_t c_w_id = paymentquery[query_offset].C_W_ID;
        uint32_t buffer_loc = 0;
        uint32_t offset = ((paymentquery[query_offset].W_ID % paramquery->warehouse_tbl_size) * 10 + paymentquery[query_offset].D_ID) * 3000;
        // printf("payment txn_ID:%d\n", txn_ID);
        // customer
        loc = query_offset + paramquery->neworderquery_slice_size;
        bool isName = paymentquery[query_offset].isName;
        uint32_t c_last = paymentquery[query_offset].C_LAST & 0xf;
        uint32_t c_last_offset = paymentquery[query_offset].C_LAST >> 4;
        uint32_t c_id = 0;
        if (isName)
        {
            for (size_t i = 0; i < 200; i++)
            {
                if (c_last == snapshot->customer_name_index[offset + c_last * 200 + i])
                {
                    c_id = i;
                    break;
                }
            }
            paymentquery[query_offset].C_ID = c_last_offset;
        }

        row_ID = offset + paymentquery[query_offset].C_ID;
        row_ID = row_ID % paramquery->customer_tbl_size;

        // find by name
        if (c_w_id < paramquery->warehouse_tbl_size * paramquery->device_ID || c_w_id >= paramquery->warehouse_tbl_size * (paramquery->device_ID + 1))
        {
            buffer_loc = atomicAdd(&willdotables->cur_sendbuffer_offset, 1);
            willdo_send[buffer_loc].device_ID = paramquery->device_ID;
            willdo_send[buffer_loc].epoch_ID = epoch_ID;
            willdo_send[buffer_loc].row_ID = row_ID;
            willdo_send[buffer_loc].txn_ID = txn_ID;
            willdo_send[buffer_loc].txnType = 'P';
            willdo_send[buffer_loc].tblType = 'C';
            willdo_send[buffer_loc].isused = true;
            willdo_send[buffer_loc].data = c_w_id;
            willdo_send[buffer_loc].isexecuted = false;
            willdo_send[buffer_loc].same_row_next_txn_ID = 0xffffffff;
            willdotables->customer_log[loc].isused = false;
        }
        else
        {
            willdotables->customer_log[loc].device_ID = paramquery->device_ID;
            willdotables->customer_log[loc].epoch_ID = epoch_ID;
            willdotables->customer_log[loc].row_ID = row_ID;
            willdotables->customer_log[loc].txn_ID = txn_ID;
            willdotables->customer_log[loc].txnType = 'P';
            willdotables->customer_log[loc].tblType = 'C';
            willdotables->customer_log[loc].isused = true;
            willdotables->customer_log[loc].isexecuted = false;
            willdotables->customer_log[loc].data = paymentquery[query_offset].H_AMOUNT;
            willdotables->customer_log[loc].same_row_next_txn_ID = 0xffffffff;
            willdotables->customer_txn_ID_to_log_offset[txn_ID] = loc;
            paymentqueryresult[txn_ID].C_ID = snapshot->customer[row_ID].C_ID;
            atomicMin(&willdotables->customer_access_control[row_ID], txn_ID);
        }
    }
}

__device__ void make_payment_willdo_history(uint32_t query_offset, Snapshot *snapshot, PaymentQuery *paymentquery, PaymentQueryResult *paymentqueryresult,
                                            ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send)
{
    if (query_offset < paramquery->paymentquery_slice_size)
    {
        uint32_t loc = 0;
        uint32_t row_ID = 0;
        uint32_t txn_ID = paymentquery[query_offset].txn_ID;

        // printf("payment txn_ID:%d\n", txn_ID);
        // history
        loc = query_offset;
        row_ID = (paymentquery[query_offset].W_ID % paramquery->warehouse_tbl_size) * 30000 + paymentquery[query_offset].H_ID;
        row_ID = row_ID % paramquery->history_tbl_size;
        willdotables->history_log[loc].device_ID = paramquery->device_ID;
        willdotables->history_log[loc].epoch_ID = epoch_ID;
        willdotables->history_log[loc].row_ID = row_ID;
        willdotables->history_log[loc].txn_ID = txn_ID;
        willdotables->history_log[loc].txnType = 'P';
        willdotables->history_log[loc].tblType = 'H';
        willdotables->history_log[loc].isused = true;
        willdotables->history_log[loc].isexecuted = false;
        willdotables->history_log[loc].same_row_next_txn_ID = 0xffffffff;
        willdotables->history_txn_ID_to_log_offset[txn_ID] = loc;
        atomicMin(&willdotables->history_access_control[row_ID], txn_ID);
    }
}

void initialize_buffer(uint32_t *device_IDs, uint32_t device_cnt)
{
    // std::cout << "initialize buffer" << std::endl;
    long long start_initialize_buffer = current_time();
    willdo_send = (Willdo **)malloc(device_cnt * sizeof(Willdo *));
    willdo_recv = (Willdo **)malloc(device_cnt * sizeof(Willdo *));
    for (uint32_t i = 0; i < device_cnt; i++)
    {
        CHECK(cudaSetDevice(device_IDs[i]));
        CHECK(cudaMalloc((void **)&willdo_send[i], gpuquery->get_buffer_size() * sizeof(Willdo)));
        CHECK(cudaMalloc((void **)&willdo_recv[i], device_cnt * gpuquery->get_buffer_size() * sizeof(Willdo)));
        CHECK(cudaMemset(willdo_send[i], 0, gpuquery->get_buffer_size() * sizeof(Willdo)));
        CHECK(cudaMemset(willdo_recv[i], 0, device_cnt * gpuquery->get_buffer_size() * sizeof(Willdo)));
    }
    // for (uint32_t i = 0; i < device_cnt; i++)
    // {
    //     // CHECK(cudaMemcpyPeerAsync());
    //     for (uint32_t j = 0; j < device_cnt; ++j)
    //     {
    //         CHECK(cudaSetDevice(device_IDs[i]));
    //         CHECK(cudaDeviceEnablePeerAccess(device_IDs[j], 0));
    //         CHECK(cudaSetDevice(device_IDs[j]));
    //         CHECK(cudaDeviceEnablePeerAccess(device_IDs[j], 0));
    //     }
    // }
    long long end_initialize_buffer = current_time();
    float cost_initialize_buffer = duration(start_initialize_buffer, end_initialize_buffer);
    std::cout << "finish initializing buffer,cost:" << cost_initialize_buffer << std::endl;
}

void release_buffer(uint32_t *device_IDs, uint32_t device_cnt)
{
    for (uint32_t i = 0; i < device_cnt; i++)
    {
        CHECK(cudaSetDevice(device_IDs[i]));
        CHECK(cudaFree(willdo_send[i]));
        CHECK(cudaFree(willdo_recv[i]));
    }
    free(willdo_send);
    free(willdo_recv);
}

void copy_willdotables_p2p(cudaStream_t *stream, uint32_t *device_IDs, uint32_t device_cnt, uint32_t size)
{ // need check
    for (uint32_t i = 0; i < device_cnt; i++)
    {
        // for (uint32_t j = i + 1; j < device_cnt; ++j)
        // {
        //     // CHECK(cudaSetDevice(device_IDs[i]));
        //     // CHECK(cudaStreamSynchronize(stream[j]));
        //     CHECK(cudaMemcpyAsync(willdo_recv[j] + i * size, willdo_send[i], size * sizeof(Willdo), cudaMemcpyDeviceToDevice, stream[i]));
        //     CHECK(cudaMemcpyAsync(willdo_recv[i] + j * size, willdo_send[j], size * sizeof(Willdo), cudaMemcpyDeviceToDevice, stream[j]));
        // }
        for (uint32_t j = i + 1; j < device_cnt; j++)
        {
            CHECK(cudaMemcpyPeerAsync(willdo_recv[j] + i * size, device_IDs[j], willdo_send[i], device_IDs[i], size * sizeof(Willdo), stream[i]));
            CHECK(cudaMemcpyPeerAsync(willdo_recv[i] + j * size, device_IDs[i], willdo_send[j], device_IDs[j], size * sizeof(Willdo), stream[j]));
        }
    }
}

__global__ void merge(Snapshot *snapshot, ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_recv)
{
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t offset = thID;
    uint32_t row_ID = 0;
    uint32_t data = 0U;
    uint32_t loc = 0U;
    uint32_t txn_ID = 0;
    while (offset < paramquery->buffer_size * paramquery->device_cnt)
    {
        if (offset < paramquery->device_ID * paramquery->buffer_size || offset >= paramquery->buffer_size * (paramquery->device_ID + 1))
        {
            data = willdo_recv[offset].data;
            if (willdo_recv[offset].isused == true && data >= paramquery->warehouse_tbl_size * paramquery->device_ID && data < paramquery->warehouse_tbl_size * (paramquery->device_ID + 1))
            {
                if (willdo_recv[offset].tblType == 'S')
                { // loc need check
                    loc = atomicAdd(&willdotables->stock_cur, 1);
                    willdotables->stock_log[loc].device_ID = willdo_recv[offset].device_ID;
                    willdotables->stock_log[loc].epoch_ID = willdo_recv[offset].epoch_ID;
                    willdotables->stock_log[loc].row_ID = willdo_recv[offset].row_ID;
                    willdotables->stock_log[loc].txn_ID = willdo_recv[offset].txn_ID;
                    willdotables->stock_log[loc].txnType = willdo_recv[offset].txnType;
                    willdotables->stock_log[loc].tblType = willdo_recv[offset].tblType;
                    willdotables->stock_log[loc].isused = willdo_recv[offset].isused;
                    willdotables->stock_log[loc].isexecuted = willdo_recv[offset].isexecuted;
                    willdotables->stock_log[loc].same_row_next_txn_ID = willdo_recv[offset].same_row_next_txn_ID;
                    row_ID = willdotables->stock_log[loc].row_ID;
                    txn_ID = willdotables->stock_log[loc].txn_ID;
                    willdotables->stock_txn_ID_to_log_offset[txn_ID] = loc;
                    atomicMin(&willdotables->stock_access_control[row_ID], txn_ID);
                }
                else if (willdo_recv[offset].tblType == 'L')
                {
                    loc = atomicAdd(&willdotables->orderline_cur, 1);
                    willdotables->orderline_log[loc].device_ID = willdo_recv[offset].device_ID;
                    willdotables->orderline_log[loc].epoch_ID = willdo_recv[offset].epoch_ID;
                    willdotables->orderline_log[loc].row_ID = willdo_recv[offset].row_ID;
                    willdotables->orderline_log[loc].txn_ID = willdo_recv[offset].txn_ID;
                    willdotables->orderline_log[loc].txnType = willdo_recv[offset].txnType;
                    willdotables->orderline_log[loc].tblType = willdo_recv[offset].tblType;
                    willdotables->orderline_log[loc].isused = willdo_recv[offset].isused;
                    willdotables->orderline_log[loc].isexecuted = willdo_recv[offset].isexecuted;
                    willdotables->orderline_log[loc].same_row_next_txn_ID = willdo_recv[offset].same_row_next_txn_ID;

                    row_ID = willdotables->orderline_log[loc].row_ID;
                    txn_ID = willdotables->orderline_log[loc].txn_ID;
                    willdotables->orderline_txn_ID_to_log_offset[txn_ID] = loc;
                    atomicMin(&willdotables->orderline_access_control[row_ID], txn_ID);
                }
                else if (willdo_recv[offset].tblType == 'C')
                {
                    loc = atomicAdd(&willdotables->customer_cur, 1);
                    willdotables->customer_log[loc].device_ID = willdo_recv[offset].device_ID;
                    willdotables->customer_log[loc].epoch_ID = willdo_recv[offset].epoch_ID;
                    willdotables->customer_log[loc].row_ID = willdo_recv[offset].row_ID;
                    willdotables->customer_log[loc].txn_ID = willdo_recv[offset].txn_ID;
                    willdotables->customer_log[loc].txnType = willdo_recv[offset].txnType;
                    willdotables->customer_log[loc].tblType = willdo_recv[offset].tblType;
                    willdotables->customer_log[loc].isused = willdo_recv[offset].isused;
                    willdotables->customer_log[loc].isexecuted = willdo_recv[offset].isexecuted;
                    willdotables->customer_log[loc].same_row_next_txn_ID = willdo_recv[offset].same_row_next_txn_ID;

                    row_ID = willdotables->customer_log[loc].row_ID;
                    txn_ID = willdotables->customer_log[loc].txn_ID;
                    willdotables->customer_txn_ID_to_log_offset[txn_ID] = loc;
                    atomicMin(&willdotables->customer_access_control[row_ID], txn_ID);
                }
            }
        }
        offset += gridDim.x * blockDim.x;
    }
}

__global__ void execute(Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                        NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                        ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_recv)
{
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t start = 0; // threadIdx.x + blockDim.x * blockIdx.x >> 3;
    // uint32_t flag = blockIdx.x & 7;
    // uint32_t step = gridDim.x >> 3;
// execute warehouse
#ifdef TEST_EXECUTE_WAREHOUSE

    start = thID;
    while (start < willdotables->warehouse_log_size)
    {
        execute_warehouse(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += blockDim.x * gridDim.x;
    }
    // __syncthreads();

#endif

    // execute district

#ifdef TEST_EXECUTE_DISTRICT
    start = thID;
    while (start < willdotables->district_log_size)
    {
        execute_district(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += blockDim.x * gridDim.x;
    }
    // __syncthreads();
#endif

    // execute customer

#ifdef TEST_EXECUTE_CUSTOMER
    start = thID;
    while (start < willdotables->customer_log_size)
    {
        execute_customer(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += blockDim.x * gridDim.x;
    }
    // __syncthreads();
#endif

// execute history
#ifdef TEST_EXECUTE_HISTORY
    start = thID;
    while (start < willdotables->history_log_size)
    {
        execute_history(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += blockDim.x * gridDim.x;
    }
    // __syncthreads();
#endif

// execute neworder
#ifdef TEST_EXECUTE_NEWORDER
    start = thID;
    while (start < willdotables->neworder_log_size)
    {
        execute_neworder(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += blockDim.x * gridDim.x;
    }
    // __syncthreads();
#endif

// execute order
#ifdef TEST_EXECUTE_ORDER
    start = thID;
    while (start < willdotables->order_log_size)
    {
        execute_order(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += blockDim.x * gridDim.x;
    }
    // __syncthreads();
#endif

// execute orderline
#ifdef TEST_EXECUTE_ORDERLINE
    start = thID;
    while (start < willdotables->orderline_log_size)
    {
        execute_orderline(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += blockDim.x * gridDim.x;
    }
    // __syncthreads();
#endif

// execute stock
#ifdef TEST_EXECUTE_STOCK
    start = thID;
    while (start < willdotables->stock_log_size)
    {
        execute_stock(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += blockDim.x * gridDim.x;
    }
    // __syncthreads();
#endif
}

__device__ void execute_warehouse(uint32_t txn_ID, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                  NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                  ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    if (txn_ID < paramquery->query_slice_size * paramquery->epoch_sync && paramquery->paymentquery_slice_size != 0)
    {
        uint32_t bitmap;
        uint32_t check = (1U << (threadIdx.x & 31)) - 1;
        uint32_t result = 0xffffffff;
        uint32_t row_ID = willdotables->warehouse_log[txn_ID].row_ID;
        u_char txnType = willdotables->warehouse_log[txn_ID].txnType;
        // uint32_t W_YTD = willdotables->warehouse_log[txn_ID].data;
        // __shared__ uint32_t warp_result[32];
        int32_t flag = txn_ID >> 5;
        int32_t start_bitmap = row_ID * willdotables->warehouse_bitmap_size / paramquery->warehouse_tbl_size;
        int32_t offset_bitmap = flag + start_bitmap;
        int32_t high = 0u;
        uint32_t same_row_pre_txn_ID = 0xffffffff;
        while (offset_bitmap >= start_bitmap)
        {
            bitmap = willdotables->warehouse_bitmap[offset_bitmap];
            result = check & bitmap;
            if (result != 0U)
            {
                break;
            }
            flag -= 1;
            offset_bitmap -= 1;
            check = 0xffffffff;
        }
        high = 31 - __clz(result);
        same_row_pre_txn_ID = high + (flag << 5);
        // printf("row_ID:%d,txnType:%c,txn_ID:%d,result:%x,same_row_pre_txn_ID:%x,bitmap:%x,flag:%d\n", row_ID, txnType, txn_ID, result, same_row_pre_txn_ID, bitmap, flag);
        willdotables->warehouse_log[txn_ID].same_row_pre_txn_ID = high + (flag << 5);
    }
}

__device__ void execute_district(uint32_t txn_ID, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                 NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                 ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    if (txn_ID < paramquery->query_slice_size * paramquery->epoch_sync && paramquery->paymentquery_slice_size != 0)
    {
        uint32_t bitmap;
        uint32_t check = (1U << (threadIdx.x & 31)) - 1;
        uint32_t result = 0xffffffff;
        uint32_t row_ID = willdotables->district_log[txn_ID].row_ID;
        u_char txnType = willdotables->district_log[txn_ID].txnType;
        int32_t flag = txn_ID >> 5;
        int32_t start_bitmap = row_ID * willdotables->district_bitmap_size / paramquery->district_tbl_size;
        int32_t offset_bitmap = flag + start_bitmap;
        int32_t high = 0u;
        uint32_t same_row_pre_txn_ID = 0xffffffff;
        while (offset_bitmap >= start_bitmap)
        {
            bitmap = willdotables->district_bitmap[offset_bitmap];
            result = check & bitmap;
            if (result != 0U)
            {
                break;
            }
            flag -= 1;
            offset_bitmap -= 1;
            check = 0xffffffff;
        }
        high = 31 - __clz(result);
        same_row_pre_txn_ID = high + (flag << 5);
        // printf("row_ID:%d,txnType:%c,txn_ID:%d,result:%x,same_row_pre_txn_ID:%d,bitmap:%x,flag:%d\n", row_ID, txnType, txn_ID, result, same_row_pre_txn_ID, bitmap, flag);
        willdotables->district_log[txn_ID].same_row_pre_txn_ID = high + (flag << 5);
    }
}

__device__ void execute_customer(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                 NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                 ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    uint32_t txn_ID = 0U;
    char txnType = 'P';
    uint32_t W_YTD = 0U;
    uint32_t row_ID = 0U;
    uint32_t min_txn = 0U;
    bool isused = false;
    uint32_t offset = 0U;
    if (query_offset < willdotables->customer_log_size)
    {
        // load bitmap to shared memory
        isused = willdotables->customer_log[query_offset].isused;
        if (!isused)
        {
            return;
        }
        txnType = willdotables->customer_log[query_offset].txnType;
        if (txnType == 'N')
        {
            return;
        }
        row_ID = willdotables->customer_log[query_offset].row_ID;
        min_txn = willdotables->customer_access_control[row_ID];
        txn_ID = willdotables->customer_log[query_offset].txn_ID;
        if (min_txn == txn_ID && txnType == 'P')
        {
            W_YTD = willdotables->customer_log[query_offset].data;
            snapshot->customer[row_ID].C_BALANCE += W_YTD;
        }
        else
        {
            // printf("row_ID:%d,min_txn:%d,txn_ID:%d,same_row_next_txn_ID:%x\n", row_ID, min_txn, txn_ID, willdotables->customer_log[query_offset].same_row_next_txn_ID);
            uint32_t access_control_offset = atomicAdd(&willdotables->customer_access_control_offset, 1);
            willdotables->customer_access_control_txn_ID[access_control_offset] = txn_ID;
            willdotables->customer_access_control_row_ID[access_control_offset] = row_ID;
            // printf("min_txn:%d,txn_ID:%d,txnType:%c\n", min_txn, txn_ID, txnType);
            while (min_txn < txn_ID)
            {
                offset = willdotables->customer_txn_ID_to_log_offset[min_txn];
                min_txn = atomicMin(&willdotables->customer_log[offset].same_row_next_txn_ID, txn_ID);
            }
            atomicMin(&willdotables->customer_log[query_offset].same_row_next_txn_ID, min_txn);
        }
    }
}

__device__ void execute_history(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    uint32_t row_ID = 0U;
    uint32_t min_txn = 0U;
    bool isused = false;
    uint32_t offset = 0U;
    uint32_t txn_ID = 0U;
    if (query_offset < willdotables->history_log_size)
    {
        // load bitmap to shared memory
        isused = willdotables->history_log[query_offset].isused;
        if (!isused)
        {
            return;
        }
        row_ID = willdotables->history_log[query_offset].row_ID;
        min_txn = willdotables->history_access_control[row_ID];
        txn_ID = willdotables->history_log[query_offset].txn_ID;
        if (min_txn == txn_ID)
        {
            snapshot->history[row_ID].H_C_ID = willdotables->history_log[query_offset].data;
        }
        else
        {
            uint32_t access_control_offset = atomicAdd(&willdotables->history_access_control_offset, 1);
            willdotables->history_access_control_txn_ID[access_control_offset] = txn_ID;
            willdotables->history_access_control_row_ID[access_control_offset] = row_ID;
            // // printf("min_txn:%d,txn_ID:%d,txnType:%c\n", min_txn, txn_ID, 'P');
            while (min_txn < txn_ID)
            {
                offset = willdotables->history_txn_ID_to_log_offset[min_txn];
                min_txn = atomicMin(&willdotables->history_log[offset].same_row_next_txn_ID, txn_ID);
            }
            atomicMin(&willdotables->history_log[query_offset].same_row_next_txn_ID, min_txn);
        }
    }
}

__device__ void execute_neworder(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                 NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                 ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    uint32_t row_ID = 0U;
    uint32_t min_txn = 0U;
    bool isused = false;
    uint32_t offset = 0U;
    uint32_t txn_ID = 0U;
    if (query_offset < willdotables->neworder_log_size)
    {
        isused = willdotables->neworder_log[query_offset].isused;
        if (!isused)
        {
            return;
        }
        row_ID = willdotables->neworder_log[query_offset].row_ID;
        min_txn = willdotables->neworder_access_control[row_ID];
        txn_ID = willdotables->neworder_log[query_offset].txn_ID;
        if (min_txn == txn_ID)
        {
            snapshot->neworder[row_ID].NO_D_ID = willdotables->district_log[query_offset].row_ID;
            snapshot->neworder[row_ID].NO_W_ID = willdotables->warehouse_log[query_offset].row_ID;
        }
        else
        {
            uint32_t access_control_offset = atomicAdd(&willdotables->neworder_access_control_offset, 1);
            willdotables->neworder_access_control_txn_ID[access_control_offset] = txn_ID;
            willdotables->neworder_access_control_row_ID[access_control_offset] = row_ID;
            // // printf("min_txn:%d,txn_ID:%d,txnType:%c\n", min_txn, txn_ID, 'N');
            while (min_txn < txn_ID)
            {
                offset = willdotables->neworder_txn_ID_to_log_offset[min_txn];
                min_txn = atomicMin(&willdotables->neworder_log[offset].same_row_next_txn_ID, txn_ID);
            }
            atomicMin(&willdotables->neworder_log[query_offset].same_row_next_txn_ID, min_txn);
        }
    }
}

__device__ void execute_order(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                              NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                              ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    uint32_t row_ID = 0U;
    uint32_t min_txn = 0U;
    bool isused = false;
    uint32_t offset = 0U;
    uint32_t txn_ID = 0U;
    if (query_offset < willdotables->order_log_size)
    {
        // load bitmap to shared memory
        isused = willdotables->order_log[query_offset].isused;
        if (!isused)
        {
            return;
        }
        row_ID = willdotables->order_log[query_offset].row_ID;
        min_txn = willdotables->order_access_control[row_ID];
        txn_ID = willdotables->order_log[query_offset].txn_ID;
        if (min_txn == txn_ID)
        {
            snapshot->order[row_ID].O_ID = row_ID;
        }
        else
        {
            uint32_t access_control_offset = atomicAdd(&willdotables->order_access_control_offset, 1);
            willdotables->order_access_control_txn_ID[access_control_offset] = txn_ID;
            willdotables->order_access_control_row_ID[access_control_offset] = row_ID;
            // // printf("min_txn:%d,txn_ID:%d,txnType:%c\n", min_txn, txn_ID, 'N');
            while (min_txn < txn_ID)
            {
                offset = willdotables->order_txn_ID_to_log_offset[min_txn];
                min_txn = atomicMin(&willdotables->order_log[offset].same_row_next_txn_ID, txn_ID);
            }
            atomicMin(&willdotables->order_log[query_offset].same_row_next_txn_ID, min_txn);
        }
    }
}

__device__ void execute_orderline(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                  NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                  ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    uint32_t row_ID = 0U;
    uint32_t min_txn = 0U;
    bool isused = false;
    uint32_t offset = 0U;
    uint32_t txn_ID = 0U;
    if (query_offset < willdotables->orderline_log_size)
    {
        // load bitmap to shared memory
        isused = willdotables->orderline_log[query_offset].isused;
        if (!isused)
        {
            return;
        }
        row_ID = willdotables->orderline_log[query_offset].row_ID;
        min_txn = willdotables->orderline_access_control[row_ID];
        txn_ID = willdotables->orderline_log[query_offset].txn_ID;
        if (min_txn == txn_ID)
        {
            snapshot->orderline[row_ID].OL_O_ID = willdotables->orderline_log[query_offset].row_ID;
        }
        else
        {
            uint32_t access_control_offset = atomicAdd(&willdotables->orderline_access_control_offset, 1);
            willdotables->orderline_access_control_txn_ID[access_control_offset] = txn_ID;
            willdotables->orderline_access_control_row_ID[access_control_offset] = row_ID;
            // printf("min_txn:%d,txn_ID:%d,txnType:%c\n", min_txn, txn_ID, 'N');
            while (min_txn < txn_ID)
            {
                offset = willdotables->orderline_txn_ID_to_log_offset[min_txn];
                min_txn = atomicMin(&willdotables->orderline_log[offset].same_row_next_txn_ID, txn_ID);
            }
            atomicMin(&willdotables->orderline_log[query_offset].same_row_next_txn_ID, min_txn);
        }
    }
}

__device__ void execute_stock(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                              NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                              ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    uint32_t row_ID = 0U;
    uint32_t min_txn = 0U;
    bool isused = false;
    uint32_t offset = 0U;
    uint32_t txn_ID = 0U;
    if (query_offset < willdotables->stock_log_size)
    {
        // load bitmap to shared memory

        isused = willdotables->stock_log[query_offset].isused;
        if (!isused)
        {
            return;
        }
        row_ID = willdotables->stock_log[query_offset].row_ID;
        min_txn = willdotables->stock_access_control[row_ID];
        txn_ID = willdotables->stock_log[query_offset].txn_ID;
        if (min_txn == txn_ID)
        {
            snapshot->stock[row_ID].S_QUANTITY -= willdotables->stock_log[query_offset].data;
        }
        else
        {
            uint32_t access_control_offset = atomicAdd(&willdotables->stock_access_control_offset, 1);
            willdotables->stock_access_control_txn_ID[access_control_offset] = txn_ID;
            willdotables->stock_access_control_row_ID[access_control_offset] = row_ID;
            // printf("min_txn:%d,txn_ID:%d,txnType:%c\n", min_txn, txn_ID, 'N');
            while (min_txn < txn_ID)
            {
                offset = willdotables->stock_txn_ID_to_log_offset[min_txn];
                min_txn = atomicMin(&willdotables->stock_log[offset].same_row_next_txn_ID, txn_ID);
            }
            atomicMin(&willdotables->stock_log[query_offset].same_row_next_txn_ID, min_txn);
        }
    }
}

__device__ void execute_item(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                             NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                             ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{ // read only
    uint32_t result = 0U;
    uint32_t row_ID = 0U;
    bool isused = false;
    if (query_offset < willdotables->item_log_size)
    {
        // load bitmap to shared memory
        isused = willdotables->item_log[query_offset].isused;
        if (isused)
        {
            row_ID = willdotables->item_log[query_offset].row_ID;
            result = snapshot->item[row_ID].I_PRICE;
        }
    }
}

__global__ void execute_serial(Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                               NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                               ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_recv)
{
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t start;
#ifdef TEST_EXECUTE_WAREHOUSE
    start = thID;
    while (start < willdotables->warehouse_log_size)
    {
        execute_warehouse_serial(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += gridDim.x * blockDim.x;
    }
#endif
#ifdef TEST_EXECUTE_DISTRICT
    start = thID;
    while (start < willdotables->district_log_size)
    {
        execute_district_serial(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += gridDim.x * blockDim.x;
    }
#endif

    // if (thID == 0)
    // {
    //     printf("willdotables->customer_access_control_offset:%d\n", willdotables->customer_access_control_offset);
    //     printf("willdotables->history_access_control_offset:%d\n", willdotables->history_access_control_offset);
    //     printf("willdotables->neworder_access_control_offset:%d\n", willdotables->neworder_access_control_offset);
    //     printf("willdotables->order_access_control_offset:%d\n", willdotables->order_access_control_offset);
    //     printf("willdotables->orderline_access_control_offset:%d\n", willdotables->orderline_access_control_offset);
    //     printf("willdotables->stock_access_control_offset:%d\n", willdotables->stock_access_control_offset);
    // }

#ifdef TEST_EXECUTE_CUSTOMER
    start = thID;
    while (start < willdotables->customer_access_control_offset)
    {
        execute_customer_serial(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += gridDim.x * blockDim.x;
    }
#endif

#ifdef TEST_EXECUTE_HISTORY
    start = thID;
    while (start < willdotables->history_access_control_offset)
    {
        execute_history_serial(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += gridDim.x * blockDim.x;
    }
#endif

#ifdef TEST_EXECUTE_NEWORDER
    start = thID;
    while (start < willdotables->neworder_access_control_offset)
    {
        execute_neworder_serial(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += gridDim.x * blockDim.x;
    }
#endif

#ifdef TEST_EXECUTE_ORDER
    start = thID;
    while (start < willdotables->order_access_control_offset)
    {
        execute_order_serial(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += gridDim.x * blockDim.x;
    }
#endif

#ifdef TEST_EXECUTE_ORDERLINE
    start = thID;
    while (start < willdotables->orderline_access_control_offset)
    {
        execute_orderline_serial(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += gridDim.x * blockDim.x;
    }
#endif

#ifdef TEST_EXECUTE_STOCK
    start = thID;
    while (start < willdotables->stock_access_control_offset)
    {
        execute_stock_serial(start, snapshot, neworderquery, paymentquery, neworderqueryresult, paymentqueryresult, paramquery, willdotables, willdo_recv);
        start += gridDim.x * blockDim.x;
    }
#endif
}

__device__ void execute_warehouse_serial(uint32_t txn_ID, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                         NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                         ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    if (txn_ID < paramquery->query_slice_size * paramquery->epoch_sync)
    {
        uint32_t row_ID = willdotables->warehouse_log[txn_ID].row_ID;
        u_char txnType = willdotables->warehouse_log[txn_ID].txnType;
        uint32_t W_YTD = willdotables->warehouse_log[txn_ID].data;
        uint32_t pre_txn_ID = willdotables->warehouse_log[txn_ID].same_row_pre_txn_ID;
        if (txnType == 'P') // && pre_txn_ID > paramquery->query_slice_size * paramquery->device_cnt) // payment
        {
            atomicAdd(&snapshot->warehouse[row_ID].W_YTD, W_YTD);
        }
        else if (txnType == 'N') // neworder
        {
            // W_YTD = snapshot->warehouse[row_ID].W_YTD;
            return;
        }
        else
        {
            return;
        }
    }
}

__device__ void execute_district_serial(uint32_t txn_ID, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                        NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                        ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    if (txn_ID < paramquery->query_slice_size * paramquery->epoch_sync)
    {
        uint32_t row_ID = willdotables->district_log[txn_ID].row_ID;
        u_char txnType = willdotables->district_log[txn_ID].txnType;
        uint32_t D_YTD = willdotables->district_log[txn_ID].data;
        uint32_t pre_txn_ID = willdotables->district_log[txn_ID].same_row_pre_txn_ID;
        if (txnType == 'P') // && pre_txn_ID > paramquery->query_slice_size * paramquery->device_cnt) // payment
        {
            atomicAdd(&snapshot->district[row_ID].D_YTD, D_YTD);
        }
        else if (txnType == 'N') // neworder
        {
            // D_YTD = snapshot->district[row_ID].D_YTD;
            return;
        }
        else
        {
            return;
        }
    }
}

__device__ void execute_customer_serial(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                        NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                        ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    if (query_offset < willdotables->customer_access_control_offset)
    {
        uint32_t txn_ID = willdotables->customer_access_control_txn_ID[query_offset];
        uint32_t row_ID = willdotables->customer_access_control_row_ID[query_offset];
        uint32_t log_loc = willdotables->customer_txn_ID_to_log_offset[txn_ID];
        uint32_t first_txn_ID = willdotables->customer_access_control[row_ID];
        if (first_txn_ID >= paramquery->query_slice_size * paramquery->device_cnt)
        {
            return;
        }
        uint32_t first_log_loc = willdotables->customer_txn_ID_to_log_offset[first_txn_ID];
        uint32_t first_same_row_next_txn_ID = willdotables->customer_log[first_log_loc].same_row_next_txn_ID;
        if (txn_ID == first_same_row_next_txn_ID)
        {
            while (first_same_row_next_txn_ID < paramquery->query_slice_size * paramquery->device_cnt)
            {
                snapshot->customer[row_ID].C_BALANCE += willdotables->customer_log[log_loc].data;
                txn_ID = first_same_row_next_txn_ID;
                log_loc = willdotables->customer_txn_ID_to_log_offset[txn_ID];
                first_same_row_next_txn_ID = willdotables->customer_log[log_loc].same_row_next_txn_ID;
            }
        }
        else
        {
            return;
        }
    }
}

__device__ void execute_history_serial(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                       NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                       ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    if (query_offset < willdotables->history_access_control_offset)
    {
        uint32_t txn_ID = willdotables->history_access_control_txn_ID[query_offset];
        uint32_t row_ID = willdotables->history_access_control_row_ID[query_offset];
        uint32_t log_loc = willdotables->history_txn_ID_to_log_offset[txn_ID];
        uint32_t first_txn_ID = willdotables->history_access_control[row_ID];
        if (first_txn_ID >= paramquery->query_slice_size * paramquery->device_cnt)
        {
            return;
        }
        uint32_t first_log_loc = willdotables->history_txn_ID_to_log_offset[first_txn_ID];
        uint32_t first_same_row_next_txn_ID = willdotables->history_log[first_log_loc].same_row_next_txn_ID;
        if (txn_ID == first_same_row_next_txn_ID)
        {
            while (first_same_row_next_txn_ID < paramquery->query_slice_size * paramquery->device_cnt)
            {
                snapshot->history[row_ID].H_C_ID = willdotables->history_log[log_loc].data;
                txn_ID = first_same_row_next_txn_ID;
                log_loc = willdotables->history_txn_ID_to_log_offset[txn_ID];
                first_same_row_next_txn_ID = willdotables->history_log[log_loc].same_row_next_txn_ID;
            }
        }
        else
        {
            return;
        }
    }
}

__device__ void execute_neworder_serial(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                        NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                        ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    if (query_offset < willdotables->neworder_access_control_offset)
    {
        uint32_t txn_ID = willdotables->neworder_access_control_txn_ID[query_offset];
        uint32_t row_ID = willdotables->neworder_access_control_row_ID[query_offset];
        uint32_t log_loc = willdotables->neworder_txn_ID_to_log_offset[txn_ID];
        uint32_t first_txn_ID = willdotables->neworder_access_control[row_ID];
        if (first_txn_ID >= paramquery->query_slice_size * paramquery->device_cnt)
        {
            return;
        }
        uint32_t first_log_loc = willdotables->neworder_txn_ID_to_log_offset[first_txn_ID];
        uint32_t first_same_row_next_txn_ID = willdotables->neworder_log[first_log_loc].same_row_next_txn_ID;
        if (txn_ID == first_same_row_next_txn_ID)
        {
            while (first_same_row_next_txn_ID < paramquery->query_slice_size * paramquery->device_cnt)
            {
                snapshot->neworder[row_ID].NO_O_ID = row_ID;
                txn_ID = first_same_row_next_txn_ID;
                log_loc = willdotables->neworder_txn_ID_to_log_offset[txn_ID];
                first_same_row_next_txn_ID = willdotables->neworder_log[log_loc].same_row_next_txn_ID;
            }
        }
        else
        {
            return;
        }
    }
}

__device__ void execute_order_serial(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                     NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                     ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    if (query_offset < willdotables->order_access_control_offset)
    {
        uint32_t txn_ID = willdotables->order_access_control_txn_ID[query_offset];
        uint32_t row_ID = willdotables->order_access_control_row_ID[query_offset];
        uint32_t log_loc = willdotables->order_txn_ID_to_log_offset[txn_ID];
        uint32_t first_txn_ID = willdotables->order_access_control[row_ID];
        if (first_txn_ID >= paramquery->query_slice_size * paramquery->device_cnt)
        {
            return;
        }
        uint32_t first_log_loc = willdotables->order_txn_ID_to_log_offset[first_txn_ID];
        uint32_t first_same_row_next_txn_ID = willdotables->order_log[first_log_loc].same_row_next_txn_ID;
        if (txn_ID == first_same_row_next_txn_ID)
        {
            while (first_same_row_next_txn_ID < paramquery->query_slice_size * paramquery->device_cnt)
            {
                snapshot->order[row_ID].O_ID = row_ID;
                txn_ID = first_same_row_next_txn_ID;
                log_loc = willdotables->order_txn_ID_to_log_offset[txn_ID];
                first_same_row_next_txn_ID = willdotables->order_log[log_loc].same_row_next_txn_ID;
            }
        }
        else
        {
            return;
        }
    }
}

__device__ void execute_orderline_serial(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                         NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                         ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    if (query_offset < willdotables->orderline_access_control_offset)
    {
        uint32_t txn_ID = willdotables->orderline_access_control_txn_ID[query_offset];
        uint32_t row_ID = willdotables->orderline_access_control_row_ID[query_offset];
        uint32_t log_loc = willdotables->orderline_txn_ID_to_log_offset[txn_ID];
        uint32_t first_txn_ID = willdotables->orderline_access_control[row_ID];
        if (first_txn_ID >= paramquery->query_slice_size * paramquery->device_cnt)
        {
            return;
        }
        uint32_t first_log_loc = willdotables->orderline_txn_ID_to_log_offset[first_txn_ID];
        uint32_t first_same_row_next_txn_ID = willdotables->orderline_log[first_log_loc].same_row_next_txn_ID;
        if (txn_ID == first_same_row_next_txn_ID)
        {
            while (first_same_row_next_txn_ID < paramquery->query_slice_size * paramquery->device_cnt * 16)
            {
                snapshot->orderline[row_ID].OL_O_ID = row_ID;
                txn_ID = first_same_row_next_txn_ID;
                log_loc = willdotables->orderline_txn_ID_to_log_offset[txn_ID];
                first_same_row_next_txn_ID = willdotables->orderline_log[log_loc].same_row_next_txn_ID;
            }
        }
        else
        {
            return;
        }
    }
}

__device__ void execute_stock_serial(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                     NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                     ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv)
{
    if (query_offset < willdotables->stock_access_control_offset)
    {
        uint32_t txn_ID = willdotables->stock_access_control_txn_ID[query_offset];
        uint32_t row_ID = willdotables->stock_access_control_row_ID[query_offset];
        uint32_t log_loc = willdotables->stock_txn_ID_to_log_offset[txn_ID];
        uint32_t first_txn_ID = willdotables->stock_access_control[row_ID];
        if (first_txn_ID >= paramquery->query_slice_size * paramquery->device_cnt)
        {
            return;
        }
        uint32_t first_log_loc = willdotables->stock_txn_ID_to_log_offset[first_txn_ID];
        uint32_t first_same_row_next_txn_ID = willdotables->stock_log[first_log_loc].same_row_next_txn_ID;
        if (txn_ID == first_same_row_next_txn_ID)
        {
            while (first_same_row_next_txn_ID < paramquery->query_slice_size * paramquery->device_cnt * 16)
            {
                snapshot->stock[row_ID].S_QUANTITY -= willdotables->stock_log[log_loc].data;
                txn_ID = first_same_row_next_txn_ID;
                log_loc = willdotables->stock_txn_ID_to_log_offset[txn_ID];
                first_same_row_next_txn_ID = willdotables->stock_log[log_loc].same_row_next_txn_ID;
            }
        }
        else
        {
            return;
        }
    }
}

void launch_make_willdo(uint32_t device_ID, cudaStream_t stream, uint32_t thID)
{
    // CHECK(cudaSetDevice(device_ID));
    // make_willdo<<<512, 512, 0, stream>>>(get_snapshot(device_ID), gpuquery->get_neworderquery_d(device_ID), gpuquery->get_paymentquery_d(device_ID),
    //                                         gpuquery->get_neworderqueryresult_d(device_ID), gpuquery->get_paymentqueryresult_d(device_ID),
    //                                         gpuquery->get_paramquery_d(device_ID), gpuquery->get_willdotable_d(device_ID), 0, willdo_send[thID]);
}

void launchQueryKernel(Result &result)
{
    uint32_t *device_IDs = gpuquery->get_device_IDs();
    uint32_t device_cnt = gpuquery->get_device_cnt();

    cudaStream_t *stream;
    stream = (cudaStream_t *)malloc(sizeof(cudaStream_t) * device_cnt);

    // ulong heap_size = 1024UL * 1024ul * 1024ul * 2ul; // 4GB
    // std::cout << "heap_size:" << heap_size << std::endl;

    // willdotables = (WilldoTable **)malloc(device_cnt * sizeof(WilldoTable *));
    for (uint32_t i = 0; i < device_cnt; i++)
    {
        CHECK(cudaSetDevice(device_IDs[i]));
        // CHECK(cudaDeviceSetLimit(cudaLimitMallocHeapSize, heap_size));
        CHECK(cudaStreamCreate(&stream[i]));
        // CHECK(cudaMalloc((void **)&willdotables[i], sizeof(WilldoTable)));
    }

    initialize_buffer(device_IDs, device_cnt);

    uint32_t epoch_sync = gpuquery->get_epoch_sync();

    std::cout << "start copy_willdotable" << std::endl;
    long long start_copy_willdotable = current_time();

    gpuquery->copy_willdotable(0, stream);
    for (uint32_t i = 0; i < device_cnt; i++)
    {
        CHECK(cudaSetDevice(device_IDs[i]));
        CHECK(cudaStreamSynchronize(stream[i]));
    }

    long long end_copy_willdotable = current_time();
    float cost_copy_willdotable = duration(start_copy_willdotable, end_copy_willdotable);

    std::cout << "copy_willdotable cost:" << cost_copy_willdotable << std::endl;

    // CHECK(cudaDeviceSynchronize());

    std::cout << "start kernels" << std::endl;

    long long start_t = current_time();
    long long end_t = current_time();
    float cost = 0.0f;

#ifndef WARMUP
    start_t = current_time();
#endif

    for (uint32_t epoch_ID = 0; epoch_ID < gpuquery->get_epoch_tp(); epoch_ID++)
    {

#ifdef WARMUP
        // if (epoch_ID == gpuquery->get_epoch_tp() * WARMUP)
        // {
        //     start_t = current_time();
        // }
        if (epoch_ID > gpuquery->get_epoch_tp() * WARMUP)
        {
            start_t = current_time();
        }
#endif

        gpuquery->copy_query(epoch_ID, stream);
        gpuquery->copy_param(epoch_ID, stream);

        for (uint32_t i = 0; i < device_cnt; i++)
        {
#ifdef MAKE_WILLDO
            CHECK(cudaSetDevice(device_IDs[i]));
            // CHECK(cudaOccupancyMaxPotentialBlockSize(&gridSize, &blockSize, make_willdo));
            make_willdo<<<512, 512, 0, stream[i]>>>(get_snapshot(device_IDs[i]), gpuquery->get_neworderquery_d(device_IDs[i]), gpuquery->get_paymentquery_d(device_IDs[i]),
                                                    gpuquery->get_neworderqueryresult_d(device_IDs[i]), gpuquery->get_paymentqueryresult_d(device_IDs[i]),
                                                    gpuquery->get_paramquery_d(device_IDs[i]), gpuquery->get_willdotable_d(device_IDs[i]), epoch_ID, willdo_send[i]);
#endif
        }

        if (epoch_ID % epoch_sync == epoch_sync - 1)
        {
            if (device_cnt > 1)
            {
#ifdef P2P
                copy_willdotables_p2p(stream, device_IDs, device_cnt, gpuquery->get_buffer_size());
#endif
                for (uint32_t i = 0; i < device_cnt; i++)
                {
#ifdef MERGE
                    CHECK(cudaSetDevice(device_IDs[i]));
                    merge<<<512, 512, 0, stream[i]>>>(get_snapshot(device_IDs[i]), gpuquery->get_paramquery_d(device_IDs[i]), gpuquery->get_willdotable_d(device_IDs[i]), epoch_ID, willdo_recv[i]);
#endif
                }
            }

            for (uint32_t i = 0; i < device_cnt; i++)
            {
#ifdef EXECUTE
                CHECK(cudaSetDevice(device_IDs[i]));
                // CHECK(cudaStreamSynchronize(stream[i]));
                execute<<<512, 512, 0, stream[i]>>>(get_snapshot(device_IDs[i]), gpuquery->get_neworderquery_d(device_IDs[i]), gpuquery->get_paymentquery_d(device_IDs[i]),
                                                    gpuquery->get_neworderqueryresult_d(device_IDs[i]), gpuquery->get_paymentqueryresult_d(device_IDs[i]),
                                                    gpuquery->get_paramquery_d(device_IDs[i]), gpuquery->get_willdotable_d(device_IDs[i]), epoch_ID, willdo_recv[i]);
#endif
            }

            for (uint32_t i = 0; i < device_cnt; i++)
            {
#ifdef EXECUTE_SERIAL
                CHECK(cudaSetDevice(device_IDs[i]));
                // CHECK(cudaStreamSynchronize(stream[i]));
                execute_serial<<<512, 512, 0, stream[i]>>>(get_snapshot(device_IDs[i]), gpuquery->get_neworderquery_d(device_IDs[i]), gpuquery->get_paymentquery_d(device_IDs[i]),
                                                           gpuquery->get_neworderqueryresult_d(device_IDs[i]), gpuquery->get_paymentqueryresult_d(device_IDs[i]),
                                                           gpuquery->get_paramquery_d(device_IDs[i]), gpuquery->get_willdotable_d(device_IDs[i]), epoch_ID, willdo_recv[i]);
#endif
            }
        }

        if (epoch_ID % epoch_sync == epoch_sync - 1)
        {
            for (uint32_t i = 0; i < device_cnt; i++)
            {
                // CHECK(cudaSetDevice(device_IDs[i]));
                CHECK(cudaMemcpyAsync(gpuquery->get_neworderqueryresult(device_IDs[i]), gpuquery->get_neworderqueryresult_d(device_IDs[i]), sizeof(NeworderQueryResult) * gpuquery->get_query_slice_size(), cudaMemcpyDeviceToHost, stream[i]));
            }
            for (uint32_t i = 0; i < device_cnt; i++)
            {
                // CHECK(cudaSetDevice(device_IDs[i]));
                CHECK(cudaMemcpyAsync(gpuquery->get_paymentqueryresult(device_IDs[i]), gpuquery->get_paymentqueryresult_d(device_IDs[i]), sizeof(PaymentQueryResult) * gpuquery->get_query_slice_size(), cudaMemcpyDeviceToHost, stream[i]));
            }
        }

        // gpuquery->copy_willdotable(0, stream);

        if (epoch_ID % epoch_sync == epoch_sync - 1)
        {
            for (uint32_t i = 0; i < device_cnt; i++)
            {
                // CHECK(cudaSetDevice(device_IDs[i]));
                CHECK(cudaMemsetAsync(willdo_send[i], 0, gpuquery->get_buffer_size() * sizeof(Willdo), stream[i]))
            }
            for (uint32_t i = 0; i < device_cnt; i++)
            {
                // CHECK(cudaSetDevice(device_IDs[i]));
                CHECK(cudaMemsetAsync(willdo_recv[i], 0, device_cnt * gpuquery->get_buffer_size() * sizeof(Willdo), stream[i]));
            }
            for (uint32_t i = 0; i < device_cnt; i++)
            {
                // CHECK(cudaSetDevice(device_IDs[i]));
                CHECK(cudaMemsetAsync(gpuquery->get_neworderqueryresult_d(device_IDs[i]), 0, sizeof(NeworderQueryResult) * gpuquery->get_query_slice_size(), stream[i]));
            }
            for (uint32_t i = 0; i < device_cnt; i++)
            {
                // CHECK(cudaSetDevice(device_IDs[i]));
                CHECK(cudaMemsetAsync(gpuquery->get_paymentqueryresult_d(device_IDs[i]), 0, sizeof(PaymentQueryResult) * gpuquery->get_query_slice_size(), stream[i]));
            }
        }

        for (uint32_t i = 0; i < device_cnt; i++)
        {
            CHECK(cudaSetDevice(device_IDs[i]));
            CHECK(cudaStreamSynchronize(stream[i]));
        }
#ifdef WARMUP
        if (epoch_ID > gpuquery->get_epoch_tp() * WARMUP)
        {
            end_t = current_time();
            cost += duration(start_t, end_t);
        }
#endif
        gpuquery->copy_willdotable(0, stream);
    }

    // for (uint32_t i = 0; i < device_cnt; i++)
    // {
    //     CHECK(cudaSetDevice(device_IDs[i]));
    //     CHECK(cudaStreamSynchronize(stream[i]));
    // }
    // end_t = current_time();
    // cost += duration(start_t, end_t);

#ifdef WARMUP
    std::cout << "Average cost of " << gpuquery->get_epoch_tp() - gpuquery->get_epoch_tp() * WARMUP << " epoches is:" << cost / (float)(gpuquery->get_epoch_tp() - gpuquery->get_epoch_tp() * WARMUP) << std::endl;
    result.cost = cost / (float)(gpuquery->get_epoch_tp() - gpuquery->get_epoch_tp() * WARMUP);
    result.epoch = gpuquery->get_epoch_tp() - gpuquery->get_epoch_tp() * WARMUP;
    result.batch_size = gpuquery->get_batch_size();
#endif

#ifndef WARMUP
    std::cout << "Average cost of " << gpuquery->get_epoch_tp() << " epoches is:" << cost / (float)(gpuquery->get_epoch_tp()) << std::endl;
    result.cost = cost / (float)(gpuquery->get_epoch_tp());
    result.epoch = gpuquery->get_epoch_tp();
    result.batch_size = gpuquery->get_batch_size();
#endif

    for (uint32_t i = 0; i < device_cnt; i++)
    {
        CHECK(cudaSetDevice(device_IDs[i]));
        CHECK(cudaStreamSynchronize(stream[i]));
    }
    for (uint32_t i = 0; i < device_cnt; i++)
    {
        CHECK(cudaSetDevice(device_IDs[i]));
        CHECK(cudaStreamDestroy(stream[i]));
        // CHECK(cudaFree(willdotables[i]));
    }
    // free(willdotables);
    release_buffer(device_IDs, device_cnt);

    // sleep(10.0f);
}