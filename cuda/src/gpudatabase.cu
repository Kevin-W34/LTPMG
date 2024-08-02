#include "../include/gpudatabase.cuh"

void initialize_gpudatabase(uint32_t warehouse_tbl_size, uint32_t device_cnt, uint32_t *device_IDs)
{
    long long start_cpy = current_time();
    gpudatabase = new GPUdatabase(warehouse_tbl_size, device_cnt, device_IDs);
    long long end_cpy = current_time();
    float cost_cpy = duration(start_cpy, end_cpy);
    std::cout << "initialize GPU database, cost:" << cost_cpy << "s." << std::endl;
}

void release_gpudatabase()
{
    long long start_cpy = current_time();
    gpudatabase->release_snapshot();
    delete gpudatabase;
    long long end_cpy = current_time();
    float cost_cpy = duration(start_cpy, end_cpy);
    std::cout << "release GPU database, cost:" << cost_cpy << "s." << std::endl;
}

void copy_database_to_gpu(WAREHOUSE_ROW *warehouse, DISTRICT_ROW *district,
                          CUSTOMER_ROW *customer, HISTORY_ROW *history,
                          NEWORDER_ROW *neworder, ORDER_ROW *order,
                          ORDERLINE_ROW *orderline, STOCK_ROW *stock,
                          ITEM_ROW *item, uint32_t *customer_name_index)
{
    long long start_cpy = current_time();
    gpudatabase->copy_database_to_gpu<WAREHOUSE_ROW>(warehouse, gpudatabase->get_warehouse(), gpudatabase->get_warehouse_size());
    gpudatabase->copy_database_to_gpu<DISTRICT_ROW>(district, gpudatabase->get_district(), gpudatabase->get_district_size());
    gpudatabase->copy_database_to_gpu<CUSTOMER_ROW>(customer, gpudatabase->get_customer(), gpudatabase->get_customer_size());
    gpudatabase->copy_database_to_gpu<HISTORY_ROW>(history, gpudatabase->get_history(), gpudatabase->get_history_size());
    gpudatabase->copy_database_to_gpu<NEWORDER_ROW>(neworder, gpudatabase->get_neworder(), gpudatabase->get_neworder_size());
    gpudatabase->copy_database_to_gpu<ORDER_ROW>(order, gpudatabase->get_order(), gpudatabase->get_order_size());
    gpudatabase->copy_database_to_gpu<ORDERLINE_ROW>(orderline, gpudatabase->get_orderline(), gpudatabase->get_orderline_size());
    gpudatabase->copy_database_to_gpu<STOCK_ROW>(stock, gpudatabase->get_stock(), gpudatabase->get_stock_size());
    gpudatabase->copy_database_to_gpu<ITEM_ROW>(item, gpudatabase->get_item(), gpudatabase->get_item_size());
    gpudatabase->copy_database_to_gpu<uint32_t>(customer_name_index, gpudatabase->get_customer_name_index(), gpudatabase->get_customer_name_index_size());

    long long end_cpy = current_time();

    gpudatabase->initialize_snapshot();
    float cost_cpy = duration(start_cpy, end_cpy);
    std::cout << "copy database to gpu, cost:" << cost_cpy << "s." << std::endl;
#ifdef PRINT_DATABASE
    gpudatabase->print_warehouse();
#endif
}

void copy_database_to_cpu()
{
    long long start_cpy = current_time();

    long long end_cpy = current_time();
    float cost_cpy = duration(start_cpy, end_cpy);
    std::cout << "copy database to cpu, cost:" << cost_cpy << "s." << std::endl;
}

GPUdatabase::GPUdatabase(uint32_t warehouse_tbl_size,
                         uint32_t device_cnt,
                         uint32_t *device_IDs)
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
    for (auto i = 0; i < this->device_cnt; i++)
    {
        this->device_IDs[i] = device_IDs[i];
        // std::cout << this->device_IDs[i] << " ";
    }
    // std::cout << std::endl;
    this->warehouse_slice_size = this->warehouse_tbl_size / this->device_cnt;
    this->district_slice_size = this->district_tbl_size / this->device_cnt;
    this->customer_slice_size = this->customer_tbl_size / this->device_cnt;
    this->history_slice_size = this->history_tbl_size / this->device_cnt;
    this->neworder_slice_size = this->neworder_tbl_size / this->device_cnt;
    this->order_slice_size = this->order_tbl_size / this->device_cnt;
    this->orderline_slice_size = this->orderline_tbl_size / this->device_cnt;
    this->stock_slice_size = this->stock_tbl_size / this->device_cnt;
    this->item_slice_size = this->item_tbl_size;

    CHECK(cudaSetDevice(this->device_IDs[0]));
    CHECK(cudaMallocHost((void **)&this->warehouse_tbl, sizeof(WAREHOUSE_ROW) * this->warehouse_tbl_size));
    CHECK(cudaMallocHost((void **)&this->district_tbl, sizeof(DISTRICT_ROW) * this->district_tbl_size));
    CHECK(cudaMallocHost((void **)&this->customer_tbl, sizeof(CUSTOMER_ROW) * this->customer_tbl_size));
    CHECK(cudaMallocHost((void **)&this->history_tbl, sizeof(HISTORY_ROW) * this->history_tbl_size));
    CHECK(cudaMallocHost((void **)&this->neworder_tbl, sizeof(NEWORDER_ROW) * this->neworder_tbl_size));
    CHECK(cudaMallocHost((void **)&this->order_tbl, sizeof(ORDER_ROW) * this->order_tbl_size));
    CHECK(cudaMallocHost((void **)&this->orderline_tbl, sizeof(ORDERLINE_ROW) * this->orderline_tbl_size));
    CHECK(cudaMallocHost((void **)&this->stock_tbl, sizeof(STOCK_ROW) * this->stock_tbl_size));
    CHECK(cudaMallocHost((void **)&this->item_tbl, sizeof(ITEM_ROW) * this->item_tbl_size));
    CHECK(cudaMallocHost((void **)&this->customer_name_index, sizeof(uint32_t) * this->customer_tbl_size));
}

GPUdatabase::~GPUdatabase()
{
    CHECK(cudaFreeHost(this->warehouse_tbl));
    CHECK(cudaFreeHost(this->district_tbl));
    CHECK(cudaFreeHost(this->customer_tbl));
    CHECK(cudaFreeHost(this->history_tbl));
    CHECK(cudaFreeHost(this->neworder_tbl));
    CHECK(cudaFreeHost(this->order_tbl));
    CHECK(cudaFreeHost(this->orderline_tbl));
    CHECK(cudaFreeHost(this->stock_tbl));
    CHECK(cudaFreeHost(this->item_tbl));
    CHECK(cudaFreeHost(this->customer_name_index));
    delete this->device_IDs;
}

template <typename Table>
void GPUdatabase::copy_database_to_gpu(Table *table_c, Table *table_g, uint32_t size_of_t)
{
    CHECK(cudaMemcpy(table_g, table_c, sizeof(Table) * size_of_t, cudaMemcpyHostToHost));
}

WAREHOUSE_ROW *GPUdatabase::get_warehouse()
{
    return this->warehouse_tbl;
}

uint32_t GPUdatabase::get_warehouse_size()
{
    return this->warehouse_tbl_size;
}

DISTRICT_ROW *GPUdatabase::get_district()
{
    return this->district_tbl;
}

uint32_t GPUdatabase::get_district_size()
{
    return this->district_tbl_size;
}

CUSTOMER_ROW *GPUdatabase::get_customer()
{
    return this->customer_tbl;
}

uint32_t GPUdatabase::get_customer_size()
{
    return this->customer_tbl_size;
}

HISTORY_ROW *GPUdatabase::get_history()
{
    return this->history_tbl;
}

uint32_t GPUdatabase::get_history_size()
{
    return this->history_tbl_size;
}

NEWORDER_ROW *GPUdatabase::get_neworder()
{
    return this->neworder_tbl;
}

uint32_t GPUdatabase::get_neworder_size()
{
    return this->neworder_tbl_size;
}

ORDER_ROW *GPUdatabase::get_order()
{
    return this->order_tbl;
}

uint32_t GPUdatabase::get_order_size()
{
    return this->order_tbl_size;
}

ORDERLINE_ROW *GPUdatabase::get_orderline()
{
    return this->orderline_tbl;
}

uint32_t GPUdatabase::get_orderline_size()
{
    return this->orderline_tbl_size;
}

STOCK_ROW *GPUdatabase::get_stock()
{
    return this->stock_tbl;
}

uint32_t GPUdatabase::get_stock_size()
{
    return this->stock_tbl_size;
}

ITEM_ROW *GPUdatabase::get_item()
{
    return this->item_tbl;
}

uint32_t GPUdatabase::get_item_size()
{
    return this->item_tbl_size;
}

uint32_t *GPUdatabase::get_customer_name_index()
{
    return this->customer_name_index;
}

uint32_t GPUdatabase::get_customer_name_index_size()
{
    return this->customer_tbl_size;
}

void GPUdatabase::print_warehouse()
{
    for (size_t i = 0; i < warehouse_tbl_size; i++)
    {
        std::cout << "warehouse_tbl:" << this->warehouse_tbl[i].W_ID << "->";
        std::cout << this->warehouse_tbl[i].W_TAX << "," << this->warehouse_tbl[i].W_YTD << ",";
        std::cout << this->warehouse_tbl[i].W_NAME << "," << this->warehouse_tbl[i].W_STREET_1 << ",";
        std::cout << this->warehouse_tbl[i].W_STREET_2 << "," << this->warehouse_tbl[i].W_CITY << ",";
        std::cout << this->warehouse_tbl[i].W_STATE << "," << this->warehouse_tbl[i].W_ZIP << std::endl;
    }

    // for (size_t i = 0; i < item_tbl_size; i++)
    // {
    //     std::cout << "item_tbl:" << item_tbl[i].I_ID << "->";
    //     std::cout << item_tbl[i].I_IM_ID << "," << item_tbl[i].I_PRICE << ",";
    //     std::cout << item_tbl[i].I_NAME << "," << item_tbl[i].I_DATA << std::endl;
    // }
}

void GPUdatabase::malloc_and_copy_snapshot(Snapshot *&snapshot, Snapshot *tmp,
                                           uint32_t device_ID)
{
    bool isExisted = false;
    uint32_t location = 0;
    for (size_t i = 0; i < this->device_cnt; i++)
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
        CHECK(cudaMalloc((void **)&tmp->warehouse, sizeof(WAREHOUSE_ROW) * this->warehouse_slice_size));
        CHECK(cudaMalloc((void **)&tmp->district, sizeof(DISTRICT_ROW) * this->district_slice_size));
        CHECK(cudaMalloc((void **)&tmp->customer, sizeof(CUSTOMER_ROW) * this->customer_slice_size));
        CHECK(cudaMalloc((void **)&tmp->history, sizeof(HISTORY_ROW) * this->history_slice_size));
        CHECK(cudaMalloc((void **)&tmp->neworder, sizeof(NEWORDER_ROW) * this->neworder_slice_size));
        CHECK(cudaMalloc((void **)&tmp->order, sizeof(ORDER_ROW) * this->order_slice_size));
        CHECK(cudaMalloc((void **)&tmp->orderline, sizeof(ORDERLINE_ROW) * this->orderline_slice_size));
        CHECK(cudaMalloc((void **)&tmp->stock, sizeof(STOCK_ROW) * this->stock_slice_size));
        CHECK(cudaMalloc((void **)&tmp->item, sizeof(ITEM_ROW) * this->item_slice_size));
        CHECK(cudaMalloc((void **)&tmp->customer_name_index, sizeof(uint32_t) * this->customer_slice_size));

        uint32_t start_offset = location * this->warehouse_slice_size;
        CHECK(cudaMemcpy(tmp->warehouse, this->warehouse_tbl + start_offset, sizeof(WAREHOUSE_ROW) * this->warehouse_slice_size, cudaMemcpyHostToDevice));
        start_offset = location * this->district_slice_size;
        CHECK(cudaMemcpy(tmp->district, this->district_tbl + start_offset, sizeof(DISTRICT_ROW) * this->district_slice_size, cudaMemcpyHostToDevice));
        start_offset = location * this->customer_slice_size;
        CHECK(cudaMemcpy(tmp->customer, this->customer_tbl + start_offset, sizeof(CUSTOMER_ROW) * this->customer_slice_size, cudaMemcpyHostToDevice));
        start_offset = location * this->history_slice_size;
        CHECK(cudaMemcpy(tmp->history, this->history_tbl + start_offset, sizeof(HISTORY_ROW) * this->history_slice_size, cudaMemcpyHostToDevice));
        start_offset = location * this->neworder_slice_size;
        CHECK(cudaMemcpy(tmp->neworder, this->neworder_tbl + start_offset, sizeof(NEWORDER_ROW) * this->neworder_slice_size, cudaMemcpyHostToDevice));
        start_offset = location * this->order_slice_size;
        CHECK(cudaMemcpy(tmp->order, this->order_tbl + start_offset, sizeof(ORDER_ROW) * this->order_slice_size, cudaMemcpyHostToDevice));
        start_offset = location * this->orderline_slice_size;
        CHECK(cudaMemcpy(tmp->orderline, this->orderline_tbl + start_offset, sizeof(ORDERLINE_ROW) * this->orderline_slice_size, cudaMemcpyHostToDevice));
        start_offset = location * this->stock_slice_size;
        CHECK(cudaMemcpy(tmp->stock, this->stock_tbl + start_offset, sizeof(STOCK_ROW) * this->stock_slice_size, cudaMemcpyHostToDevice));
        CHECK(cudaMemcpy(tmp->item, this->item_tbl, sizeof(ITEM_ROW) * this->item_slice_size, cudaMemcpyHostToDevice));
        start_offset = location * this->customer_slice_size;
        CHECK(cudaMemcpy(tmp->customer_name_index, this->customer_name_index + start_offset, sizeof(uint32_t) * this->customer_slice_size, cudaMemcpyHostToDevice));

        // std::cout << "device_ID:" << device_ID << " location:" << location << std::endl;
        // std::cout << "tmp->warehouse:" << tmp->warehouse << std::endl;
        // std::cout << "tmp:" << tmp << std::endl;

        CHECK(cudaMalloc((void **)&snapshot, sizeof(Snapshot)));
        CHECK(cudaMemcpy(snapshot, tmp, sizeof(Snapshot), cudaMemcpyHostToDevice));
        // std::cout << "Snapshot:" << snapshot << std::endl;
    }
}

void GPUdatabase::initialize_snapshot()
{
    malloc_and_copy_snapshot(this->snapshot_0, this->tmp + 0, 0);
    malloc_and_copy_snapshot(this->snapshot_1, this->tmp + 1, 1);
    malloc_and_copy_snapshot(this->snapshot_2, this->tmp + 2, 2);
    malloc_and_copy_snapshot(this->snapshot_3, this->tmp + 3, 3);
    malloc_and_copy_snapshot(this->snapshot_4, this->tmp + 4, 4);
    malloc_and_copy_snapshot(this->snapshot_5, this->tmp + 5, 5);
    malloc_and_copy_snapshot(this->snapshot_6, this->tmp + 6, 6);
    malloc_and_copy_snapshot(this->snapshot_7, this->tmp + 7, 7);
}

void GPUdatabase::free_snapshot(Snapshot *&snapshot, Snapshot *tmp,
                                uint32_t device_ID)
{
    bool isExisted = false;
    for (size_t i = 0; i < this->device_cnt; i++)
    {
        if (device_ID == this->device_IDs[i])
        {
            isExisted = true;
        }
    }
    if (isExisted)
    {
        CHECK(cudaSetDevice(device_ID));
        CHECK(cudaFree(tmp->warehouse));
        CHECK(cudaFree(tmp->district));
        CHECK(cudaFree(tmp->customer));
        CHECK(cudaFree(tmp->history));
        CHECK(cudaFree(tmp->neworder));
        CHECK(cudaFree(tmp->order));
        CHECK(cudaFree(tmp->orderline));
        CHECK(cudaFree(tmp->stock));
        CHECK(cudaFree(tmp->item));
        CHECK(cudaFree(tmp->customer_name_index));
        CHECK(cudaFree(snapshot));
    }
}

void GPUdatabase::release_snapshot()
{
    free_snapshot(this->snapshot_0, this->tmp + 0, 0);
    free_snapshot(this->snapshot_1, this->tmp + 1, 1);
    free_snapshot(this->snapshot_2, this->tmp + 2, 2);
    free_snapshot(this->snapshot_3, this->tmp + 3, 3);
    free_snapshot(this->snapshot_4, this->tmp + 4, 4);
    free_snapshot(this->snapshot_5, this->tmp + 5, 5);
    free_snapshot(this->snapshot_6, this->tmp + 6, 6);
    free_snapshot(this->snapshot_7, this->tmp + 7, 7);
}

Snapshot *GPUdatabase::get_snapshot(uint32_t device_ID)
{
    switch (device_ID)
    {
    case 0:
        return this->snapshot_0;
    case 1:
        return this->snapshot_1;
    case 2:
        return this->snapshot_2;
    case 3:
        return this->snapshot_3;
    case 4:
        return this->snapshot_4;
    case 5:
        return this->snapshot_5;
    case 6:
        return this->snapshot_6;
    case 7:
        return this->snapshot_7;
    default:
        return nullptr;
    }
}

uint32_t *GPUdatabase::get_device_IDs()
{
    return this->device_IDs;
}

uint32_t GPUdatabase::get_device_cnt()
{
    return this->device_cnt;
}

void GPUdatabase::print()
{
    for (size_t i = 0; i < this->warehouse_tbl_size; i++)
    {
        std::cout << "warehouse_tbl:" << warehouse_tbl[i].W_ID << ",";
        std::cout << warehouse_tbl[i].W_TAX << "," << warehouse_tbl[i].W_YTD << ",";
        std::cout << std::endl;
    }
}

__global__ void testDatabase(Snapshot *snapshot, uint32_t device_ID)
{
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    printf("deviceID:%d,thID:%d,W_ID:0x%d\n", device_ID, thID, snapshot->warehouse[thID].W_ID);
    // printf("deviceID:%d,thID:%d\n", device_ID, thID);
}

__global__ void testDatabase1(WAREHOUSE_ROW *warehouse, uint32_t device_ID)
{
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    printf("deviceID:%d,thID:%d,W_ID:0x%d\n", device_ID, thID, warehouse[thID].W_ID);
    // printf("deviceID:%d,thID:%d\n", device_ID, thID);
}

void launchDatabaseKernel()
{
    // uint32_t *device_IDs = gpudatabase->get_device_IDs();
    // uint32_t device_cnt = gpudatabase->get_device_cnt();
    // for (size_t i = 0; i < device_cnt; i++)
    // {
    //     CHECK(cudaSetDevice(device_IDs[i]));
    //     testDatabase<<<1, 4>>>(gpudatabase->get_snapshot(device_IDs[i]), device_IDs[i]);
    //     CHECK(cudaDeviceSynchronize());
    //     // testDatabase1<<<1, 1>>>(gpudatabase->tmp[device_IDs[i]].warehouse, device_IDs[i]);
    //     // CHECK(cudaDeviceSynchronize());
    // }
    // // gpudatabase->release_snapshot();
}

Snapshot *get_snapshot(uint32_t device_ID)
{
    return gpudatabase->get_snapshot(device_ID);
}