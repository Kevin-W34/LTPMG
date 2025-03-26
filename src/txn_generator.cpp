#include "../include/txn_generator.h"

template<typename T>
void Txn_Generator<T>::initialize(std::shared_ptr<Param> param) {
    if (param->data_distribution == "zipf") {
        random.init_rand_zipf(param->ycsb_size, param->zipf_config);
    }
}

template<>
void Txn_Generator<YCSB_A_Query>::initialize(std::shared_ptr<Param> param) {
    if (param->data_distribution == "zipf") {
        random.init_rand_zipf(param->ycsb_size, param->zipf_config);
    }
}

template<>
void Txn_Generator<YCSB_B_Query>::initialize(std::shared_ptr<Param> param) {
    if (param->data_distribution == "zipf") {
        random.init_rand_zipf(param->ycsb_size, param->zipf_config);
    }
}

template<>
void Txn_Generator<YCSB_C_Query>::initialize(std::shared_ptr<Param> param) {
    if (param->data_distribution == "zipf") {
        random.init_rand_zipf(param->ycsb_size, param->zipf_config);
    }
}

template<>
void Txn_Generator<YCSB_D_Query>::initialize(std::shared_ptr<Param> param) {
    if (param->data_distribution == "zipf") {
        random.init_rand_zipf(param->ycsb_size, param->zipf_config);
    }
}

template<>
void Txn_Generator<YCSB_E_Query>::initialize(std::shared_ptr<Param> param) {
    if (param->data_distribution == "zipf") {
        random.init_rand_zipf(param->ycsb_size, param->zipf_config);
    }
}

template<>
void Txn_Generator<Test_Query>::generate_txn(std::shared_ptr<Param> param, size_t size) {
    transactions.clear();
#ifndef LTPMG_TXN_GENERATOR_M

    for (int i = 0; i < size; ++i)
    {
        Test_Query new_query;
        new_query.TID = i;
        new_query.Row_0 = random.rand_test(0, 3);   // select
        new_query.Row_1 = random.rand_test(4, 6);   // insert
        new_query.Row_2 = random.rand_test(7, 10);  // update
        new_query.Row_3 = random.rand_test(11, 13); // scan
        new_query.Row_4 = random.rand_test(14, 17); // scan
        new_query.Row_5 = random.rand_test(18, 20); // delete
        transactions.emplace_back(new_query);
    }
    LOG(INFO) << "transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated Test_Query transactions.";
#endif

#ifdef LTPMG_TXN_GENERATOR_M
    size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数

    size_t batch_size = size / num_threads;
    std::vector<std::thread> threads;
    Test_Query **local_transactions;
    local_transactions = new Test_Query *[num_threads];

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        local_transactions[t] = new Test_Query[count];
    }

    auto generate_batch = [this](std::shared_ptr<Param> param, size_t startID, size_t start, size_t count,
                                 Test_Query *local_transactions) {
        for (size_t i = 0; i < count; ++i) {
            // printf("%d,%d\n", startID, start + i);
            Test_Query new_query;
            new_query.Row_0 = random.rand_test(0, 3); // select table 1
            new_query.Row_1 = random.rand_test(4, 6); // insert table 1
            new_query.Row_2 = random.rand_test(7, 10); // update table 1
            new_query.Row_3 = random.rand_test(11, 14); // delete table 1
            new_query.Row_4 = random.rand_test(15, 15); // scan table 1
            new_query.Row_5 = random.rand_test(20, 20); // scan table 1
            local_transactions[i] = new_query;
        }
    };

    // random.init_rand_zipf(param->test_1_size, 0.1);
    // random.init_rand_zipf(param->test_1_size, 0.2);
    // random.init_rand_zipf(param->test_1_size, 0.3);
    // random.init_rand_zipf(param->test_1_size, 0.4);
    // random.init_rand_zipf(param->test_1_size, 0.5);
    // random.init_rand_zipf(param->test_1_size, 0.6);
    // random.init_rand_zipf(param->test_1_size, 0.7);
    // random.init_rand_zipf(param->test_1_size, 0.8);
    // random.init_rand_zipf(param->test_1_size, 0.9);
    // uint32_t random_size = 100;
    // random.init_rand_zipf(random_size, 0.1);
    // random.init_rand_zipf(random_size, 1.0);
    // random.init_rand_zipf(random_size, 5.0);


    // std::cout << "rand_zipf:";
    // for (size_t t = 0; t < 20; ++t) {
    //     uint32_t result = random.rand_zipf(0, random_size);
    //     std::cout << result << " ";
    // }
    // std::cout << std::endl;

    // std::string clast = random.rand_C_LAST();
    // std::cout << "clast: " << clast << std::endl;
    // for (uint32_t i = 0; i < clast.size(); ++i) {
    //     std::cout << clast[i];
    // }
    // std::cout << std::endl;
    // uint32_t value = 0;
    //
    // // 对于每个字符，将其 ASCII 值移到正确的位置并累加到 value
    // for (size_t j = 0; j < 4; ++j) {
    //     // 将 ASCII 值左移以保存在正确的位置
    //     value |= static_cast<uint32_t>(clast[j]) << (24 - j * 8);
    // }
    // std::cout << value << std::endl;


    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        threads.emplace_back(std::bind(generate_batch, param, t, start, count, local_transactions[t]));
    }

    for (auto &t: threads) {
        t.join();
    }

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        transactions.insert(transactions.end(), local_transactions[t], local_transactions[t] + count);
        delete[] local_transactions[t];
    }
    delete[] local_transactions;

#endif

    LOG(INFO) << "Test_Query transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated Test_Query transactions.";
}

template<>
void Txn_Generator<Test_Query_2>::generate_txn(std::shared_ptr<Param> param, size_t size) {
    transactions.clear();
#ifndef LTPMG_TXN_GENERATOR_M

    for (int i = 0; i < size; ++i)
    {
        Test_Query_2 new_query;
        new_query.TID = i;
        new_query.Row_0 = random.rand_test(0, 1); // select
        new_query.Row_1 = random.rand_test(2, 3); // select
        new_query.Row_2 = random.rand_test(4, 5); // select
        new_query.Row_3 = random.rand_test(6, 7); // select
        new_query.Row_4 = random.rand_test(6, 7); // select
        transactions.emplace_back(new_query);
    }
    LOG(INFO) << "transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated Test_Query_2 transactions.";
#endif

#ifdef LTPMG_TXN_GENERATOR_M
    size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数

    size_t batch_size = size / num_threads;
    std::vector<std::thread> threads;
    Test_Query_2 **local_transactions;
    local_transactions = new Test_Query_2 *[num_threads];

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        local_transactions[t] = new Test_Query_2[count];
    }

    auto generate_batch = [this](std::shared_ptr<Param> param, size_t startID, size_t start, size_t count,
                                 Test_Query_2 *local_transactions) {
        for (size_t i = 0; i < count; ++i) {
            // printf("%d,%d\n", startID, start + i);
            Test_Query_2 new_query;

            new_query.Row_0 = 0; //random.rand_test(0, 0); // select table 0
            new_query.Row_1 = 1; //random.rand_test(2, 3); // select table 0
            new_query.Row_2 = 2; //random.rand_test(4, 5); // select table 0
            new_query.Row_3 = 3; //random.rand_test(6, 7); // select table 0
            new_query.Row_4 = 4; //random.rand_test(6, 7); // select table 0
            local_transactions[i] = new_query;
        }
    };

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        threads.emplace_back(std::bind(generate_batch, param, t, start, count, local_transactions[t]));
    }

    for (auto &t: threads) {
        t.join();
    }

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        transactions.insert(transactions.end(), local_transactions[t], local_transactions[t] + count);
        delete[] local_transactions[t];
    }
    delete[] local_transactions;

#endif

    LOG(INFO) << "Test_Query_2 transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated Test_Query_2 transactions.";
}

template<>
void Txn_Generator<Neworder_Query>::generate_txn(std::shared_ptr<Param> param, size_t size) {
    transactions.clear();
#ifndef LTPMG_TXN_GENERATOR_M
    for (int i = 0; i < size; ++i)
    {
        Neworder_Query new_query;
        new_query.W_ID = (UINT32)i * 2;
        new_query.D_ID = (UINT32)i * 3;
        new_query.C_ID = (UINT32)i * 4;

        transactions.emplace_back(new_query);
    }
#endif
#ifdef LTPMG_TXN_GENERATOR_M
    size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数

    size_t batch_size = size / num_threads;
    std::vector<std::thread> threads;
    Neworder_Query **local_transactions;
    local_transactions = new Neworder_Query *[num_threads];

    auto generate_batch = [this](size_t start, size_t count, std::shared_ptr<Param> param,
                                 Neworder_Query *local_transactions) {
        Neworder_Query new_query;
        for (size_t i = 0; i < count; ++i) {
            new_query.W_ID = random.uniform_dist(0, param->warehouse_size - 1);
            new_query.D_ID = random.uniform_dist(0, 9);
            new_query.C_ID = random.uniform_dist(0, 2999);
            new_query.O_ID = random.uniform_dist(0, 2999);
            new_query.N_O_ID = random.uniform_dist(0, 2999);
            // new_query.O_OL_CNT = random.uniform_dist(5, 15);
            new_query.O_OL_CNT = 15;
            new_query.O_OL_ID = random.uniform_dist(0, 450000 - 15);
            for (size_t i = 0; i < new_query.O_OL_CNT; ++i) {
                new_query.INFO[i].OL_I_ID = random.uniform_dist(0, 99999);
                new_query.INFO[i].OL_QUANTITY = 1;
                uint32_t isRemote = random.uniform_dist(0, 99);
                if (isRemote < 60) {
                    new_query.INFO[i].OL_SUPPLY_W_ID = new_query.W_ID;
                } else {
                    new_query.INFO[i].OL_SUPPLY_W_ID = random.uniform_dist(0, param->warehouse_size - 1);
                    while (new_query.INFO[i].OL_SUPPLY_W_ID == new_query.W_ID) {
                        new_query.INFO[i].OL_SUPPLY_W_ID = random.uniform_dist(0, param->warehouse_size - 1);
                    }
                }
            }
            local_transactions[i] = new_query;
        }
    };
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        local_transactions[t] = new Neworder_Query[count];
    }

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        threads.emplace_back(std::bind(generate_batch, start, count, param, local_transactions[t]));
    }
    for (auto &t: threads) {
        t.join();
    }
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        transactions.insert(transactions.end(), local_transactions[t], local_transactions[t] + count);
        delete[] local_transactions[t];
    }
    delete[] local_transactions;

#endif
    LOG(INFO) << "Neworder_Query transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated Neworder_Query transactions.";
}

template<>
void Txn_Generator<Payment_Query>::generate_txn(std::shared_ptr<Param> param, size_t size) {
    transactions.clear();
#ifndef LTPMG_TXN_GENERATOR_M
    for (int i = 0; i < size; ++i)
    {
        Neworder_Query new_query;
        new_query.W_ID = (UINT32)i * 2;
        new_query.D_ID = (UINT32)i * 3;
        new_query.C_ID = (UINT32)i * 4;

        transactions.emplace_back(new_query);
    }
#endif
#ifdef LTPMG_TXN_GENERATOR_M
    size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数

    size_t batch_size = size / num_threads;
    std::vector<std::thread> threads;
    Payment_Query **local_transactions;
    local_transactions = new Payment_Query *[num_threads];

    auto generate_batch = [this](size_t start, size_t count, std::shared_ptr<Param> param,
                                 Payment_Query *local_transactions) {
        Payment_Query new_query;
        for (size_t i = 0; i < count; ++i) {
            new_query.W_ID = random.uniform_dist(0, param->warehouse_size - 1);
            new_query.D_ID = random.uniform_dist(0, 9);
            new_query.C_ID = random.uniform_dist(0, 2999);
            new_query.H_AMOUNT = 10;
            new_query.H_ID = random.uniform_dist(0, 2999);
            new_query.C_D_ID = new_query.D_ID;
            new_query.C_W_ID = new_query.W_ID;
            std::string clast = random.rand_C_LAST();
            uint32_t value = 0;
            for (size_t j = 0; j < 4; ++j) {
                value |= static_cast<uint32_t>(clast[j]) << (24 - j * 8);
            }
            new_query.C_LAST = value;
            uint32_t isName = random.uniform_dist(0, 99);
            if (isName < 85) {
                new_query.isName = 0;
            } else {
                new_query.isName = 1;
            }

            local_transactions[i] = new_query;
        }
    };
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        local_transactions[t] = new Payment_Query[count];
    }

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        threads.emplace_back(std::bind(generate_batch, start, count, param, local_transactions[t]));
    }
    for (auto &t: threads) {
        t.join();
    }
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        transactions.insert(transactions.end(), local_transactions[t], local_transactions[t] + count);
        delete[] local_transactions[t];
    }
    delete[] local_transactions;

#endif
    LOG(INFO) << "Payment_Query transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated Payment_Query transactions.";
}

template<>
void Txn_Generator<Orderstatus_Query>::generate_txn(std::shared_ptr<Param> param, size_t size) {
    transactions.clear();
#ifndef LTPMG_TXN_GENERATOR_M
    for (int i = 0; i < size; ++i)
    {
        Neworder_Query new_query;
        new_query.W_ID = (UINT32)i * 2;
        new_query.D_ID = (UINT32)i * 3;
        new_query.C_ID = (UINT32)i * 4;

        transactions.emplace_back(new_query);
    }
#endif
#ifdef LTPMG_TXN_GENERATOR_M
    size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数

    size_t batch_size = size / num_threads;
    std::vector<std::thread> threads;
    Orderstatus_Query **local_transactions;
    local_transactions = new Orderstatus_Query *[num_threads];

    auto generate_batch = [this](size_t start, size_t count, std::shared_ptr<Param> param,
                                 Orderstatus_Query *local_transactions) {
        Orderstatus_Query new_query;
        for (size_t i = 0; i < count; ++i) {
            new_query.W_ID = random.uniform_dist(0, param->warehouse_size - 1);
            new_query.D_ID = random.uniform_dist(0, 9);
            new_query.C_ID = random.uniform_dist(0, 2999);
            std::string clast = random.rand_C_LAST();
            uint32_t value = 0;
            for (size_t j = 0; j < 4; ++j) {
                value |= static_cast<uint32_t>(clast[j]) << (24 - j * 8);
            }
            new_query.C_LAST = value;
            uint32_t isName = random.uniform_dist(0, 99);
            if (isName < 85) {
                new_query.isName = 0;
            } else {
                new_query.isName = 1;
            }
            new_query.O_ID = random.uniform_dist(0, 2999);
            new_query.OL_ID = random.uniform_dist(0, 44999);
            local_transactions[i] = new_query;
        }
    };
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        local_transactions[t] = new Orderstatus_Query[count];
    }

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        threads.emplace_back(std::bind(generate_batch, start, count, param, local_transactions[t]));
    }
    for (auto &t: threads) {
        t.join();
    }
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        transactions.insert(transactions.end(), local_transactions[t], local_transactions[t] + count);
        delete[] local_transactions[t];
    }
    delete[] local_transactions;

#endif
    LOG(INFO) << "Orderstatus_Query transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated Orderstatus_Query transactions.";
}

template<>
void Txn_Generator<Delivery_Query>::generate_txn(std::shared_ptr<Param> param, size_t size) {
    transactions.clear();
#ifndef LTPMG_TXN_GENERATOR_M
    for (int i = 0; i < size; ++i)
    {
        Neworder_Query new_query;
        new_query.W_ID = (UINT32)i * 2;
        new_query.D_ID = (UINT32)i * 3;
        new_query.C_ID = (UINT32)i * 4;

        transactions.emplace_back(new_query);
    }
#endif
#ifdef LTPMG_TXN_GENERATOR_M
    size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数

    size_t batch_size = size / num_threads;
    std::vector<std::thread> threads;
    Delivery_Query **local_transactions;
    local_transactions = new Delivery_Query *[num_threads];

    auto generate_batch = [this](size_t start, size_t count, std::shared_ptr<Param> param,
                                 Delivery_Query *local_transactions) {
        Delivery_Query new_query;
        for (size_t i = 0; i < count; ++i) {
            std::unordered_set<uint32_t> rowSet;
            for (size_t j = 0; j < 10; ++j) {
                new_query.NO_O_ID[j] = random.uniform_dist(0, 2999);
                while (rowSet.find(new_query.NO_O_ID[j]) != rowSet.end()) {
                    new_query.NO_O_ID[j] = random.uniform_dist(0, 2999);
                }
                rowSet.insert(new_query.NO_O_ID[j]);
            }
            for (size_t j = 0; j < 10; ++j) {
                new_query.NO_W_ID[j] = random.uniform_dist(0, param->warehouse_size - 1);
                new_query.NO_D_ID[j] = random.uniform_dist(0, 9);
                new_query.NO_C_ID[j] = random.uniform_dist(0, 2999);
            }
            local_transactions[i] = new_query;
        }
    };
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        local_transactions[t] = new Delivery_Query[count];
    }

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        threads.emplace_back(std::bind(generate_batch, start, count, param, local_transactions[t]));
    }
    for (auto &t: threads) {
        t.join();
    }
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        transactions.insert(transactions.end(), local_transactions[t], local_transactions[t] + count);
        delete[] local_transactions[t];
    }
    delete[] local_transactions;

#endif
    LOG(INFO) << "Delivery_Query transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated Delivery_Query transactions.";
}

template<>
void Txn_Generator<Stocklevel_Query>::generate_txn(std::shared_ptr<Param> param, size_t size) {
    transactions.clear();
#ifndef LTPMG_TXN_GENERATOR_M
    for (int i = 0; i < size; ++i)
    {
        Neworder_Query new_query;
        new_query.W_ID = (UINT32)i * 2;
        new_query.D_ID = (UINT32)i * 3;
        new_query.C_ID = (UINT32)i * 4;

        transactions.emplace_back(new_query);
    }
#endif
#ifdef LTPMG_TXN_GENERATOR_M
    size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数

    size_t batch_size = size / num_threads;
    std::vector<std::thread> threads;
    Stocklevel_Query **local_transactions;
    local_transactions = new Stocklevel_Query *[num_threads];

    auto generate_batch = [this](size_t start, size_t count, std::shared_ptr<Param> param,
                                 Stocklevel_Query *local_transactions) {
        Stocklevel_Query new_query;
        for (size_t i = 0; i < count; ++i) {
            new_query.W_ID = random.uniform_dist(0, param->warehouse_size - 1);
            new_query.D_ID = random.uniform_dist(0, 9);
            // new_query.query_cnt = random.uniform_dist(5, 10);
            std::unordered_set<int> rowSet;
            for (uint32_t j = 0; j < 10; ++j) {
                new_query.I_ID[j] = random.uniform_dist(0, 99999);
                while (rowSet.find(new_query.I_ID[j]) != rowSet.end()) {
                    new_query.I_ID[j] = random.uniform_dist(0, 99999);
                }
                rowSet.insert(new_query.I_ID[j]);
            }
            rowSet.clear();
            for (uint32_t j = 0; j < 10; ++j) {
                new_query.O_OL_ID[j] = random.uniform_dist(0, 44999);
                while (rowSet.find(new_query.O_OL_ID[j]) != rowSet.end()) {
                    new_query.O_OL_ID[j] = random.uniform_dist(0, 44999);
                }
                rowSet.insert(new_query.O_OL_ID[j]);
            }

            local_transactions[i] = new_query;
        }
    };
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        local_transactions[t] = new Stocklevel_Query[count];
    }

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        threads.emplace_back(std::bind(generate_batch, start, count, param, local_transactions[t]));
    }
    for (auto &t: threads) {
        t.join();
    }
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        transactions.insert(transactions.end(), local_transactions[t], local_transactions[t] + count);
        delete[] local_transactions[t];
    }
    delete[] local_transactions;

#endif
    LOG(INFO) << "Stocklevel_Query transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated Stocklevel_Query transactions.";
}

template<>
void Txn_Generator<YCSB_A_Query>::generate_txn(std::shared_ptr<Param> param, size_t size) {
    transactions.clear();
#ifndef LTPMG_TXN_GENERATOR_M
    for (int i = 0; i < size; ++i)
    {
        Neworder_Query new_query;
        new_query.W_ID = (UINT32)i * 2;
        new_query.D_ID = (UINT32)i * 3;
        new_query.C_ID = (UINT32)i * 4;

        transactions.emplace_back(new_query);
    }
#endif
#ifdef LTPMG_TXN_GENERATOR_M
    size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数

    size_t batch_size = size / num_threads;
    std::vector<std::thread> threads;
    YCSB_A_Query **local_transactions;
    local_transactions = new YCSB_A_Query *[num_threads];

    auto generate_batch = [this](size_t start, size_t count, std::shared_ptr<Param> param,
                                 YCSB_A_Query *local_transactions) {
        YCSB_A_Query new_query;
        for (size_t i = 0; i < count; ++i) {
            std::unordered_set<int> rowSet;

            for (size_t j = 0; j < 10; ++j) {
                if (param->data_distribution == "unif") {
                    new_query.ROW_ID[j] = random.uniform_dist(0, param->ycsb_size - 1);
                    while (rowSet.find(new_query.ROW_ID[j]) != rowSet.end()) {
                        new_query.ROW_ID[j] = random.uniform_dist(0, param->ycsb_size - 1);
                    }
                    rowSet.insert(new_query.ROW_ID[j]);
                } else if (param->data_distribution == "zipf") {
                    new_query.ROW_ID[j] = random.rand_zipf(0, param->ycsb_size - 1);
                    while (rowSet.find(new_query.ROW_ID[j]) != rowSet.end()) {
                        new_query.ROW_ID[j] = random.rand_zipf(0, param->ycsb_size - 1);
                    }
                    rowSet.insert(new_query.ROW_ID[j]);
                }

                local_transactions[i] = new_query;
            }
        }
    };
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        local_transactions[t] = new YCSB_A_Query[count];
    }

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        threads.emplace_back(std::bind(generate_batch, start, count, param, local_transactions[t]));
    }
    for (auto &t: threads) {
        t.join();
    }
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        transactions.insert(transactions.end(), local_transactions[t], local_transactions[t] + count);
        delete[] local_transactions[t];
    }
    delete[] local_transactions;

#endif
    LOG(INFO) << "YCSB_A_Query transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated YCSB_A_Query transactions.";
}

template<>
void Txn_Generator<YCSB_B_Query>::generate_txn(std::shared_ptr<Param> param, size_t size) {
    transactions.clear();
#ifndef LTPMG_TXN_GENERATOR_M
    for (int i = 0; i < size; ++i)
    {
        Neworder_Query new_query;
        new_query.W_ID = (UINT32)i * 2;
        new_query.D_ID = (UINT32)i * 3;
        new_query.C_ID = (UINT32)i * 4;

        transactions.emplace_back(new_query);
    }
#endif
#ifdef LTPMG_TXN_GENERATOR_M
    size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数

    size_t batch_size = size / num_threads;
    std::vector<std::thread> threads;
    YCSB_B_Query **local_transactions;
    local_transactions = new YCSB_B_Query *[num_threads];

    auto generate_batch = [this](size_t start, size_t count, std::shared_ptr<Param> param,
                                 YCSB_B_Query *local_transactions) {
        YCSB_B_Query new_query;
        for (size_t i = 0; i < count; ++i) {
            std::unordered_set<int> rowSet;

            for (size_t j = 0; j < 10; ++j) {
                if (param->data_distribution == "unif") {
                    new_query.ROW_ID[j] = random.uniform_dist(0, param->ycsb_size - 1);
                    while (rowSet.find(new_query.ROW_ID[j]) != rowSet.end()) {
                        new_query.ROW_ID[j] = random.uniform_dist(0, param->ycsb_size - 1);
                    }
                    rowSet.insert(new_query.ROW_ID[j]);
                } else if (param->data_distribution == "zipf") {
                    new_query.ROW_ID[j] = random.rand_zipf(0, param->ycsb_size - 1);
                    while (rowSet.find(new_query.ROW_ID[j]) != rowSet.end()) {
                        new_query.ROW_ID[j] = random.rand_zipf(0, param->ycsb_size - 1);
                    }
                    rowSet.insert(new_query.ROW_ID[j]);
                }

                local_transactions[i] = new_query;
            }
        }
    };
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        local_transactions[t] = new YCSB_B_Query[count];
    }

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        threads.emplace_back(std::bind(generate_batch, start, count, param, local_transactions[t]));
    }
    for (auto &t: threads) {
        t.join();
    }
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        transactions.insert(transactions.end(), local_transactions[t], local_transactions[t] + count);
        delete[] local_transactions[t];
    }
    delete[] local_transactions;

#endif
    LOG(INFO) << "YCSB_B_Query transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated YCSB_B_Query transactions.";
}

template<>
void Txn_Generator<YCSB_C_Query>::generate_txn(std::shared_ptr<Param> param, size_t size) {
    transactions.clear();
#ifndef LTPMG_TXN_GENERATOR_M
    for (int i = 0; i < size; ++i)
    {
        Neworder_Query new_query;
        new_query.W_ID = (UINT32)i * 2;
        new_query.D_ID = (UINT32)i * 3;
        new_query.C_ID = (UINT32)i * 4;

        transactions.emplace_back(new_query);
    }
#endif
#ifdef LTPMG_TXN_GENERATOR_M
    size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数

    size_t batch_size = size / num_threads;
    std::vector<std::thread> threads;
    YCSB_C_Query **local_transactions;
    local_transactions = new YCSB_C_Query *[num_threads];

    auto generate_batch = [this](size_t start, size_t count, std::shared_ptr<Param> param,
                                 YCSB_C_Query *local_transactions) {
        YCSB_C_Query new_query;
        for (size_t i = 0; i < count; ++i) {
            std::unordered_set<int> rowSet;

            for (size_t j = 0; j < 10; ++j) {
                if (param->data_distribution == "unif") {
                    new_query.ROW_ID[j] = random.uniform_dist(0, param->ycsb_size - 1);
                    while (rowSet.find(new_query.ROW_ID[j]) != rowSet.end()) {
                        new_query.ROW_ID[j] = random.uniform_dist(0, param->ycsb_size - 1);
                    }
                    rowSet.insert(new_query.ROW_ID[j]);
                } else if (param->data_distribution == "zipf") {
                    new_query.ROW_ID[j] = random.rand_zipf(0, param->ycsb_size - 1);
                    while (rowSet.find(new_query.ROW_ID[j]) != rowSet.end()) {
                        new_query.ROW_ID[j] = random.rand_zipf(0, param->ycsb_size - 1);
                    }
                    rowSet.insert(new_query.ROW_ID[j]);
                    // uint32_t tmp = random.uniform_dist(0, 99);
                    // uint32_t config = param->zipf_config * 5;
                    // if (tmp < config) {
                    //     new_query.ROW_ID[j] = random.uniform_dist(0, 100);
                    // } else {
                    //     new_query.ROW_ID[j] = random.uniform_dist(100, param->ycsb_size - 1);
                    // }
                    // while (rowSet.find(new_query.ROW_ID[j]) != rowSet.end()) {
                    //     if (tmp < config) {
                    //         new_query.ROW_ID[j] = random.uniform_dist(0, 100);
                    //     } else {
                    //         new_query.ROW_ID[j] = random.uniform_dist(100, param->ycsb_size - 1);
                    //     }
                    // }
                    // rowSet.insert(new_query.ROW_ID[j]);
                }

                local_transactions[i] = new_query;
            }
        }
    };

    // random.init_rand_zipf(100, 1.0);
    // std::cout << "rand_zipf:";
    // for (size_t t = 0; t < 20; ++t) {
    //     uint32_t result = random.rand_zipf(0, 100);
    //     std::cout << result << " ";
    // }
    // std::cout << std::endl;

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        local_transactions[t] = new YCSB_C_Query[count];
    }

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        threads.emplace_back(std::bind(generate_batch, start, count, param, local_transactions[t]));
    }
    for (auto &t: threads) {
        t.join();
    }
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        transactions.insert(transactions.end(), local_transactions[t], local_transactions[t] + count);
        delete[] local_transactions[t];
    }
    delete[] local_transactions;

#endif
    LOG(INFO) << "YCSB_C_Query transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated YCSB_C_Query transactions.";
}

template<>
void Txn_Generator<YCSB_D_Query>::generate_txn(std::shared_ptr<Param> param, size_t size) {
    transactions.clear();
#ifndef LTPMG_TXN_GENERATOR_M
    for (int i = 0; i < size; ++i)
    {
        Neworder_Query new_query;
        new_query.W_ID = (UINT32)i * 2;
        new_query.D_ID = (UINT32)i * 3;
        new_query.C_ID = (UINT32)i * 4;

        transactions.emplace_back(new_query);
    }
#endif
#ifdef LTPMG_TXN_GENERATOR_M
    size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数

    size_t batch_size = size / num_threads;
    std::vector<std::thread> threads;
    YCSB_D_Query **local_transactions;
    local_transactions = new YCSB_D_Query *[num_threads];

    auto generate_batch = [this](size_t start, size_t count, std::shared_ptr<Param> param,
                                 YCSB_D_Query *local_transactions) {
        YCSB_D_Query new_query;
        for (size_t i = 0; i < count; ++i) {
            std::unordered_set<int> rowSet;

            for (size_t j = 0; j < 10; ++j) {
                if (param->data_distribution == "unif") {
                    new_query.ROW_ID[j] = random.uniform_dist(0, param->ycsb_size - 1);
                    while (rowSet.find(new_query.ROW_ID[j]) != rowSet.end()) {
                        new_query.ROW_ID[j] = random.uniform_dist(0, param->ycsb_size - 1);
                    }
                    rowSet.insert(new_query.ROW_ID[j]);
                } else if (param->data_distribution == "zipf") {
                    new_query.ROW_ID[j] = random.rand_zipf(0, param->ycsb_size - 1);
                    while (rowSet.find(new_query.ROW_ID[j]) != rowSet.end()) {
                        new_query.ROW_ID[j] = random.rand_zipf(0, param->ycsb_size - 1);
                    }
                    rowSet.insert(new_query.ROW_ID[j]);
                }

                local_transactions[i] = new_query;
            }
        }
    };
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        local_transactions[t] = new YCSB_D_Query[count];
    }

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        threads.emplace_back(std::bind(generate_batch, start, count, param, local_transactions[t]));
    }
    for (auto &t: threads) {
        t.join();
    }
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        transactions.insert(transactions.end(), local_transactions[t], local_transactions[t] + count);
        delete[] local_transactions[t];
    }
    delete[] local_transactions;

#endif
    LOG(INFO) << "YCSB_D_Query transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated YCSB_D_Query transactions.";
}

template<>
void Txn_Generator<YCSB_E_Query>::generate_txn(std::shared_ptr<Param> param, size_t size) {
    transactions.clear();
#ifndef LTPMG_TXN_GENERATOR_M
    for (int i = 0; i < size; ++i)
    {
        Neworder_Query new_query;
        new_query.W_ID = (UINT32)i * 2;
        new_query.D_ID = (UINT32)i * 3;
        new_query.C_ID = (UINT32)i * 4;

        transactions.emplace_back(new_query);
    }
#endif
#ifdef LTPMG_TXN_GENERATOR_M
    size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数

    size_t batch_size = size / num_threads;
    std::vector<std::thread> threads;
    YCSB_E_Query **local_transactions;
    local_transactions = new YCSB_E_Query *[num_threads];

    auto generate_batch = [this](size_t start, size_t count, std::shared_ptr<Param> param,
                                 YCSB_E_Query *local_transactions) {
        YCSB_E_Query new_query;
        for (size_t i = 0; i < count; ++i) {
            std::unordered_set<int> rowSet;

            for (size_t j = 0; j < 10; ++j) {
                if (param->data_distribution == "unif") {
                    new_query.ROW_ID[j] = random.uniform_dist(0, param->ycsb_size - 21);
                    while (rowSet.find(new_query.ROW_ID[j]) != rowSet.end()) {
                        new_query.ROW_ID[j] = random.uniform_dist(0, param->ycsb_size - 21);
                    }
                    rowSet.insert(new_query.ROW_ID[j]);
                } else if (param->data_distribution == "zipf") {
                    new_query.ROW_ID[j] = random.rand_zipf(0, param->ycsb_size - 99);
                    while (rowSet.find(new_query.ROW_ID[j]) != rowSet.end()) {
                        new_query.ROW_ID[j] = random.rand_zipf(0, param->ycsb_size - 99);
                    }
                    rowSet.insert(new_query.ROW_ID[j]);
                }

                local_transactions[i] = new_query;
            }
        }
    };
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        local_transactions[t] = new YCSB_E_Query[count];
    }

    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        threads.emplace_back(std::bind(generate_batch, start, count, param, local_transactions[t]));
    }
    for (auto &t: threads) {
        t.join();
    }
    for (size_t t = 0; t < num_threads; ++t) {
        size_t start = t * batch_size;
        size_t count = (t == num_threads - 1) ? (size - start) : batch_size;
        transactions.insert(transactions.end(), local_transactions[t], local_transactions[t] + count);
        delete[] local_transactions[t];
    }
    delete[] local_transactions;

#endif
    LOG(INFO) << "YCSB_E_Query transactions.size()=" << transactions.size();
    LOG(INFO) << "Generated YCSB_E_Query transactions.";
}
