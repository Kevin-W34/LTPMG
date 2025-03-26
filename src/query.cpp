#include "../include/query.h"

Query::Query() {
}

Query::~Query() {
}

void Query::generate_txn(std::shared_ptr<Param> param) {
    if (param->benchmark == "TEST") {
        // test_query
        txn_generator_test_query.reset();
        txn_generator_test_query = std::make_shared<Txn_Generator<Test_Query> >(Txn_Generator<Test_Query>());

        txn_generator_test_query->generate_txn(param, param->test_query_batch_size);

        for (const auto &transaction_test: txn_generator_test_query->get_txn()) {
            transactions_batch.emplace_back(transaction_test);
        }

        // test_query_2
        txn_generator_test_query_2.reset();
        txn_generator_test_query_2 = std::make_shared<Txn_Generator<Test_Query_2> >(Txn_Generator<Test_Query_2>());

        txn_generator_test_query_2->generate_txn(param, param->test_query_2_batch_size);

        for (const auto &transaction_test: txn_generator_test_query_2->get_txn()) {
            transactions_batch.emplace_back(transaction_test);
        }

        std::default_random_engine rng(static_cast<unsigned int>(std::time(nullptr)));
        std::shuffle(transactions_batch.begin(), transactions_batch.end(), rng);
        uint32_t tid = 0;
        for (auto &transaction_test: transactions_batch) {
            if (transaction_test.type() == typeid(Test_Query)) {
                Test_Query new_query = std::any_cast<Test_Query>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            } else if (transaction_test.type() == typeid(Test_Query_2)) {
                Test_Query_2 new_query = std::any_cast<Test_Query_2>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            }
            tid+=1;
        }

        std::sort(transactions_batch.begin(), transactions_batch.end(),
                  [this](const std::any &a, const std::any &b) { return this->sortByType(a, b); });

        // std::cout << typeid(Test_Query) << " " << typeid(Test_Query_2) << std::endl;

#ifdef LTPMG_GPUQUERY_PRINT
        // for (auto &transaction_test: transactions_batch) {
        //     if (transaction_test.type() == typeid(Test_Query)) {
        //         std::cout << std::any_cast<Test_Query>(transaction_test).TID << " ";
        //         std::cout << std::any_cast<Test_Query>(transaction_test).Row_0 << " ";
        //         std::cout << std::any_cast<Test_Query>(transaction_test).Row_1 << " ";
        //         std::cout << std::any_cast<Test_Query>(transaction_test).Row_2 << " ";
        //         std::cout << std::any_cast<Test_Query>(transaction_test).Row_3 << " ";
        //         std::cout << std::any_cast<Test_Query>(transaction_test).Row_4 << " ";
        //         std::cout << std::any_cast<Test_Query>(transaction_test).Row_5 << " ";
        //         std::cout << std::endl;
        //     } else if (transaction_test.type() == typeid(Test_Query_2)) {
        //         std::cout << std::any_cast<Test_Query_2>(transaction_test).TID << " ";
        //         std::cout << std::any_cast<Test_Query_2>(transaction_test).Row_0 << " ";
        //         std::cout << std::any_cast<Test_Query_2>(transaction_test).Row_1 << " ";
        //         std::cout << std::any_cast<Test_Query_2>(transaction_test).Row_2 << " ";
        //         std::cout << std::any_cast<Test_Query_2>(transaction_test).Row_3 << " ";
        //         std::cout << std::any_cast<Test_Query_2>(transaction_test).Row_4 << " ";
        //         std::cout << std::endl;
        //     }
        // }
        // std::cout << std::endl;
#endif
        txn_generator_test_query.reset();

        txn_generator_test_query_2.reset();
    } else if (param->benchmark == "TPCC_PART") {
        txn_generator_neworder_query.reset();
        txn_generator_neworder_query = std::make_shared<Txn_Generator<
            Neworder_Query> >(Txn_Generator<Neworder_Query>());

        txn_generator_neworder_query->generate_txn(param, param->neworder_query_batch_size);

        for (const auto &transaction_query: txn_generator_neworder_query->get_txn()) {
            transactions_batch.emplace_back(transaction_query);
        }

        txn_generator_payment_query.reset();
        txn_generator_payment_query = std::make_shared<Txn_Generator<
            Payment_Query> >(Txn_Generator<Payment_Query>());

        txn_generator_payment_query->generate_txn(param, param->payment_query_batch_size);

        for (const auto &transaction_query: txn_generator_payment_query->get_txn()) {
            transactions_batch.emplace_back(transaction_query);
        }

        std::default_random_engine rng(static_cast<unsigned int>(std::time(nullptr)));
        std::shuffle(transactions_batch.begin(), transactions_batch.end(), rng);
        uint32_t tid = 0;
        for (auto &transaction_test: transactions_batch) {
            if (transaction_test.type() == typeid(Neworder_Query)) {
                Neworder_Query new_query = std::any_cast<Neworder_Query>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            } else if (transaction_test.type() == typeid(Payment_Query)) {
                Payment_Query new_query = std::any_cast<Payment_Query>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            }
            tid+=1;
        }

        std::sort(transactions_batch.begin(), transactions_batch.end(),
                  [this](const std::any &a, const std::any &b) { return this->sortByType(a, b); });

        // for (auto &transaction_test: transactions_batch) {
        //     if (transaction_test.type() == typeid(Neworder_Query)) {
        //         std::cout << typeid(Neworder_Query).name() << std::endl;
        //     }
        //     if (transaction_test.type() == typeid(Payment_Query)) {
        //         std::cout << typeid(Payment_Query).name() << std::endl;
        //     }
        // }

#ifdef LTPMG_GPUQUERY_PRINT
        // for (auto &transaction_test: transactions_batch) {
        //     if (transaction_test.type() == typeid(Neworder_Query)) {
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).TID << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).W_ID << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).D_ID << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).C_ID << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).O_ID << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).N_O_ID << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).O_OL_CNT << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).O_OL_ID << " ";
        //         std::cout << std::endl;
        //     } else if (transaction_test.type() == typeid(Payment_Query)) {
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).TID << " ";
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).W_ID << " ";
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).D_ID << " ";
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).isName << " ";
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).C_ID << " ";
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).C_LAST << " ";
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).H_AMOUNT << " ";
        //         std::cout << std::endl;
        //     }
        // }
        // std::cout << std::endl;
#endif
        txn_generator_neworder_query.reset();

        txn_generator_payment_query.reset();
    } else if (param->benchmark == "TPCC_ALL") {
        txn_generator_neworder_query.reset();
        txn_generator_neworder_query = std::make_shared<Txn_Generator<
            Neworder_Query> >(Txn_Generator<Neworder_Query>());

        txn_generator_neworder_query->generate_txn(param, param->neworder_query_batch_size);

        for (const auto &transaction_query: txn_generator_neworder_query->get_txn()) {
            transactions_batch.emplace_back(transaction_query);
        }

        txn_generator_payment_query.reset();
        txn_generator_payment_query = std::make_shared<Txn_Generator<
            Payment_Query> >(Txn_Generator<Payment_Query>());

        txn_generator_payment_query->generate_txn(param, param->payment_query_batch_size);

        for (const auto &transaction_query: txn_generator_payment_query->get_txn()) {
            transactions_batch.emplace_back(transaction_query);
        }

        txn_generator_orderstatus_query.reset();
        txn_generator_orderstatus_query = std::make_shared<Txn_Generator<
            Orderstatus_Query> >(Txn_Generator<Orderstatus_Query>());

        txn_generator_orderstatus_query->generate_txn(param, param->orderstatus_query_batch_size);

        for (const auto &transaction_query: txn_generator_orderstatus_query->get_txn()) {
            transactions_batch.emplace_back(transaction_query);
        }

        txn_generator_delivery_query.reset();
        txn_generator_delivery_query = std::make_shared<Txn_Generator<
            Delivery_Query> >(Txn_Generator<Delivery_Query>());

        txn_generator_delivery_query->generate_txn(param, param->delivery_query_batch_size);

        for (const auto &transaction_query: txn_generator_delivery_query->get_txn()) {
            transactions_batch.emplace_back(transaction_query);
        }

        txn_generator_stocklevel_query.reset();
        txn_generator_stocklevel_query = std::make_shared<Txn_Generator<
            Stocklevel_Query> >(Txn_Generator<Stocklevel_Query>());

        txn_generator_stocklevel_query->generate_txn(param, param->stocklevel_query_batch_size);

        for (const auto &transaction_query: txn_generator_stocklevel_query->get_txn()) {
            transactions_batch.emplace_back(transaction_query);
        }

        std::default_random_engine rng(static_cast<unsigned int>(std::time(nullptr)));
        std::shuffle(transactions_batch.begin(), transactions_batch.end(), rng);
        uint32_t tid = 0;
        for (auto &transaction_test: transactions_batch) {
            if (transaction_test.type() == typeid(Neworder_Query)) {
                Neworder_Query new_query = std::any_cast<Neworder_Query>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            } else if (transaction_test.type() == typeid(Payment_Query)) {
                Payment_Query new_query = std::any_cast<Payment_Query>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            } else if (transaction_test.type() == typeid(Orderstatus_Query)) {
                Orderstatus_Query new_query = std::any_cast<Orderstatus_Query>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            } else if (transaction_test.type() == typeid(Delivery_Query)) {
                Delivery_Query new_query = std::any_cast<Delivery_Query>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            } else if (transaction_test.type() == typeid(Stocklevel_Query)) {
                Stocklevel_Query new_query = std::any_cast<Stocklevel_Query>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            }
            tid+=1;
        }

        std::sort(transactions_batch.begin(), transactions_batch.end(),
                  [this](const std::any &a, const std::any &b) { return this->sortByType(a, b); });
#ifdef LTPMG_GPUQUERY_PRINT
        // for (auto &transaction_test: transactions_batch) {
        //     if (transaction_test.type() == typeid(Neworder_Query)) {
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).TID << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).W_ID << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).D_ID << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).C_ID << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).O_ID << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).N_O_ID << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).O_OL_CNT << " ";
        //         std::cout << std::any_cast<Neworder_Query>(transaction_test).O_OL_ID << " ";
        //         std::cout << std::endl;
        //     } else if (transaction_test.type() == typeid(Payment_Query)) {
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).TID << " ";
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).W_ID << " ";
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).D_ID << " ";
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).isName << " ";
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).C_ID << " ";
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).C_LAST << " ";
        //         std::cout << std::any_cast<Payment_Query>(transaction_test).H_AMOUNT << " ";
        //         std::cout << std::endl;
        //     }
        // }
        // std::cout << std::endl;
#endif
        txn_generator_neworder_query.reset();

        txn_generator_payment_query.reset();

        txn_generator_orderstatus_query.reset();

        txn_generator_delivery_query.reset();

        txn_generator_stocklevel_query.reset();
    } else if (param->benchmark == "YCSB_A") {
        // ycsb_a_query
        txn_generator_ycsb_a_query.reset();
        txn_generator_ycsb_a_query = std::make_shared<Txn_Generator<YCSB_A_Query> >(Txn_Generator<YCSB_A_Query>());
        txn_generator_ycsb_a_query->initialize(param);

        txn_generator_ycsb_a_query->generate_txn(param, param->ycsb_a_query_batch_size);

        for (const auto &transaction_test: txn_generator_ycsb_a_query->get_txn()) {
            transactions_batch.emplace_back(transaction_test);
        }

        std::default_random_engine rng(static_cast<unsigned int>(std::time(nullptr)));
        std::shuffle(transactions_batch.begin(), transactions_batch.end(), rng);
        uint32_t tid = 0;
        for (auto &transaction_test: transactions_batch) {
            if (transaction_test.type() == typeid(YCSB_A_Query)) {
                YCSB_A_Query new_query = std::any_cast<YCSB_A_Query>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            }
            tid+=1;
        }

        // std::sort(transactions_batch.begin(), transactions_batch.end(),
        //           [this](const std::any &a, const std::any &b) { return this->sortByType(a, b); });


#ifdef LTPMG_GPUQUERY_PRINT
        for (auto &transaction_test: transactions_batch) {
            if (transaction_test.type() == typeid(YCSB_A_Query)) {
                std::cout << std::any_cast<YCSB_A_Query>(transaction_test).TID << " ";
                for (uint32_t i = 0; i < 10; ++i) {
                    std::cout << std::any_cast<YCSB_A_Query>(transaction_test).ROW_ID[i] << " ";
                }

                std::cout << std::endl;
            }
        }
        std::cout << std::endl;
#endif
        txn_generator_ycsb_b_query.reset();
    } else if (param->benchmark == "YCSB_B") {
        // ycsb_b_query
        txn_generator_ycsb_b_query.reset();
        txn_generator_ycsb_b_query = std::make_shared<Txn_Generator<YCSB_B_Query> >(Txn_Generator<YCSB_B_Query>());
        txn_generator_ycsb_b_query->initialize(param);

        txn_generator_ycsb_b_query->generate_txn(param, param->ycsb_b_query_batch_size);

        for (const auto &transaction_test: txn_generator_ycsb_b_query->get_txn()) {
            transactions_batch.emplace_back(transaction_test);
        }

        std::default_random_engine rng(static_cast<unsigned int>(std::time(nullptr)));
        std::shuffle(transactions_batch.begin(), transactions_batch.end(), rng);
        uint32_t tid = 0;
        for (auto &transaction_test: transactions_batch) {
            if (transaction_test.type() == typeid(YCSB_B_Query)) {
                YCSB_B_Query new_query = std::any_cast<YCSB_B_Query>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            }
            tid+=1;
        }
        // std::sort(transactions_batch.begin(), transactions_batch.end(),
        //           [this](const std::any &a, const std::any &b) { return this->sortByType(a, b); });

#ifdef LTPMG_GPUQUERY_PRINT
        for (auto &transaction_test: transactions_batch) {
            if (transaction_test.type() == typeid(YCSB_B_Query)) {
                std::cout << std::any_cast<YCSB_B_Query>(transaction_test).TID << " ";
                for (uint32_t i = 0; i < 10; ++i) {
                    std::cout << std::any_cast<YCSB_B_Query>(transaction_test).ROW_ID[i] << " ";
                }

                std::cout << std::endl;
            }
        }
        std::cout << std::endl;
#endif
        txn_generator_ycsb_b_query.reset();
    } else if (param->benchmark == "YCSB_C") {
        // ycsb_c_query
        txn_generator_ycsb_c_query.reset();
        txn_generator_ycsb_c_query = std::make_shared<Txn_Generator<YCSB_C_Query> >(Txn_Generator<YCSB_C_Query>());
        txn_generator_ycsb_c_query->initialize(param);

        txn_generator_ycsb_c_query->generate_txn(param, param->ycsb_c_query_batch_size);

        for (const auto &transaction_test: txn_generator_ycsb_c_query->get_txn()) {
            transactions_batch.emplace_back(transaction_test);
        }

        std::default_random_engine rng(static_cast<unsigned int>(std::time(nullptr)));
        std::shuffle(transactions_batch.begin(), transactions_batch.end(), rng);
        uint32_t tid = 0;
        for (auto &transaction_test: transactions_batch) {
            if (transaction_test.type() == typeid(YCSB_C_Query)) {
                YCSB_C_Query new_query = std::any_cast<YCSB_C_Query>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            }
            tid+=1;
        }
        // std::sort(transactions_batch.begin(), transactions_batch.end(),
        //           [this](const std::any &a, const std::any &b) { return this->sortByType(a, b); });

#ifdef LTPMG_GPUQUERY_PRINT
        for (auto &transaction_test: transactions_batch) {
            if (transaction_test.type() == typeid(YCSB_C_Query)) {
                std::cout << std::any_cast<YCSB_C_Query>(transaction_test).TID << " ";
                for (uint32_t i = 0; i < 10; ++i) {
                    std::cout << std::any_cast<YCSB_C_Query>(transaction_test).ROW_ID[i] << " ";
                }

                std::cout << std::endl;
            }
        }
        std::cout << std::endl;
#endif
        txn_generator_ycsb_c_query.reset();
    } else if (param->benchmark == "YCSB_D") {
        // ycsb_d_query
        txn_generator_ycsb_d_query.reset();
        txn_generator_ycsb_d_query = std::make_shared<Txn_Generator<YCSB_D_Query> >(Txn_Generator<YCSB_D_Query>());
        txn_generator_ycsb_d_query->initialize(param);

        txn_generator_ycsb_d_query->generate_txn(param, param->ycsb_d_query_batch_size);

        for (const auto &transaction_test: txn_generator_ycsb_d_query->get_txn()) {
            transactions_batch.emplace_back(transaction_test);
        }

        std::default_random_engine rng(static_cast<unsigned int>(std::time(nullptr)));
        std::shuffle(transactions_batch.begin(), transactions_batch.end(), rng);
        uint32_t tid = 0;
        for (auto &transaction_test: transactions_batch) {
            if (transaction_test.type() == typeid(YCSB_D_Query)) {
                YCSB_D_Query new_query = std::any_cast<YCSB_D_Query>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            }
            tid+=1;
        }
        // std::sort(transactions_batch.begin(), transactions_batch.end(),
        //           [this](const std::any &a, const std::any &b) { return this->sortByType(a, b); });

#ifdef LTPMG_GPUQUERY_PRINT
        for (auto &transaction_test: transactions_batch) {
            if (transaction_test.type() == typeid(YCSB_D_Query)) {
                std::cout << std::any_cast<YCSB_D_Query>(transaction_test).TID << " ";
                for (uint32_t i = 0; i < 10; ++i) {
                    std::cout << std::any_cast<YCSB_D_Query>(transaction_test).ROW_ID[i] << " ";
                }

                std::cout << std::endl;
            }
        }
        std::cout << std::endl;
#endif
        txn_generator_ycsb_d_query.reset();
    } else if (param->benchmark == "YCSB_E") {
        // ycsb_e_query
        txn_generator_ycsb_e_query.reset();
        txn_generator_ycsb_e_query = std::make_shared<Txn_Generator<YCSB_E_Query> >(Txn_Generator<YCSB_E_Query>());
        txn_generator_ycsb_e_query->initialize(param);

        txn_generator_ycsb_e_query->generate_txn(param, param->ycsb_e_query_batch_size);

        for (const auto &transaction_test: txn_generator_ycsb_e_query->get_txn()) {
            transactions_batch.emplace_back(transaction_test);
        }

        std::default_random_engine rng(static_cast<unsigned int>(std::time(nullptr)));
        std::shuffle(transactions_batch.begin(), transactions_batch.end(), rng);
        uint32_t tid = 0;
        for (auto &transaction_test: transactions_batch) {
            if (transaction_test.type() == typeid(YCSB_E_Query)) {
                YCSB_E_Query new_query = std::any_cast<YCSB_E_Query>(transaction_test);
                new_query.TID = tid;
                transaction_test = std::any(new_query);
            }
            tid+=1;
        }
        // std::sort(transactions_batch.begin(), transactions_batch.end(),
        //           [this](const std::any &a, const std::any &b) { return this->sortByType(a, b); });

#ifdef LTPMG_GPUQUERY_PRINT
        for (auto &transaction_test: transactions_batch) {
            if (transaction_test.type() == typeid(YCSB_E_Query)) {
                std::cout << std::any_cast<YCSB_E_Query>(transaction_test).TID << " ";
                for (uint32_t i = 0; i < 10; ++i) {
                    std::cout << std::any_cast<YCSB_E_Query>(transaction_test).ROW_ID[i] << " ";
                }

                std::cout << std::endl;
            }
        }
        std::cout << std::endl;
#endif
        txn_generator_ycsb_e_query.reset();
    }
}

void Query::malloc_global_txn(std::shared_ptr<Param> param) {
    if (param->benchmark == "TEST") {
        global_txn_info_size = param->test_query_subtxn_cnt;
    } else if (param->benchmark == "TPCC_ALL") {
        global_txn_info_size = param->neworder_query_subtxn_cnt;
        global_txn_info_size += param->payment_query_subtxn_cnt;
        global_txn_info_size += param->orderstatus_query_subtxn_cnt;
        global_txn_info_size += param->delivery_query_subtxn_cnt;
        global_txn_info_size += param->stocklevel_query_subtxn_cnt;
    } else if (param->benchmark == "TPCC_PART") {
        global_txn_info_size = param->neworder_query_subtxn_cnt;
        global_txn_info_size += param->payment_query_subtxn_cnt;
    } else if (param->benchmark == "YCSB_A") {
        global_txn_info_size = param->ycsb_a_query_subtxn_cnt;
    } else if (param->benchmark == "YCSB_B") {
        global_txn_info_size = param->ycsb_b_query_subtxn_cnt;
    } else if (param->benchmark == "YCSB_C") {
        global_txn_info_size = param->ycsb_c_query_subtxn_cnt;
    } else if (param->benchmark == "YCSB_D") {
        global_txn_info_size = param->ycsb_d_query_subtxn_cnt;
    } else if (param->benchmark == "YCSB_E") {
        global_txn_info_size = param->ycsb_e_query_subtxn_cnt;
    }
}

void Query::free_global_txn() {
}

void Query::initial_on_GPU(std::shared_ptr<Param> param) {
    LOG(INFO) << "Query::transfer_to_GPU() start initial_on_GPU()";
    initial_query_on_GPU(param, std::make_shared<std::vector<std::any> >(transactions_batch), global_txn_info);
    LOG(INFO) << "Query::transfer_to_GPU() end initial_on_GPU()";
}

void Query::transfer_to_GPU(std::shared_ptr<Param> param) {
    LOG(INFO) << "Query::transfer_to_GPU() start transfer_query_to_GPU()";
    transfer_query_to_GPU(param, std::make_shared<std::vector<std::any> >(transactions_batch), global_txn_info);
    LOG(INFO) << "Query::transfer_to_GPU() end transfer_query_to_GPU()";
}

void Query::transfer_to_CPU(std::shared_ptr<Param> param) {
    LOG(INFO) << "Query::transfer_to_CPU() start transfer_query_to_CPU()";
    transfer_query_to_CPU(param, std::make_shared<std::vector<std::any> >(transactions_batch), global_txn_info);
    LOG(INFO) << "Query::transfer_to_CPU() end transfer_query_to_CPU()";
}
