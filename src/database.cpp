#include "../include/database.h"

Database::Database(const std::string &filepath) : filepath(filepath) {
    rows = this->parser.parse(this->filepath); // 使用 JSON 文件, 解析每个数据表包含的属性类型/属性名称/行数
    LOG(INFO) << "rows.size()=" << rows.size();
}

Database::~Database() {
    // free_global_row();
}

void Database::generate(std::shared_ptr<Param> param) {
    int count = 0;
    // std::vector<std::vector<std::any> > table;
    param->set_table_cnt(rows.size());
    for (const auto &row: rows) {
        // table.clear();
        uint32_t table_size = row.get_table_size();
        if (row.get_table_name() == "table1") {
            table_size = row.get_table_size();
        } else if (row.get_table_name() == "table2") {
            table_size = row.get_table_size();
        } else if (row.get_table_name() == "Warehouse") {
            table_size = param->warehouse_size;
        } else if (row.get_table_name() == "District") {
            table_size = param->district_size;
        } else if (row.get_table_name() == "Customer") {
            table_size = param->customer_size;
        } else if (row.get_table_name() == "Neworder") {
            table_size = param->neworder_size;
        } else if (row.get_table_name() == "History") {
            table_size = param->history_size;
        } else if (row.get_table_name() == "Order") {
            table_size = param->order_size;
        } else if (row.get_table_name() == "Orderline") {
            table_size = param->orderline_size;
        } else if (row.get_table_name() == "Stock") {
            table_size = param->stock_size;
        } else if (row.get_table_name() == "Item") {
            table_size = param->item_size;
        } else if (row.get_table_name() == "ycsb") {
            table_size = param->ycsb_size;
        }
        LOG(INFO) << "row.get_table_size()=" << table_size;
        // TODO list: 按照表遍历生成数据, 为多线程生成数据做准备

#ifndef LTPMG_DATABASE_MULTITHREAD
                for (size_t i = 0; i < table_size; ++i) {
                    std::vector<std::any> generate_row = generator.generateData(row);
                    if (i % 1000000 == 0) {
                        std::cout << "generate_row[" << i << "]" << std::endl;
                    }
#ifdef LTPMG_DATABASE_CHECK
                            for (const auto &data : generate_row)
                            {
                                if (data.type() == typeid(int))
                                {
                                    std::cout << std::any_cast<int>(data) << " ";
                                }
                                else if (data.type() == typeid(double))
                                {
                                    std::cout << std::any_cast<double>(data) << " ";
                                }
                                else if (data.type() == typeid(std::string))
                                {
                                    std::cout << std::any_cast<std::string>(data) << " ";
                                }
                            }
                            std::cout << std::endl;
#endif
                    table.emplace_back(generate_row);
                }
                tables.emplace_back(table);
#endif
    }
}

void Database::malloc_global_row(std::shared_ptr<Param> param) {
    LOG(INFO) << "start Database::malloc_global_row()";
    this->index_for_GPU = new Global_Table_Index[param->table_cnt];
    this->tables_for_GPU_info = new Global_Table_Info[param->table_cnt];
    for (size_t i = 0; i < param->table_cnt; ++i) {
        this->tables_for_GPU_info[i].table_cnt = param->table_cnt;
    }
    this->tables_for_GPU = new Global_Table[param->table_cnt];
    for (size_t i = 0; i < param->table_cnt; ++i) {
        for (const auto &data: rows[i].getAttributes()) {
            if (data.getType() == "int") {
                tables_for_GPU_info[i].int_size += 1;
            } else if (data.getType() == "string") {
                tables_for_GPU_info[i].string_size += 1;
            } else if (data.getType() == "double") {
                tables_for_GPU_info[i].double_size += 1;
            }
        }

        // tables_for_GPU_info[i].table_size = rows[i].get_table_size();
        if (rows[i].get_table_name() == "table1") {
            tables_for_GPU_info[i].table_size = rows[i].get_table_size();
        } else if (rows[i].get_table_name() == "table2") {
            tables_for_GPU_info[i].table_size = rows[i].get_table_size();
        } else if (rows[i].get_table_name() == "Warehouse") {
            tables_for_GPU_info[i].table_size = param->warehouse_size;
        } else if (rows[i].get_table_name() == "District") {
            tables_for_GPU_info[i].table_size = param->district_size;
        } else if (rows[i].get_table_name() == "Customer") {
            tables_for_GPU_info[i].table_size = param->customer_size;
        } else if (rows[i].get_table_name() == "Neworder") {
            tables_for_GPU_info[i].table_size = param->neworder_size;
        } else if (rows[i].get_table_name() == "History") {
            tables_for_GPU_info[i].table_size = param->history_size;
        } else if (rows[i].get_table_name() == "Order") {
            tables_for_GPU_info[i].table_size = param->order_size;
        } else if (rows[i].get_table_name() == "Orderline") {
            tables_for_GPU_info[i].table_size = param->orderline_size;
        } else if (rows[i].get_table_name() == "Stock") {
            tables_for_GPU_info[i].table_size = param->stock_size;
        } else if (rows[i].get_table_name() == "Item") {
            tables_for_GPU_info[i].table_size = param->item_size;
        } else if (rows[i].get_table_name() == "ycsb") {
            tables_for_GPU_info[i].table_size = param->ycsb_size;
        }

        LOG(INFO) << "table " << i
                << ",table_size=" << tables_for_GPU_info[i].table_size
                << ",int_size=" << tables_for_GPU_info[i].int_size
                << ",string_size=" << tables_for_GPU_info[i].string_size
                << ",double_size=" << tables_for_GPU_info[i].double_size;
        this->tables_for_GPU[i].int_data = new INT32[
            tables_for_GPU_info[i].int_size * tables_for_GPU_info[i].table_size]; // Row1int1,Row1int2,Row2int1,Row2int2
        this->tables_for_GPU[i].string_data = new UINT32[
            tables_for_GPU_info[i].string_size * tables_for_GPU_info[i].table_size
            * tables_for_GPU_info[i].string_length];
        this->tables_for_GPU[i].double_data = new DOUBLE[
            tables_for_GPU_info[i].double_size * tables_for_GPU_info[i].table_size];

        memset(this->tables_for_GPU[i].int_data, 0x0,
               sizeof(INT32) * tables_for_GPU_info[i].int_size * tables_for_GPU_info[i].table_size);
        memset(this->tables_for_GPU[i].string_data, 0x0,
               sizeof(INT32) * tables_for_GPU_info[i].string_size * tables_for_GPU_info[i].string_length *
               tables_for_GPU_info[i].table_size);
        memset(this->tables_for_GPU[i].double_data, 0x0,
               sizeof(INT32) * tables_for_GPU_info[i].double_size * tables_for_GPU_info[i].table_size);

        if (param->benchmark == "TPCC_PART") {
            index_for_GPU[2].index = new UINT32[tables_for_GPU_info[2].table_size];
        } else if (param->benchmark == "TPCC_ALL") {
            index_for_GPU[2].index = new UINT32[tables_for_GPU_info[2].table_size];
        }
    }
    LOG(INFO) << "end Database::malloc_global_row()";
}

std::vector<UINT32> Database::string_to_uint(const std::string &input) {
    std::vector<UINT32> output;

    size_t length = input.size();

    // 每四个字符组成一个 uint32_t
    for (size_t i = 0; i < length; i += 4) {
        uint32_t value = 0;
        // 对于每个字符，将其 ASCII 值移到正确的位置并累加到 value
        for (size_t j = 0; j < 4; ++j) {
            if (i + j < length) {
                // 将 ASCII 值左移以保存在正确的位置
                value |= static_cast<uint32_t>(input[i + j]) << (24 - j * 8);
            } else {
                value |= 0 << (24 - j * 8);
            }
        }
        output.emplace_back(value);
    }
    return output;
}

void Database::copy_to_global_row(std::shared_ptr<Param> param) {
#ifndef LTPMG_DATABASE_MULTITHREAD
    int tableID = 0;
    for (const auto &table: tables) {
        LOG(INFO) << "Database::copy_to_global_row() table.size()=" << table.size();
        int rowID = 0;
        for (const auto &attributes: table) {
            int intID = 0;
            int stringID = 0;
            int doubleID = 0;
            for (const auto &data: attributes) {
                if (data.type() == typeid(int)) {
                    tables_for_GPU[tableID].int_data[tables_for_GPU_info[tableID].int_size * rowID + intID] =
                            std::any_cast<int>(data);
                    ++intID;
                } else if (data.type() == typeid(double)) {
                    tables_for_GPU[tableID].double_data[tables_for_GPU_info[tableID].double_size * rowID + doubleID] =
                            std::any_cast<double>(data);
                    ++doubleID;
                } else if (data.type() == typeid(std::string)) {std::vector<UINT32> res = string_to_uint(std::any_cast<std::string>(data));

                    // std::cout << std::hex;
                    for (size_t i = 0; i < res.size(); ++i) {
                        tables_for_GPU[tableID].string_data[
                            tables_for_GPU_info[tableID].string_size * rowID * tables_for_GPU_info[tableID].
                            string_length + stringID * tables_for_GPU_info[tableID].string_length + i] = res[i];
                    }

                    ++stringID;
                }
            }
            ++rowID;
        }
        ++tableID;
    }
#endif
#ifdef LTPMG_DATABASE_MULTITHREAD


    for (size_t i = 0; i < param->table_cnt; ++i) {
        LOG(INFO) << "MultiThread Database::copy_to_global_row() table.size()=" << tables_for_GPU_info[i].table_size;
        auto generate_batch_INT32 = [this](size_t start, size_t count,
                                           INT32 *table_int_data, std::shared_ptr<Param> param) {
            for (size_t i = 0; i < count; ++i) {
                table_int_data[start + i] = generator.generateRandomInt();
            }
        };

        auto generate_batch_STRING = [this](size_t start, size_t count, size_t string_length,
                                            UINT32 *table_string_data, std::shared_ptr<Param> param) {
            for (size_t i = 0; i < count; ++i) {
                std::string str = generator.generateRandomString();
                std::vector<UINT32> stoc = string_to_uint(str);
                for (size_t j = 0; j < string_length; ++j) {
                    if (j < str.size()) {
                        table_string_data[(start + i) * string_length + j] = stoc[j];
                    } else {
                        table_string_data[(start + i) * string_length + j] = 0;
                    }
                }
            }
        };

        auto generate_batch_DOUBLE = [this](size_t start, size_t count,
                                            DOUBLE *table_double_data, std::shared_ptr<Param> param) {
            for (size_t i = 0; i < count; ++i) {
                table_double_data[start + i] = generator.generateRandomDouble();
            }
        };

        size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数
        std::vector<std::thread> threads;

        size_t total_size = tables_for_GPU_info[i].table_size * tables_for_GPU_info[i].int_size;
        size_t batch_size = (total_size + num_threads - 1) / num_threads;
        if (batch_size == 0) {
            batch_size = 1;
        }

        for (size_t t = 0; t < num_threads; ++t) {
            size_t start = t * batch_size;
            // size_t count = (t == num_threads - 1) ? (param->table_size - start) : batch_size;
            size_t count = (start < total_size) ? std::min(batch_size, total_size - start) : 0;
            if (count > 0) {
                threads.emplace_back(std::bind(generate_batch_INT32, start, count,
                                               tables_for_GPU[i].int_data, param));
            }
        }
        for (auto &t: threads) {
            t.join();
        }

        threads.clear();
        total_size = tables_for_GPU_info[i].table_size *
                     tables_for_GPU_info[i].string_size;
        batch_size = (total_size + num_threads - 1) / num_threads;
        if (batch_size == 0) {
            batch_size = 1;
        }
        for (size_t t = 0; t < num_threads; ++t) {
            size_t start = t * batch_size;
            size_t count = (start < total_size) ? std::min(batch_size, total_size - start) : 0;
            if (count > 0) {
                threads.emplace_back(std::bind(generate_batch_STRING, start, count,
                                               tables_for_GPU_info[i].string_length, tables_for_GPU[i].string_data,
                                               param));
            }
        }

        for (auto &t: threads) {
            t.join();
        }

        threads.clear();
        total_size = tables_for_GPU_info[i].table_size * tables_for_GPU_info[i].double_size;
        batch_size = (total_size + num_threads - 1) / num_threads;
        if (batch_size == 0) {
            batch_size = 1;
        }
        for (size_t t = 0; t < num_threads; ++t) {
            size_t start = t * batch_size;
            size_t count = (start < total_size) ? std::min(batch_size, total_size - start) : 0;
            if (count > 0) {
                threads.emplace_back(std::bind(generate_batch_DOUBLE, start, count,
                                               tables_for_GPU[i].double_data, param));
            }
        }

        for (auto &t: threads) {
            t.join();
        }
        threads.clear();
    }

    auto generate_batch_index = [this](size_t start, size_t count,
                                       UINT32 *index, std::shared_ptr<Param> param) {
        static std::vector<std::string> last_names = {
            "AAA", "BBB", "CCC", "DDD", "EEE",
            "FFF", "GGG", "HHH", "III", "JJJ",
            "KKK", "LLL", "MMM", "NNN", "OOO"
        };
        std::vector<UINT32> last_names_vector;
        for (size_t i = 0; i < last_names.size(); ++i) {
            std::string result = "";
            uint32_t tmp = rand() % last_names.size();
            result = last_names[tmp].c_str();
            uint32_t value = 0;
            for (size_t j = 0; j < 4; ++j) {
                value |= static_cast<uint32_t>(result[j]) << (24 - j * 8);
            }
            last_names_vector.emplace_back(value);
        }
        for (size_t i = 0; i < count; ++i) {
            uint32_t tmp = rand() % last_names.size();
            uint32_t value = last_names_vector[tmp];
            index[start + i] = value;
        }
    };

    if (param->benchmark == "TPCC_PART") {
        // 生成index
        for (size_t i = 0; i < param->table_cnt; ++i) {
            if (i == 2) {
                //customer
                LOG(INFO) << "MultiThread generate_index table.size()=" << tables_for_GPU_info[i].table_size;
                size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数
                std::vector<std::thread> threads;

                size_t total_size = tables_for_GPU_info[i].table_size;
                size_t batch_size = (total_size + num_threads - 1) / num_threads;
                if (batch_size == 0) {
                    batch_size = 1;
                }
                for (size_t t = 0; t < num_threads; ++t) {
                    size_t start = t * batch_size;
                    // size_t count = (t == num_threads - 1) ? (param->table_size - start) : batch_size;
                    size_t count = (start < total_size) ? std::min(batch_size, total_size - start) : 0;
                    if (count > 0) {
                        threads.emplace_back(std::bind(generate_batch_index, start, count,
                                                       index_for_GPU[i].index, param));
                    }
                }
                for (auto &t: threads) {
                    t.join();
                }
                threads.clear();
            }
        }
    } else if (param->benchmark == "TPCC_ALL") {
        // 生成index
        for (size_t i = 0; i < param->table_cnt; ++i) {
            if (i == 2) {
                //customer
                LOG(INFO) << "MultiThread generate_index table.size()=" << tables_for_GPU_info[i].table_size;
                size_t num_threads = std::thread::hardware_concurrency(); // 获取可用线程数
                std::vector<std::thread> threads;

                size_t total_size = tables_for_GPU_info[i].table_size;
                size_t batch_size = (total_size + num_threads - 1) / num_threads;
                if (batch_size == 0) {
                    batch_size = 1;
                }
                for (size_t t = 0; t < num_threads; ++t) {
                    size_t start = t * batch_size;
                    // size_t count = (t == num_threads - 1) ? (param->table_size - start) : batch_size;
                    size_t count = (start < total_size) ? std::min(batch_size, total_size - start) : 0;
                    if (count > 0) {
                        threads.emplace_back(std::bind(generate_batch_index, start, count,
                                                       index_for_GPU[i].index, param));
                    }
                }
                for (auto &t: threads) {
                    t.join();
                }
                threads.clear();
            }
        }
    }
#endif
}

void Database::free_global_row(std::shared_ptr<Param> param) {
    LOG(INFO) << "start Database::free_global_row()";
    if (param->benchmark == "TPCC_PART") {
        delete[] index_for_GPU[2].index;
    } else if (param->benchmark == "TPCC_ALL") {
        delete[] index_for_GPU[2].index;
    }
    for (size_t i = 0; i < param->table_cnt; ++i) {
        delete[] tables_for_GPU[i].int_data;
        delete[] tables_for_GPU[i].string_data;
        delete[] tables_for_GPU[i].double_data;
    }
    delete[] tables_for_GPU;
    delete[] tables_for_GPU_info;
    delete[] index_for_GPU;
    LOG(INFO) << "end Database::free_global_row()";
}

void Database::print() const {
    for (const auto &table: tables) {
        std::cout << "table.size()=" << table.size() << std::endl;
        for (const auto &attributes: table) {
            for (const auto &data: attributes) {
                if (data.type() == typeid(int)) {
                    std::cout << std::any_cast<int>(data) << " ";
                } else if (data.type() == typeid(double)) {
                    std::cout << std::any_cast<double>(data) << " ";
                } else if (data.type() == typeid(std::string)) {
                    std::cout << std::any_cast<std::string>(data) << " ";
                }
            }
            std::cout << std::endl;
        }
    }
}

void Database::print_global_row() const {
    std::cout << std::dec;
    for (size_t i = 0; i < rows.size(); ++i) {
        std::cout << "table.size()=" << tables_for_GPU_info[i].table_size << std::endl;
        for (size_t j = 0; j < tables_for_GPU_info[i].int_size * tables_for_GPU_info[i].table_size; ++j) {
            std::cout << tables_for_GPU[i].int_data[j] << " ";
        }
        std::cout << std::endl;

        for (size_t j = 0; j < tables_for_GPU_info[i].double_size * tables_for_GPU_info[i].table_size; ++j) {
            std::cout << tables_for_GPU[i].double_data[j] << " ";
        }
        std::cout << std::endl;

        std::cout << std::hex;
        for (size_t j = 0; j < tables_for_GPU_info[i].string_size * tables_for_GPU_info[i].table_size *
                           tables_for_GPU_info[i].string_length; ++j) {
            // if (j % tables_for_GPU_info[i].string_length == 0) {
            //     std::cout << std::endl;
            // }
            std::cout << tables_for_GPU[i].string_data[j] << " ";
        }
        std::cout << std::dec;
        std::cout << std::endl;
    }
}

void Database::test() {
}

void Database::transfer_to_GPU(std::shared_ptr<Param> param) {
    LOG(INFO) << "Database::transfer_to_GPU() start transfer_database_to_GPU()";
    transfer_database_to_GPU(param, tables_for_GPU_info, tables_for_GPU, index_for_GPU);
    LOG(INFO) << "Database::transfer_to_GPU() end transfer_database_to_GPU()";
}

void Database::transfer_to_CPU(std::shared_ptr<Param> param) {
    LOG(INFO) << "Database::transfer_to_CPU() start transfer_database_to_CPU()";
    transfer_database_to_CPU(param, tables_for_GPU_info, tables_for_GPU, index_for_GPU);
    LOG(INFO) << "Database::transfer_to_CPU() end transfer_database_to_CPU()";
}
