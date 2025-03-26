#pragma once

#ifndef LTPMG_TXN_GENERATOR
#define LTPMG_TXN_GENERATOR

#ifndef LTPMG_TXN_GENERATOR_M
#define LTPMG_TXN_GENERATOR_M
#endif

#include "define.h"
#include "param.h"
#include "txn_structure.h"
#include "random.h"

template<typename T>
class Txn_Generator {
private:
    std::vector<T> transactions;
    Random random;
    // std::atomic_uint32_t tid;

public:
    Txn_Generator() {
    };

    ~Txn_Generator() {
    };

    void initialize(std::shared_ptr<Param> param);

    void generate_txn(std::shared_ptr<Param> param, size_t size);

    std::vector<T> &get_txn() { return transactions; }
};

template<>
void Txn_Generator<Test_Query>::generate_txn(std::shared_ptr<Param> param, size_t size);

template<>
void Txn_Generator<Test_Query_2>::generate_txn(std::shared_ptr<Param> param, size_t size);

template<>
void Txn_Generator<Neworder_Query>::generate_txn(std::shared_ptr<Param> param, size_t size);

template<>
void Txn_Generator<Payment_Query>::generate_txn(std::shared_ptr<Param> param, size_t size);

template<>
void Txn_Generator<Orderstatus_Query>::generate_txn(std::shared_ptr<Param> param, size_t size);

template<>
void Txn_Generator<Stocklevel_Query>::generate_txn(std::shared_ptr<Param> param, size_t size);

template<>
void Txn_Generator<Delivery_Query>::generate_txn(std::shared_ptr<Param> param, size_t size);

template<>
void Txn_Generator<YCSB_A_Query>::generate_txn(std::shared_ptr<Param> param, size_t size);

template<>
void Txn_Generator<YCSB_B_Query>::generate_txn(std::shared_ptr<Param> param, size_t size);

template<>
void Txn_Generator<YCSB_C_Query>::generate_txn(std::shared_ptr<Param> param, size_t size);

template<>
void Txn_Generator<YCSB_D_Query>::generate_txn(std::shared_ptr<Param> param, size_t size);

template<>
void Txn_Generator<YCSB_E_Query>::generate_txn(std::shared_ptr<Param> param, size_t size);
#endif
