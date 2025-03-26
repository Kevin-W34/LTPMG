#pragma once

#include "common.h"
#include "stdio.h"
#include "define.h"
#include <cstring>

class Random {
public:
    Random();

    ~Random();

    void init_seed(uint64_t seed);

    void set_seed(uint64_t seed);

    uint64_t get_seed();

    uint64_t next();

    uint64_t next(uint32_t bits);

    /* [0.0, 1.0) */
    double next_double();

    uint64_t uniform_dist(uint64_t a, uint64_t b);

    std::string rand_str(std::size_t length, const std::string &str);

    std::string a_string(std::size_t min_len, std::size_t max_len);

    uint64_t non_uniform_distribution(uint64_t A, uint64_t x, uint64_t y);

    std::string n_string(std::size_t min_len, std::size_t max_len);

    std::string rand_zip();

    std::string rand_last_name(int n);

    std::string rand_C_LAST();

    uint32_t rand_test(uint64_t a, uint64_t b);

    void init_rand_zipf(uint64_t n, double s);

    uint32_t rand_zipf(uint64_t a, uint64_t b);

private:
    static const std::string &alpha() {
        static std::string alpha_ =
                "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        return alpha_;
    };

    static const std::vector<std::string> &customer_last_names() {
        static std::vector<std::string> last_names = {
            "BAR", "OUGHT", "ABLE", "PRI", "PRES",
            "ESE", "ANTI", "CALLY", "ATION", "EING"
        };
        return last_names;
    };

    static const std::string &numeric() {
        static std::string numeric_ = "0123456789";
        return numeric_;
    };

    uint64_t seed_;

    std::vector<double> zipfian_distribution;
};
