#include "../include/random.h"

Random::Random()
{
    uint64_t seed = 0;
    init_seed(seed);
    srand((uint16_t)time(0));
}

Random::~Random()
{
}

void Random::init_seed(uint64_t seed)
{
    seed_ = (seed ^ 0x5DEECE66DULL) & ((1ULL << 48) - 1);
}

void Random::set_seed(uint64_t seed)
{
    seed_ = seed;
}

uint64_t Random::get_seed()
{
    return seed_;
}

uint64_t Random::next()
{
    return ((uint64_t)next(32) << 32) + next(32);
}

uint64_t Random::next(uint32_t bits)
{
    seed_ = (seed_ * 0x5DEECE66DULL + 0xBULL) & ((1ULL << 48) - 1);
    return (seed_ >> (48 - bits));
}

double Random::next_double()
{
    return (((uint64_t)next(26) << 27) + next(27)) / (double)(1ULL << 53);
}

uint64_t Random::uniform_dist(uint64_t a, uint64_t b)
{
    if (a == b)
        return a;
    // return next() % (b - a + 1) + a;
    return rand() % (b - a + 1) + a;
}

std::string Random::rand_str(std::size_t length, const std::string &str)
{
    std::string result;
    auto str_len = str.length();
    for (auto i = 0u; i < length; i++)
    {
        int k = uniform_dist(0, str_len - 1);
        result += str[k];
    }
    return result;
}

std::string Random::a_string(std::size_t min_len, std::size_t max_len)
{
    auto len = uniform_dist(min_len, max_len);
    return rand_str(len, alpha());
}

uint64_t Random::non_uniform_distribution(uint64_t A, uint64_t x, uint64_t y)
{
    return (uniform_dist(0, A) | uniform_dist(x, y)) % (y - x + 1) + x;
}

std::string Random::n_string(std::size_t min_len, std::size_t max_len)
{
    auto len = uniform_dist(min_len, max_len);
    return rand_str(len, numeric());
}

std::string Random::rand_zip()
{
    auto zip = n_string(4, 4);
    // append "11111"
    for (int i = 0; i < 5; i++)
    {
        zip += '1';
    }
    return zip;
}

std::string Random::rand_last_name(int n)
{
    const auto &last_names = customer_last_names();
    const auto &s1 = last_names[n / 100];
    const auto &s2 = last_names[n / 10 % 10];
    const auto &s3 = last_names[n % 10];
    return s1 + s2 + s3;
}

std::string Random::rand_C_LAST()
{
    static std::vector<std::string> last_names = {
        "AAA", "BBB", "CCC", "DDD", "EEE",
        "FFF", "GGG", "HHH", "III", "JJJ",
        "KKK", "LLL", "MMM", "NNN", "OOO"};
    std::string result = "";
    // for (size_t i = 0; i < 3; i++)
    // {
    auto tmp = this->uniform_dist(0, last_names.size() - 1);
    result = last_names[tmp].c_str();
    // }
    return result;
}