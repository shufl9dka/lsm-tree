#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include <bitset>
#include <cmath>
#include <random>
#include <string_view>

const double LN2 = log(2);

#pragma pack(push, 1)

template<size_t _S>
class BloomFilter {
public:
    BloomFilter(size_t capacity) {
        _hashes = size_t(ceil((double(_S) / capacity) * LN2) + 1e-9);
    }

    void Put(std::string_view str) {
        std::mt19937 generator(std::hash(str));
        for (size_t i = 0; i < _hashes; ++i) {
            _bitset[generator() % _S] = 1;
        }
    }

    bool ProbablyHas(std::string_view str) {
        std::mt19937 generator(std::hash(str));
        for (size_t i = 0; i < _hashes; ++i) {
            if (_bitset[generator() % _S] != 1) {
                return false;
            }
        }
        return true;
    }

private:
    std::bitset<_S> _bitset;
    size_t _hashes;
};

#endif
