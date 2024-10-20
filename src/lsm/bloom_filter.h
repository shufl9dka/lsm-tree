#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include <iostream>
#include <vector>
#include <cmath>
#include <random>
#include <string_view>

const double LN2 = log(2);

#pragma pack(push, 1)

template<size_t _S>
class BloomFilter {
public:
    BloomFilter() : _hashes(1UL) {
        _bitset.assign(_S, false);
    };

    BloomFilter(size_t capacity) : _hashes(size_t(ceil((double(_S) / capacity) * LN2) + 1e-9)) {
        _bitset.assign(_S, false);
    }

    void Put(const std::string& str) {
        std::seed_seq seed{std::hash<std::string>{}(str)};
        std::mt19937 generator(seed);
        for (size_t i = 0; i < _hashes; ++i) {
            _bitset[generator() % _S] = 1;
        }
    }

    bool ProbablyHas(const std::string& str) {
        std::seed_seq seed{std::hash<std::string>{}(str)};
        std::mt19937 generator(seed);
        for (size_t i = 0; i < _hashes; ++i) {
            if (_bitset[generator() % _S] != 1) {
                return false;
            }
        }
        return true;
    }

    void Extend(const BloomFilter<_S>& other) {
        for (size_t i = 0; i < _S; ++i) {
            _bitset[i] |= other._bitset[i];
        }
    }

    friend class SSTable;

private:
    std::vector<char> _bitset;
    size_t _hashes;
};

#endif
