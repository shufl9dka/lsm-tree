#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include <iostream>
#include <bitset>
#include <climits>
#include <cmath>
#include <random>
#include <string_view>

const double LN2 = log(2);

template<size_t _S>
class BloomFilter {
public:
    BloomFilter() : _hashes(1UL) {
        _bitset = new std::bitset<_S>();
    }

    ~BloomFilter() {
        delete _bitset;
    }

    BloomFilter(const BloomFilter<_S>& other) : _hashes(other._hashes) {
        if (other._bitset != nullptr) {
            _bitset = new std::bitset<_S>(*(other._bitset));
        }
    }

    BloomFilter<_S>& operator=(const BloomFilter<_S>& other) {
        _hashes = other._hashes;
        if (other._bitset != nullptr) {
            *_bitset = *other._bitset;
        } else {
            delete _bitset;
            _bitset = nullptr;
        }
    }

    BloomFilter(BloomFilter<_S>&& other) noexcept : _bitset(other._bitset), _hashes(other._hashes) {
        other._bitset = nullptr;
    }

    BloomFilter<_S>& operator=(BloomFilter<_S>&& other) noexcept {
        if (this != &other) {
            delete _bitset;

            _bitset = other._bitset;
            _hashes = other._hashes;
        }
        return *this;
    }

    void Put(const std::string& str) {
        std::seed_seq seed{std::hash<std::string>{}(str)};
        std::mt19937 generator(seed);
        for (size_t i = 0; i < _hashes; ++i) {
            (*_bitset)[generator() % _S] = 1;
        }
    }

    bool ProbablyHas(const std::string& str) const {
        std::seed_seq seed{std::hash<std::string>{}(str)};
        std::mt19937 generator(seed);
        for (size_t i = 0; i < _hashes; ++i) {
            if ((*_bitset)[generator() % _S] != 1) {
                return false;
            }
        }
        return true;
    }

    void Extend(const BloomFilter<_S>& other) {
        *_bitset |= *other._bitset;
    }

    inline std::bitset<_S>* Data() noexcept {
        return _bitset;
    }

    constexpr size_t SizeBytes() const noexcept {
        return (_S + CHAR_BIT - 1) / CHAR_BIT;
    }

    inline void UpdateHashes(size_t capacity) noexcept {
        if (!_bitset->any()) {
            _hashes = size_t(ceil((double(_S) / capacity) * LN2) + 1e-9);
        }
    }

private:
    std::bitset<_S>* _bitset;
    size_t _hashes;
};

#endif
