#include "sstable.h"

#include <iostream>
#include <algorithm>
#include <cstdio>
#include <fstream>
#include <filesystem>
#include <queue>
#include <stdexcept>

struct _KVfromSST {
    SSTable::TKeyValue kv;
    size_t i;

    _KVfromSST(const SSTable::TKeyValue& _kv, size_t _i) : kv(_kv), i(_i) {}
};

inline bool operator <(const _KVfromSST& a, const _KVfromSST& b) {
    return a.kv.first == b.kv.first ? a.i < b.i : a.kv.first > b.kv.first;
}

SSTable::SSTable(const std::string& filepath) : _filepath(filepath) {
    std::ifstream file(filepath, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Failed to open SSTable file for reading");
    }

    file.seekg(-std::streamoff(_bloom_filter._bitset.size()), std::ios::end);
    _stopPos = file.tellg();
    file.read(reinterpret_cast<char*>(_bloom_filter._bitset.data()), _bloom_filter._bitset.size());
}

SSTable::SSTable(const std::string& filepath, MemTable& memTable) : _filepath(filepath) {
    std::ofstream file(filepath, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Failed to open SSTable file for writing");
    }

    std::cout << "gop1" << std::endl;
    std::unordered_map<std::string, std::string> mem(std::move(memTable._map));
    memTable._map.clear();

    _bloom_filter = BloomFilter<BLOOM_SIZE>(mem.size());
    std::cout << "gop2" << std::endl;

    std::cout << "filter gool" << std::endl;
    std::cout << mem.size() << std::endl;
    for (const auto& [key, value] : mem) {
        _bloom_filter.Put(key);
        file << KEY_TOKEN << key << VALUE_TOKEN << value;
    }

    file.write(reinterpret_cast<char*>(_bloom_filter._bitset.data()), _bloom_filter._bitset.size());
    file.close();

    std::ifstream inp(filepath, std::ios::binary | std::ios::ate);
    _stopPos = inp.tellg() - std::streampos(_bloom_filter._bitset.size());
}

SSTable::SSTable(const std::string& filepath, std::vector<SSTable>& toMerge) : _filepath(filepath) {
    std::cout << "ok1" << std::endl;
    std::ofstream file(filepath, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Failed to open SSTable file for writing");
    }
    std::cout << "ok2" << std::endl;

    std::priority_queue<_KVfromSST> pq;
    auto updatePqWithTable = [&](size_t idx) {
        std::optional<TKeyValue> kv = toMerge[idx].GetNextKV();
        if (kv.has_value()) {
            pq.emplace(*kv, idx);
        }
    };

    std::cout << "MERGE1" << std::endl;
    for (size_t i = 0; i < toMerge.size(); ++i) {
        updatePqWithTable(i);
    }

    std::cout << "MERGE2" << std::endl;
    while (!pq.empty()) {
        const auto& best = pq.top(); pq.pop();

        file << KEY_TOKEN << best.kv.first << VALUE_TOKEN << best.kv.second;
        updatePqWithTable(best.i);
    }

    for (const auto& ssTable : toMerge) {
        _bloom_filter.Extend(ssTable._bloom_filter);
    }
    file.write(reinterpret_cast<char*>(_bloom_filter._bitset.data()), _bloom_filter._bitset.size());
    file.close();

    std::ifstream inp(filepath, std::ios::binary | std::ios::ate);
    _stopPos = inp.tellg() - std::streampos(_bloom_filter._bitset.size());
    inp.close();

    for (auto& ssTable : toMerge) {
        ssTable.Clear();
    }
    toMerge.clear();
    std::cout << "clean" << std::endl;
}

std::optional<std::string> SSTable::At(const std::string& key) {
    if (!_bloom_filter.ProbablyHas(key)) {
        return std::nullopt;
    }

    OpenReadFile();

    std::streampos low = 0;
    std::streampos high = _stopPos;

    auto findNextKeyTokenPos = [&](std::streampos pos, std::streampos high) -> std::optional<std::streampos> {
        std::vector<char> buffer(BUFFER_SIZE);
        while (pos <= high) {
            _file.seekg(pos);
            size_t toRead = std::min(std::streamoff(BUFFER_SIZE), high - pos + 1);
            _file.read(buffer.data(), toRead);
            size_t readCount = _file.gcount();

            for (size_t i = 0; i < readCount; ++i) {
                if (buffer[i] == KEY_TOKEN) {
                    return pos + std::streamoff(i);
                }
            }
            if (readCount < toRead) {
                break;
            }

            pos += std::streamoff(readCount);
        }
        return std::nullopt;
    };

    auto getKeyValueAt = [&](std::streampos keyTokenPos) -> std::optional<std::pair<TKeyValue, std::streampos>> {
        _file.clear(); // EOF flags
        _file.seekg(keyTokenPos);

        char ch;
        _file.get(ch);
        if (!_file || ch != KEY_TOKEN) {
            return std::nullopt;
        }

        std::string keyRead;
        while (_file.get(ch)) {
            if (ch == VALUE_TOKEN) {
                break;
            }
            keyRead += ch;
        }
        if (!_file) {
            return std::nullopt;
        }

        std::string valueRead;
        while (_file.get(ch)) {
            if (ch == KEY_TOKEN) {
                _file.unget();
                break;
            }
            valueRead += ch;
            if (_file.tellg() >= _stopPos) {
                break;
            }
        }

        std::streampos nextPos = _file.tellg();
        if (!_file) {
            nextPos = _stopPos;
        }

        return std::make_optional(std::make_pair(std::make_pair(keyRead, valueRead), nextPos));
    };

    while (low <= high) {
        std::streampos mid = low + (high - low) / 2;

        auto optKeyTokenPos = findNextKeyTokenPos(mid, high);
        if (!optKeyTokenPos.has_value()) {
            high = mid - std::streamoff(1);
            continue;
        }
        std::streampos keyTokenPos = optKeyTokenPos.value();

        auto kvOpt = getKeyValueAt(keyTokenPos);
        if (!kvOpt.has_value()) {
            high = mid - std::streamoff(1);
            continue;
        }
        auto kvPos = kvOpt.value();

        if (kvPos.first.first < key) {          // found is less
            low = kvPos.second;
        } else if (kvPos.first.first == key) {  // good key
            CloseReadFile();
            return kvPos.first.second;
        } else {                                // found is
            if (keyTokenPos > 0) {
                high = keyTokenPos - std::streamoff(1);
            } else {
                break;
            }
        }
    }

    CloseReadFile();
    return std::nullopt;
}

void SSTable::OpenReadFile() {
    _file.open(_filepath, std::ios::binary);
}

void SSTable::CloseReadFile() {
    _file.close();
}

void SSTable::Clear() {
    if (_file.is_open()) {
        CloseReadFile();
    }

    std::remove(_filepath.c_str());
}

std::optional<SSTable::TKeyValue> SSTable::GetNextKV(std::optional<std::streampos> startPos) {
    if (!_file.is_open()) {
        OpenReadFile();
    }
    if (startPos.has_value()) {
        _file.seekg(*startPos);
    }

    std::vector<char> buffer(BUFFER_SIZE);
    std::string key, value;

    int state = 0;
    size_t pos = _file.tellg();
    while (pos < static_cast<size_t>(_stopPos)) {
        _file.read(buffer.data(), std::min(BUFFER_SIZE, static_cast<size_t>(_stopPos) - pos));
        size_t appended = _file.gcount();

        for (size_t i = 0; i < appended; ++i, ++pos) {
            char ch = buffer[i];
            if (ch == KEY_TOKEN) {
                if (state == 2) {
                    _file.seekg(std::streampos(pos + 1));
                    return std::make_optional<TKeyValue>(key, value);
                } else {
                    state = 1;
                    continue;
                }
            }
            if (ch == VALUE_TOKEN && state == 1) {
                state = 2;
                continue;
            }
            (state == 1 ? key : value) += ch;
        }
    }

    if (key.empty()) {
        return std::nullopt;
    } else {
        return std::make_optional<TKeyValue>(key, value);
    }
}
