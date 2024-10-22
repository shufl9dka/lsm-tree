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

    file.seekg(-std::streamoff(_bloom_filter.SizeBytes()), std::ios::end);
    _stopPos = file.tellg();
    file.read(reinterpret_cast<char*>(_bloom_filter.Data()), _bloom_filter.SizeBytes());
}

SSTable::SSTable(const std::string& filepath, MemTable& memTable) : _filepath(filepath) {
    std::ofstream file(filepath, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Failed to open SSTable file for writing");
    }

    std::unordered_map<std::string, std::string> mem(std::move(memTable._map));
    // here, memTable is free to be used in another thread so we can do the part below in background

    std::vector<std::pair<std::string, std::string>> ssv;
    ssv.reserve(mem.size());
    while (!mem.empty()) {
        auto it = mem.begin();
        ssv.emplace_back(std::move(it->first), std::move(it->second));
        mem.erase(it);
    }
    std::sort(ssv.begin(), ssv.end());

    _bloom_filter.UpdateHashes(ssv.size());
    for (const auto& [key, value] : ssv) {
        _bloom_filter.Put(key);
        file << KEY_TOKEN << key << VALUE_TOKEN << value;
    }

    file.write(reinterpret_cast<char*>(_bloom_filter.Data()), _bloom_filter.SizeBytes());
    file.close();

    std::ifstream inp(filepath, std::ios::binary | std::ios::ate);
    _stopPos = inp.tellg() - std::streampos(_bloom_filter.SizeBytes());
}

SSTable::SSTable(const std::string& filepath, std::vector<SSTable>& toMerge) : _filepath(filepath) {
    std::ofstream file(filepath, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Failed to open SSTable file for writing");
    }

    std::priority_queue<_KVfromSST> pq;
    auto updatePqWithTable = [&](size_t idx) {
        std::optional<TKVPos> kv = toMerge[idx].GetNextKV();
        if (kv.has_value()) {
            pq.emplace((*kv).first, idx);
        }
    };

    for (size_t i = 0; i < toMerge.size(); ++i) {
        updatePqWithTable(i);
    }

    while (!pq.empty()) {
        const auto& best = pq.top(); pq.pop();

        file << KEY_TOKEN << best.kv.first << VALUE_TOKEN << best.kv.second;
        updatePqWithTable(best.i);
    }

    for (const auto& ssTable : toMerge) {
        _bloom_filter.Extend(ssTable._bloom_filter);
    }
    file.write(reinterpret_cast<char*>(_bloom_filter.Data()), _bloom_filter.SizeBytes());
    file.close();

    std::ifstream inp(filepath, std::ios::binary | std::ios::ate);
    _stopPos = inp.tellg() - std::streampos(_bloom_filter.SizeBytes());
    inp.close();

    for (auto& ssTable : toMerge) {
        ssTable.Clear();
    }
    toMerge.clear();
}

std::optional<std::string> SSTable::At(const std::string& key) {
    if (!_bloom_filter.ProbablyHas(key)) {
        return std::nullopt;
    }

    OpenReadFile();

    std::streampos low = 0;
    std::streampos high = _stopPos;

    while (high - low > 8 * BUFFER_SIZE) {
        std::streampos mid = low + (high - low) / 2;

        auto kvPos = GetNextKV(mid, int64_t(high) + 1);
        if (!kvPos.has_value()) {
            high = mid - std::streamoff(1);
            continue;
        }

        auto [kv, keyPos] = kvPos.value();
        if (kv.first < key) {           // found is less
            low = keyPos.second;
        } else if (kv.first == key) {   // good key
            CloseReadFile();
            return kv.second;
        } else {                        // found is greater
            if (keyPos.first > low) {   // and not on the beggining
                high = keyPos.first - std::streamoff(1);
            } else {
                break;
            }
        }
    }

    std::vector<char> buffer(high - low + 1);
    size_t sz = ReadToBuf(buffer.data(), high - low + 1, low);
    CloseReadFile();

    std::string line(buffer.begin(), buffer.begin() + sz);
    std::string toFind = std::string() + KEY_TOKEN + key + VALUE_TOKEN;
    size_t keyBegin = line.find(toFind);
    if (keyBegin == std::string::npos) {
        return std::nullopt;
    }

    size_t valueStart = keyBegin + toFind.size();
    size_t valueEnd = std::min(line.size(), line.find(KEY_TOKEN, valueStart));
    return line.substr(valueStart, valueEnd - valueStart);
}

void SSTable::OpenReadFile() {
    _file.open(_filepath, std::ios::binary);
}

void SSTable::CloseReadFile() {
    _file.close();
}

size_t SSTable::ReadToBuf(char* buffer, size_t max, std::optional<std::streampos> startPos) {
    if (!_file.is_open()) {
        OpenReadFile();
    }
    if (startPos.has_value()) {
        _file.seekg(*startPos);
    }

    _file.read(buffer, max);
    return _file.gcount();
}

void SSTable::Clear() {
    if (_file.is_open()) {
        CloseReadFile();
    }

    std::remove(_filepath.c_str());
}

std::optional<SSTable::TKVPos> SSTable::GetNextKV(std::optional<std::streampos> startPos, std::optional<std::streampos> stopPos) {
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
    std::streampos keyPos;
    std::streampos lStopPos = (stopPos.has_value()) ? std::min(*stopPos, _stopPos) : _stopPos;

    while (pos < static_cast<size_t>(lStopPos)) {
        size_t appended = ReadToBuf(buffer.data(), std::min(BUFFER_SIZE, static_cast<size_t>(lStopPos) - pos));

        for (size_t i = 0; i < appended; ++i, ++pos) {
            char ch = buffer[i];
            if (ch == KEY_TOKEN) {
                if (state == 0) {
                    keyPos = pos;
                    state = 1;
                    continue;
                } else {
                    auto nextPos = std::streampos(pos + 1);
                    _file.seekg(nextPos);
                    return std::make_optional(
                        std::make_pair(
                            std::make_pair(key, value),
                            std::make_pair(keyPos, nextPos)
                        )
                    );
                }
            }
            if (ch == VALUE_TOKEN && state == 1) {
                state = 2;
                continue;
            }
            if (state != 0) {
                (state == 1 ? key : value) += ch;
            }
        }
    }

    if (key.empty()) {
        return std::nullopt;
    } else {
        return std::make_optional(
            std::make_pair(
                std::make_pair(key, value),
                std::make_pair(keyPos, pos)
            )
        );
    }
}
