#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <optional>
#include <string>
#include <unordered_map>

class MemTable {
public:
    std::optional<std::string> At(const std::string& key) {
        if (!_map.contains(key)) {
            return std::nullopt;
        }
        return _map[key];
    }

    size_t Put(const std::string& key, const std::string& value) {
        _map[key] = value;
        return _map.size();
    }

    friend class SSTable;

private:
    std::unordered_map<std::string, std::string> _map;
};

#endif
