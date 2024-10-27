#ifndef SSTABLE_H
#define SSTABLE_H

#include "bloom_filter.h"
#include "memtable.h"

#include <fstream>
#include <string>

const size_t BLOOM_SIZE = 64 * 1024;
const size_t BUFFER_SIZE = 512;

const char KEY_TOKEN = '\0';
const char VALUE_TOKEN = '\1';

class SSTable {
public:
    using TKeyValue = std::pair<std::string, std::string>;
    using TKVPos = std::pair<TKeyValue, std::pair<std::streampos, std::streampos>>;

    // loads SSTable from `filepath`
    explicit SSTable(const std::string& filepath);

    // writes memTable to disk at `filepath` file
    SSTable(const std::string& filepath, MemTable& memTable);

    // merges `toMerge` SSTables into `this` SSTable and writes it to `filepath`
    // `toMerge` is ordered from the least to most recent (toMerge[0] is later then toMerge[1])
    SSTable(const std::string& filepath, std::vector<SSTable>& toMerge);

    std::optional<std::string> At(const std::string& key);

    void Clear();

private:
    void OpenReadFile(std::optional<std::streampos> startPos = std::nullopt);

    void CloseReadFile();

    size_t ReadToBuf(char* buffer, size_t max, std::optional<std::streampos> startPos = std::nullopt);

    std::optional<TKVPos> GetNextKV(std::optional<std::streampos> startPos = std::nullopt, std::optional<std::streampos> stopPos = std::nullopt);

    BloomFilter<BLOOM_SIZE> _bloom_filter;
    std::string _filepath;
    std::ifstream _file;
    std::streampos _stopPos;
};

#endif
