#ifndef SSTABLE_H
#define SSTABLE_H

#include <string>
#include "bloom/bloom_filter.h"
#include "memtable.h"

const size_t BLOOM_SIZE = 128 * 1024;

class SSTable {
public:
    // loads SSTable from `filepath`
    explicit SSTable(const std::string& filepath);

    // writes memTable to disk at `filepath` file
    SSTable(const std::string& filepath, MemTable&& memTable);

    // merges `toMerge` SSTables into `this` SSTable and writes it to `filepath`
    // `toMerge` is ordered from the least to most recent (toMerge[0] is later then toMerge[1])
    SSTable(const std::string& filepath, const std::vector<SSTable>& toMerge);

    std::string At(const std::string& key);

private:
    BloomFilter<BLOOM_SIZE> _bloom_filter;
    std::string _filepath;
};

#endif
