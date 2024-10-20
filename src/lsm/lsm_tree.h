#ifndef LSM_TREE_H
#define LSM_TREE_H

#include "memtable.h"
#include "sstable.h"

#include <string>
#include <vector>

const size_t MEMTABLE_CAPACITY = 16 * 1024;
const size_t MAX_LEVEL_RUNS = 3;

class LSMTree {
public:
    explicit LSMTree(const std::string& dirpath);

    std::optional<std::string> At(const std::string& key);

    void Put(const std::string& key, const std::string& value);

private:
    void CompactLevel(size_t level);

    std::string GenerateSSTableFilename(size_t level);

    MemTable _memTable;
    std::string _dirpath;
    std::vector<std::vector<SSTable>> _levels;
    std::unordered_map<size_t, size_t> _levelNextRun;
};

#endif
