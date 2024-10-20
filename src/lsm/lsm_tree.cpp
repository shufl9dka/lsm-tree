#include "lsm_tree.h"
#include "sstable.h"

#include <iostream>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <sstream>
#include <sys/stat.h>
#include <unordered_map>

bool directoryExists(const std::string& path) {
    struct stat info;
    if (stat(path.c_str(), &info) != 0) {
        return false;
    } else if (info.st_mode & S_IFDIR) {
        return true;
    }
    return false;
}

LSMTree::LSMTree(const std::string& dirpath) : _dirpath(dirpath) {
    const std::filesystem::path dir = _dirpath;
    if (!directoryExists(dir)) {
        std::filesystem::create_directories(dir);
    }

    std::vector<std::tuple<size_t, size_t, std::string>> sortedRuns; // need one because we have to maintain recents order
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.is_regular_file()) {
            unsigned int level, run;
            const std::string filename = entry.path().filename().string();

            if (sscanf(filename.c_str(), "L%uR%u.sst", &level, &run) == 2) {    // L<level>R<run>.sst
                while (_levels.size() <= level) {
                    _levels.emplace_back();
                }

                if (_levelNextRun[level] <= run) {
                    _levelNextRun[level] = run + 1;
                }

                sortedRuns.push_back(std::make_tuple(level, run, entry.path().string()));
            }
        }
    }

    std::sort(sortedRuns.begin(), sortedRuns.end());
    for (const auto& [level, run, filepath] : sortedRuns) {
        _levels[level].push_back(SSTable(filepath));
    }
}

std::optional<std::string> LSMTree::At(const std::string& key) {
    auto value = _memTable.At(key);
    if (value.has_value()) {
        return value;
    }

    for (auto& level : _levels) {
        for (size_t ri = level.size(); ri > 0; --ri) {
            auto val = level[ri - 1].At(key);
            if (val.has_value()) {
                return val;
            }
        }
    }

    return std::nullopt;
}

void LSMTree::Put(const std::string& key, const std::string& value) {
    size_t memSize = _memTable.Put(key, value);

    if (memSize >= MEMTABLE_CAPACITY) {
        if (_levels.empty()) {
            _levels.emplace_back();
        }

        std::string filepath = _dirpath + "/" + GenerateSSTableFilename(0);
        _levels[0].emplace_back(filepath, _memTable);

        std::cout << "gaaaay" << std::endl;
        CompactLevel(0);
        std::cout << "down" << std::endl;
    }
}

std::string LSMTree::GenerateSSTableFilename(size_t level) {
    size_t run_number = _levelNextRun[level]++;
    return "L" + std::to_string(level) + "R" + std::to_string(run_number) + ".sst";
}

void LSMTree::CompactLevel(size_t level) {
    if (level >= _levels.size()) {
        std::cout << "nolevel" << std::endl;
        return;
    }

    std::cout << "checker" << std::endl;
    if (_levels[level].size() >= MAX_LEVEL_RUNS) {
        std::cout << "compaaact" << std::endl;
        std::vector<SSTable> toMerge;
        for (size_t i = 0; i < MAX_LEVEL_RUNS; ++i) {
            toMerge.push_back(std::move(_levels[level][i]));
        }
        _levels[level].erase(_levels[level].begin(), _levels[level].begin() + MAX_LEVEL_RUNS);

        std::string filename = GenerateSSTableFilename(level + 1);
        std::string filepath = _dirpath + "/" + filename;

        if (_levels.size() <= static_cast<size_t>(level + 1)) {
            _levels.emplace_back();
        }
        std::cout << "viva";
        SSTable newTable(filepath, toMerge);
        std::cout << "giga?" << std::endl;
        _levels[level + 1].push_back(std::move(newTable));
        std::cout << "jija" << std::endl;
        CompactLevel(level + 1);
    }
}
