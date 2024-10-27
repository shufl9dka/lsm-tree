#include "lsm/lsm_tree.h"

#include <iostream>
#include <chrono>
#include <string>
#include <random>
#include <filesystem>
#include <algorithm>

std::mt19937 rng(42);

auto prepareKeysValues(size_t nRuns) {
    std::vector<std::string> keys, values;
    for (size_t i = 0; i < nRuns; ++i) {
        keys.push_back("key" + std::to_string(i));
        values.push_back("value" + std::to_string(i));
    }

    std::shuffle(keys.begin(), keys.end(), rng);
    std::shuffle(values.begin(), values.end(), rng);
    return std::make_pair(keys, values);
}

int main(int argc, char *argv[]) {
    int nRuns = std::atoi(argv[1]);
    int nLookups = std::atoi(argv[2]);
    std::cout << "nRuns = " << nRuns << ", nLookups = " << nLookups << std::endl;

    const std::string testDir = "./lsm_test";
    std::filesystem::remove_all(testDir);
    LSMTree tree(testDir);

    auto [keys, values] = prepareKeysValues(nRuns);
    std::cout << keys.size() << " " << values.size() << std::endl;

    std::cout << "Starting insert phase" << std::endl;
    auto startInsert = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < nRuns; ++i) {
        tree.Put(keys[i], values[i]);
    }
    auto endInsert = std::chrono::high_resolution_clock::now();
    auto insertPhase = std::chrono::duration_cast<std::chrono::milliseconds>(endInsert - startInsert);

    std::cout << "Inserted " << nRuns << " key-value pairs in " << insertPhase.count() << " ms" << std::endl;
    std::cout << "Insertion throughput: " << (nRuns * 1000.0 / insertPhase.count()) << " ops/sec" << std::endl;

    size_t found = 0;
    size_t incorrect = 0;
    auto startLookup = std::chrono::high_resolution_clock::now();
    for (size_t step = 0; step < nLookups; ++step) {
        size_t i = rng() % nRuns;
        auto value = tree.At(keys[i]);
        if (value.has_value()) {
            ++found;
            incorrect += static_cast<size_t>(*value != values[i]);
        }
    }
    auto endLookup = std::chrono::high_resolution_clock::now();
    auto lookupPhase = std::chrono::duration_cast<std::chrono::milliseconds>(endLookup - startLookup);

    std::cout << "Looked up " << nLookups << " keys in " << lookupPhase.count() << " ms" << std::endl;
    std::cout << "Found " << found << " of " << nLookups << " keys, " << incorrect << " has incorrect values" << std::endl;
    std::cout << "Lookup throughput: " << (nLookups * 1000.0 / lookupPhase.count()) << " ops/sec" << std::endl;

    return 0;
}
