#include "lsm/lsm_tree.h"

#include <iostream>
#include <chrono>
#include <string>
#include <random>
#include <filesystem>
#include <algorithm>

int main() {
    // Remove any existing LSM tree data from previous runs
    const std::string lsm_dir = "./lsm_test";
    std::filesystem::remove_all(lsm_dir);

    // Set up the LSM Tree
    LSMTree tree(lsm_dir);
    std::cout << "ok" << std::endl;

    const int NUM_INSERTS = 100000; // Number of key-value pairs to insert
    const int NUM_LOOKUPS = 10000;  // Number of key lookups to perform

    // Generate unique keys and corresponding values
    std::vector<std::string> keys;
    std::vector<std::string> values;

    for (int i = 0; i < NUM_INSERTS; ++i) {
        keys.push_back("key" + std::to_string(i));
        values.push_back("value" + std::to_string(i));
    }

    // Shuffle the keys to insert them in random order
    std::mt19937 rng(12345);
    std::shuffle(keys.begin(), keys.end(), rng);

    // Benchmark insertions
    std::cout << "AMOGIUM" << std::endl;
    auto start_insert = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < NUM_INSERTS; ++i) {
        tree.Put(keys[i], values[i]);
    }
    auto end_insert = std::chrono::high_resolution_clock::now();
    auto insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_insert - start_insert);
    std::cout << "Inserted " << NUM_INSERTS << " key-value pairs in " << insert_duration.count() << " ms\n";
    std::cout << "Insertion throughput: " << (NUM_INSERTS * 1000.0 / insert_duration.count()) << " ops/sec\n";

    // Prepare keys for lookup (some existing, some non-existing)
    std::uniform_int_distribution<int> dist(0, NUM_INSERTS - 1);
    std::vector<std::string> lookup_keys;
    int num_existing_keys = NUM_LOOKUPS / 2;

    // Add existing keys to lookup
    for (int i = 0; i < num_existing_keys; ++i) {
        lookup_keys.push_back("key" + std::to_string(dist(rng)));
    }
    // Add non-existing keys to lookup
    for (int i = 0; i < NUM_LOOKUPS - num_existing_keys; ++i) {
        lookup_keys.push_back("nonexistent_key" + std::to_string(dist(rng)));
    }

    // Shuffle lookup keys
    std::shuffle(lookup_keys.begin(), lookup_keys.end(), rng);

    // Benchmark lookups
    int found = 0;
    auto start_lookup = std::chrono::high_resolution_clock::now();
    for (const auto& key : lookup_keys) {
        auto value = tree.At(key);
        if (value.has_value()) {
            ++found;
        }
    }
    auto end_lookup = std::chrono::high_resolution_clock::now();
    auto lookup_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_lookup - start_lookup);
    std::cout << "Looked up " << NUM_LOOKUPS << " keys in " << lookup_duration.count() << " ms\n";
    std::cout << "Found " << found << " keys out of " << NUM_LOOKUPS << "\n";
    std::cout << "Lookup throughput: " << (NUM_LOOKUPS * 1000.0 / lookup_duration.count()) << " ops/sec\n";

    return 0;
}
