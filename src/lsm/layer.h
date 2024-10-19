#ifndef LAYER_H
#define LAYER_H

#include <string>
#include <vector>
#include "sstable.h"

class Layer {
public:
    std::vector<SSTable> sstables;

    explicit Layer(const std::string& dirpath);
};

#endif
