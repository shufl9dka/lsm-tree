#ifndef LSM_TREE_H
#define LSM_TREE_H

#include <string>
#include <vector>
#include "layer.h"

class LSMTree {
public:
    explicit LSMTree(const std::string& dirpath);

    std::string At(const std::string& key);

    void Put(const std::string& key, const std::string& value);

private:
    std::vector<Layer> _levels;
};

#endif
