#include <iostream>
#include "algorithm/algorithm.h"

int main() {
    std::string dataset = "dblp"; // input dataset expected to have name dblp.csv
    int limit = 0; // this option does not do anything yet.
    run_algorithm(AlgorithmType::PATH_4, dataset, limit);
    return 0;
}

