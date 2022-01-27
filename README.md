# Ranked enumeration of Join Queries with Projection
This project shows how to perform ranked enumeration for join queries with projections efficiently.

## Programming Language and Datasets
The source code is written in C++ and the CMake file is included to show what compilation flags have been used. Two datasets, dblp and imdb, have also been provided. The format for these datasets is the following: each line in the file represents a tuple (a,b). The vertex id is used as the weight for the ranking function. The code assumes that we perform a self-join on the same relation for different type of queries.

## Detailed Instructions
Install g++ on the Ubunutu machine using the command ``sudo apt install g++``. Check that g++ is installed correctly using ``g++ --version``. There are no other dependencies required. Make sure that the data file is present in same directory as the main function. The format of the data file must be as described above. The utils/config.h file contains the list of algorithms implemented for the specific queries. The user should fix an algorithm by changing the ``AlgorithmType`` enum on line 7 in the main.cpp file. Then, you can run the following command to generate the ``main.out`` file

``g++ main.cpp utils/config.h algorithm/algorithm.h algorithm/algorithm.cpp -std=c++14 -O3 -Ofast -march=native -mfma -mavx -fomit-frame-pointer -ffp-contract=fast -flto -DARMA_NO_DEBUG``

Finally, use ``./main.out`` to run the program. For users who prefer using IDE, a sample CMakeLists.txt has been provided that can be used with CLion.

If the user wants to implement the algorithm for a new query, they must add the algorithm name in the enum in utils/config.h. The algorithm itself should be implemented in algorithm/algorithm.h file and the glue code to invoke it must be added in algorithm/algorithm.cpp file.
