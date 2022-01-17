

#ifndef RANKEDLIMIT_ALGORITHM_H
#define RANKEDLIMIT_ALGORITHM_H
#include <vector>
#include <iostream>
#include <unordered_map>
#include <string>
#include <queue>
#include <algorithm>
#include <set>
#include <bitset>
#include <sstream>
#include <limits.h>
#include "../utils/config.h"
#include <sys/time.h>
#include <fstream>
using namespace std;

#if __APPLE__
const string BASE = "/Users/shaleen/Downloads/";
#else
const string BASE = "";
#endif

#define COUT if (0) cout

class Algorithm {
public:
    unordered_map<int, vector<int>> x_to_y;
    unordered_map<int, vector<int>> y_to_x;
    int space_used_bytes;
    set<long long int> checkpoints { 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 200000000 , 300000000,
                                     400000000, 500000000, 1000000000,
                                     10000000000, 50000000000, 100000000000, 200000000000 };

    void load(std::string dataset) {
        std::cout << "starting load of dataset: " << dataset << std::endl;
        std::ifstream file(BASE + dataset + ".csv");
        std::string   line;
        unordered_map<int, std::set<int>> temp_map;
        while(std::getline(file, line)) {
            std::stringstream  lineStream(line);
            int x, y; char ch;
            while(lineStream >> x >> ch >> y) {
                temp_map[x].insert(y);
            }
        }
        for (auto& it : temp_map) {
            std::vector<int> v;
            v.assign(it.second.begin(), it.second.end());
            x_to_y[it.first] = v;
            std::sort(x_to_y[it.first].begin(), x_to_y[it.first].end());
        }
        for (auto&it : x_to_y) {
            for (auto&val : it.second) {
                y_to_x[val].push_back(it.first);
            }
        }
        for(auto& it : y_to_x) {
            std::sort(it.second.begin(), it.second.end());
        }
        std::cout << "completed load of dataset: " << dataset << std::endl;
    }
    // Return the intersections for all workload queries
    virtual void execute(std::string dataset, int limit) = 0;
    virtual std::string name() = 0;
};

void run_algorithm(AlgorithmType algo, std::string dataset, int limit = 0);

class BFS_THREEPATH : public Algorithm {
public:

    BFS_THREEPATH() {};

    virtual std::string name() {
        return "bfs 3 path";
    }

    virtual void execute(std::string dataset, int limit) {
        cout << "executing " << name() << endl;
        timeval starting, ending, s1, t1, s2, t2;
        gettimeofday(&starting, NULL);
        load(dataset);
        unordered_map<int, set<int>> x_to_x_two_path;

        for (auto&it : x_to_y) {
            for (auto &val : it.second) {
                for (auto& val2 : y_to_x[val]) {
                    x_to_x_two_path[it.first].insert(val2);
                }
            }
        }

        unordered_map<int, set<int>> x_to_y_three_path;

        for (auto&it : x_to_x_two_path) {
            for(auto&it2 : it.second) {
                for (auto &val : x_to_y[it2]) {
                    x_to_y_three_path[it.first].insert(val);
                }
            }
        }

        gettimeofday(&s1, NULL);

        vector<pair<int, int>> materialized;

        for(auto& it : x_to_y_three_path) {
            for(auto&val : it.second) {
                materialized.push_back(make_pair(it.first + val, it.first));
            }
        }

        gettimeofday(&s2, NULL);
        cout << "sorting materialized" << endl;
        sort(materialized.begin(), materialized.end());
        cout << "sorting materialized done " << materialized.size() << endl;
        gettimeofday(&ending, NULL);
        cout << "Preprocessing: " << s1.tv_sec - starting.tv_sec + (s1.tv_usec - starting.tv_usec) / 1e6 << endl;
        cout << "Materializing: " << s2.tv_sec - s1.tv_sec + (s2.tv_usec - s1.tv_usec) / 1e6 << endl;
        cout << "Sorting: " << ending.tv_sec - s2.tv_sec + (ending.tv_usec - s2.tv_usec) / 1e6 << endl;
        cout << "All: " << ending.tv_sec - starting.tv_sec + (ending.tv_usec - starting.tv_usec) / 1e6 << endl;
        cout << "end executing " << name() << endl;
    }

};

class BFS_FOURPATH : public Algorithm {
public:

    BFS_FOURPATH() {};

    virtual std::string name() {
        return "bfs 4 path";
    }

    virtual void execute(std::string dataset, int limit) {
        cout << "executing " << name() << endl;
        load(dataset);
        timeval starting, ending, s1, t1, s2, t2;
        gettimeofday(&starting, NULL);
        unordered_map<int, set<int>> x_to_x_two_path;

        for (auto&it : x_to_y) {
            for (auto &val : it.second) {
                for (auto& val2 : y_to_x[val]) {
                    x_to_x_two_path[it.first].insert(val2);
                }
            }
        }

        unordered_map<int, set<int>> x_to_y_three_path;

        for (auto&it : x_to_x_two_path) {
            for(auto&it2 : it.second) {
                for (auto &val : x_to_y[it2]) {
                    x_to_y_three_path[it.first].insert(val);
                }
            }
        }

        unordered_map<int, set<int>> x_to_x_four_path;

        for (auto&it : x_to_y_three_path) {
            for (auto &val : it.second) {
                for (auto& val2 : y_to_x[val]) {
                    x_to_x_four_path[it.first].insert(val2);
                }
            }
        }

        gettimeofday(&s1, NULL);

        vector<pair<int, int>> materialized;

        for(auto& it : x_to_x_four_path) {
            for(auto&val : it.second) {
                materialized.push_back(make_pair(it.first + val, it.first));
            }
        }

        gettimeofday(&s2, NULL);
        cout << "sorting materialized" << endl;
        sort(materialized.begin(), materialized.end());
        cout << "sorting materialized done " << materialized.size() << endl;
        gettimeofday(&ending, NULL);
        cout << "Preprocessing: " << s1.tv_sec - starting.tv_sec + (s1.tv_usec - starting.tv_usec) / 1e6 << endl;
        cout << "Materializing: " << s2.tv_sec - s1.tv_sec + (s2.tv_usec - s1.tv_usec) / 1e6 << endl;
        cout << "Sorting: " << ending.tv_sec - s2.tv_sec + (ending.tv_usec - s2.tv_usec) / 1e6 << endl;
        cout << "All: " << ending.tv_sec - starting.tv_sec + (ending.tv_usec - starting.tv_usec) / 1e6 << endl;
        cout << "end executing " << name() << endl;
    }

};

class PATH_THREE : public Algorithm {
public:

    PATH_THREE() {};

    virtual std::string name() {
        return "path 3";
    }

    virtual void execute(std::string dataset, int limit) {
        cout << "executing " << name() << endl;
        timeval starting, ending, s1, t1, s2, t2, temp;
        gettimeofday(&starting, NULL);
        load(dataset);
        std::vector<int> new_candidate_count_vec;
        int counter = 1;
        unordered_map<long long int, pair<long long int, long long int>> time_space;
        int tot_size = 0;
        for (auto&it : x_to_y) {
            for (auto&val : it.second) {
                ++tot_size;
            }
        }
        new_candidate_count_vec.reserve(tot_size);
        for(auto& it : y_to_x) {
            for (auto& it2 : it.second) {
                new_candidate_count_vec.push_back(0);
            }
        }

        priority_queue<tuple<unsigned long, int, int, int, int, bool, int>, vector<tuple<unsigned long, int, int, int, int, bool, int>>, greater<tuple<unsigned long, int, int, int, int, bool, int>>> pq;
        int vec_counter = 0;
        for(auto& it : y_to_x) {
            for (auto& it2 : it.second) {
                unsigned long a, b;
                bool left_to_right;
                if (it.second.size() <= x_to_y[it2].size()) {
                    a = y_to_x[it.first][0];
                    b = x_to_y[it2][0];
                    left_to_right = true;
                } else {
                    b = x_to_y[it2][0];
                    a = y_to_x[it.first][0];
                    left_to_right = false;
                }
                auto t = std::make_tuple(a+b,a,it.first, it2, 1, left_to_right, vec_counter);
                new_candidate_count_vec.at(vec_counter) += 1;
                ++vec_counter;
                //cout << std::get<0>(t) << " " << std::get<1>(t) << " " << std::get<2>(t) << " " << std::get<3>(t) << endl;
                pq.push(t);
            }
        }
        cout << pq.size() << endl;
        tuple<unsigned long, int, int, int, int, bool, int> last = std::make_tuple(-1,-1,-1,-1, -1, false, -1);

        gettimeofday(&s1, NULL);

        while(!pq.empty()) {
            const auto& t = pq.top();
            pq.pop();
            const auto y =  std::get<2>(t);
            const auto x =  std::get<3>(t);
            const auto a =  std::get<1>(t);
            const auto b = std::get<0>(t) - a;
            const auto index = std::get<4>(t);
            const auto left_or_right = std::get<5>(t);
            const auto vec_counter = std::get<6>(t);

            if (!(std::get<0>(t) == std::get<0>(last) && std::get<1>(t) == std::get<1>(last))) {
                // cout << std::get<0>(t) << " " << std::get<1>(t) << " " << std::get<2>(t) << " " << std::get<3>(t) << endl;
                counter += 1;
                // cout << counter << endl;
                if (checkpoints.find(counter) != checkpoints.end()) {
                    gettimeofday(&temp, NULL);
                    long long int timediff = temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                    pair<long long int, long long int> p = make_pair(timediff, pq.size());
                    time_space[counter] = p;
                    cout << counter << " tuples in time and space " << timediff << " " << pq.size() << endl;
                }
            }
            last = t;
            if (left_or_right) {
                if (index < x_to_y[x].size()) {
                    unsigned long new_b = x_to_y[x][index];
                    auto tup = std::make_tuple(a+new_b,a, y, x, index+1, left_or_right, vec_counter);
                    //cout << "pushed in first" << a << " " << a+new_b << " " << pointers[y][x][a] << endl;
                    pq.push(tup);
                }

                if (new_candidate_count_vec.at(vec_counter) < y_to_x[y].size()) {
                    unsigned long new_a = y_to_x[y][new_candidate_count_vec.at(vec_counter)];
                    unsigned long b_for_new_a = x_to_y[x][0];
                    new_candidate_count_vec.at(vec_counter) += 1;
                    auto new_tup = std::make_tuple(new_a+b_for_new_a,new_a, y, x, 1, left_or_right, vec_counter);
                    //cout << "pushed in second" << new_a << " " << new_a+b_for_new_a << " " << pointers[y][x][new_a] <<  endl;
                    pq.push(new_tup);
                }
            } else {
                if (index < y_to_x[y].size()) {
                    unsigned long new_a = y_to_x[y][index];
                    auto tup = std::make_tuple(new_a+b,new_a, y, x, index+1, left_or_right, vec_counter);
                    pq.push(tup);
                }

                if (new_candidate_count_vec.at(vec_counter) < x_to_y[x].size()) {
                    unsigned long new_b = x_to_y[x][new_candidate_count_vec.at(vec_counter)];
                    unsigned long a_for_new_b = y_to_x[y][0];
                    new_candidate_count_vec.at(vec_counter) += 1;
                    auto new_tup = std::make_tuple(new_b+a_for_new_b,a_for_new_b, y, x, 1, left_or_right, vec_counter);
                    pq.push(new_tup);
                }

            }
        }
        cout << "number of tuples enumerated : " << counter << endl;
        gettimeofday(&ending, NULL);
        cout << "All: " << ending.tv_sec - starting.tv_sec + (ending.tv_usec - starting.tv_usec) / 1e6 << endl;
        cout << "end executing " << name() << endl;
    }

};

class PATH_FOUR : public Algorithm {
public:

    PATH_FOUR() {};

    virtual std::string name() {
        return "path 4";
    }

    virtual void execute(std::string dataset, int limit) {
        cout << "executing " << name() << endl;
        timeval starting, ending, s1, t1, s2, t2, temp;
        gettimeofday(&starting, NULL);
        load(dataset);
        unordered_map<int, vector<int>> x_to_x_mat;
        std::vector<int> new_candidate_count_vec_x_to_x;
        unordered_map<int, bool> x_finished;
        unordered_map<long long int, pair<long long int, long long int>> time_space;
        new_candidate_count_vec_x_to_x.reserve(x_to_y.size());
        for (auto&it : x_to_y) {
            x_finished[it.first] = false;
        }
        cout << x_to_y.size() << endl;
        std::unordered_map<int, priority_queue<tuple<unsigned long, int, int, int>,
                vector<tuple<unsigned long, int, int, int>>,
                greater<tuple<unsigned long, int, int, int>>>> pq_x_to_y;

        for(auto& it : x_to_y) {
            for (auto& it2 : it.second) {
                auto t = std::make_tuple(y_to_x[it2][0], it.first, it2, 1);
                COUT << std::get<0>(t) << " " << std::get<1>(t) << " " << std::get<2>(t) << " " << std::get<3>(t) << endl;
                pq_x_to_y[it.first].push(t);
            }
        }

        for(auto& it : x_to_y) {
            int new_candidate = -1;
            const auto current = pq_x_to_y[it.first].top();
            COUT << "current : x " << std::get<0>(current) << " x : " << std::get<1>(current)
                 <<  " y " << std::get<2>(current) << "  index  " << std::get<3>(current) << endl;
            COUT << "new_candidate : " << new_candidate << endl;
            pq_x_to_y[it.first].pop();
            new_candidate = std::get<0>(current);
            x_to_x_mat[it.first].push_back(new_candidate);
            const auto y = std::get<2>(current);
            const auto index = std::get<3>(current);
            if (index < y_to_x[y].size()) {
                // COUT << index << " " << y_to_x[y].size() << endl;
                auto t = std::make_tuple(y_to_x[y].at(index) , it.first, y, index+1);
                COUT << "pushed : x " << std::get<0>(t) << " x : " << std::get<1>(t)
                     <<  " y " << std::get<2>(t) << "  index  " << std::get<3>(t) << endl;
                pq_x_to_y[it.first].push(t);
            }
            COUT << " next candidate for x : " << it.first << " is " << new_candidate << endl;
            auto local = pq_x_to_y[it.first].top();
            COUT << "local top " << std::get<0>(local) << endl;
            while(!pq_x_to_y[it.first].empty() && new_candidate == std::get<0>(local)) {
                COUT << "local top " << std::get<0>(local) << endl;
                const auto localy = std::get<2>(local);
                const auto localindex = std::get<3>(local);
                if (localindex < y_to_x[localy].size()) {
                    auto localt = std::make_tuple(y_to_x[localy].at(localindex), it.first, localy, localindex + 1);
                    pq_x_to_y[it.first].push(localt);
                    COUT << "pushed : x " << std::get<0>(localt) << " x : " << std::get<1>(localt)
                         <<  " y " << std::get<2>(localt) << "  index  " << std::get<3>(localt) << endl;
                }
                pq_x_to_y[it.first].pop();
                local = pq_x_to_y[it.first].top();
            }
            COUT << "for x : " << it.first << " x_mat size " << x_to_x_mat[it.first].size() << endl;
        }
        priority_queue<tuple<unsigned long, int, int, int, int>,
                vector<tuple<unsigned long, int, int, int, int>>,
                greater<tuple<unsigned long, int, int, int, int>>> pq;
        int vec_counter = 0;
        for (auto& it : x_to_y) {
            new_candidate_count_vec_x_to_x.push_back(0);
            unsigned long a = x_to_x_mat[it.first][new_candidate_count_vec_x_to_x.at(vec_counter)]; // a fixed and move over b
            unsigned long b = x_to_x_mat[it.first][new_candidate_count_vec_x_to_x.at(vec_counter)];
            auto t = std::make_tuple(a+b, a, it.first, 1, vec_counter);
            COUT << a+b << " " << a << " for x : " << it.first << endl;
            new_candidate_count_vec_x_to_x.at(vec_counter) += 1;
            pq.push(t);
            ++vec_counter;
        }

        cout << pq.size() << endl;
        tuple<unsigned long, int, int, int, int> last = std::make_tuple(0,-1,-1,-1, -1);

        gettimeofday(&s1, NULL);
        int counter = 1;
        while(!pq.empty()) {
            const auto t = pq.top();
            pq.pop();
            const auto index =  std::get<3>(t);
            const auto x =  std::get<2>(t);
            const auto a =  std::get<1>(t);
            const auto vec_counter = std::get<4>(t);

            if (!(std::get<0>(t) == std::get<0>(last) && std::get<1>(t) == std::get<1>(last))) {
                COUT << " output " << std::get<0>(t) << " " << std::get<1>(t)
                     << " " << std::get<2>(t) << " " << std::get<3>(t) << endl;
                counter += 1;
                if (checkpoints.find(counter) != checkpoints.end()) {
                    gettimeofday(&temp, NULL);
                    long long int timediff = temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                    pair<long long int, long long int> p = make_pair(timediff, pq.size());
                    time_space[counter] = p;
                    cout << counter << " tuples in time and space " << timediff << " " << pq.size() << endl;
                }
            }
            last = t;
            if (!x_finished[x]) {
                // pop more stuff for x
                COUT << x << " not finished and pq[x] status : " << pq_x_to_y[x].empty() << " index : " << index <<   endl;
                if (index == x_to_x_mat[x].size() && !pq_x_to_y[x].empty()) {
                    COUT << "end of index" << endl;
                    int new_candidate = -1;
                    const auto current = pq_x_to_y[x].top();
                    COUT << "current : x " << std::get<0>(current) << " x : " << std::get<1>(current)
                         <<  " y " << std::get<2>(current) << "  index  " << std::get<3>(current) << endl;
                    COUT << "new_candidate : " << new_candidate << endl;
                    pq_x_to_y[x].pop();
                    new_candidate = std::get<0>(current);
                    x_to_x_mat[x].push_back(new_candidate);
                    const auto y = std::get<2>(current);
                    const auto index = std::get<3>(current);
                    if (index < y_to_x[y].size()) {
                        // COUT << index << " " << y_to_x[y].size() << endl;
                        auto t = std::make_tuple(y_to_x[y].at(index) , x, y, index+1);
                        COUT << "pushed : x " << std::get<0>(t) << " x : " << std::get<1>(t)
                             <<  " y " << std::get<2>(t) << "  index  " << std::get<3>(t) << endl;
                        pq_x_to_y[x].push(t);
                    }
                    COUT << " next candidate for x : " << x << " is " << new_candidate << endl;
                    if (!pq_x_to_y[x].empty()) {
                        auto local = pq_x_to_y[x].top();
                        while(!pq_x_to_y[x].empty() && new_candidate == std::get<0>(local)) {
                            COUT << "local top " << std::get<0>(local) << endl;
                            const auto localy = std::get<2>(local);
                            const auto localindex = std::get<3>(local);
                            if (localindex < y_to_x[localy].size()) {
                                auto localt = std::make_tuple(y_to_x[localy].at(localindex), x, localy, localindex + 1);
                                pq_x_to_y[x].push(localt);
                                COUT << "pushed : x " << std::get<0>(localt) << " x : " << std::get<1>(localt)
                                     <<  " y " << std::get<2>(localt) << "  index  " << std::get<3>(localt) << endl;
                            }
                            pq_x_to_y[x].pop();
                            local = pq_x_to_y[x].top();
                        }
                    } else {
                        COUT << "finished " << x << endl;
                        x_finished[x] = true;
                    }
                }
                if (pq_x_to_y[x].empty()) {
                    x_finished[x] = true;
                }
            }
            COUT << " mater size of x : " << x << " is " << x_to_x_mat[x].size() << endl;
            if (index < x_to_x_mat[x].size()) {
                unsigned long new_b = x_to_x_mat[x][index];
                auto tup = std::make_tuple(a + new_b, a, x, index+1, vec_counter);
                COUT << "pushed new candidate " << a+new_b << " " << a << " " << endl;
                pq.push(tup);
            }
            if (new_candidate_count_vec_x_to_x.at(vec_counter) < x_to_x_mat[x].size()) {
                unsigned long new_a = x_to_x_mat[x][new_candidate_count_vec_x_to_x.at(vec_counter)];
                unsigned long b_for_new_a = x_to_x_mat[x][0];
                new_candidate_count_vec_x_to_x.at(vec_counter) += 1;
                auto new_tup = std::make_tuple(new_a+b_for_new_a,new_a, x, 1, vec_counter);
                COUT << "pushed in second" << new_a+b_for_new_a << " " << new_a <<   endl;
                pq.push(new_tup);
            }
        }
        cout << "number of tuples enumerated : " << counter << endl;
        gettimeofday(&ending, NULL);
        cout << "All: " << ending.tv_sec - starting.tv_sec + (ending.tv_usec - starting.tv_usec) / 1e6 << endl;
        for(auto& it : time_space) {
            cout << it.first << " tuples in time and space " << it.second.first << " " << it.second.second << endl;
        }
        cout << "end executing " << name() << endl;
    }

};

class STAR_TWO : public Algorithm {


public:
    unsigned long degree;

    STAR_TWO(unsigned long degree_input) {degree = degree_input;}

    virtual std::string name() {
        return "star 2";
    }

    virtual void execute(std::string dataset, int limit) {
            cout << "executing " << name() << endl;
            timeval starting, ending, s1, t1, s2, t2, temp;
            gettimeofday(&starting, NULL);
            load(dataset);
            unordered_map<int, vector<int>> x_pointers;
            x_pointers.reserve(x_to_y.size());
            for (auto &it : x_to_y) {
                for (auto &val : it.second) {
                    x_pointers[it.first].push_back(0);
                }
            }
            unordered_map<long long int, pair<long long int, long long int>> time_space;
            int pq_size = 0;

            vector<pair<int, int>> materialized;
            unsigned long materialized_pointer = 0;
            for (auto &it : x_to_y) {
                if (it.second.size() <= degree) {
                    continue;
                }

                set<int> dedup;
                for (int val : it.second) {
                    vector<int> &z = y_to_x[val];
                    for (int val_z : z) {
                        dedup.insert(val_z);
                    }
                }

                for (int val : dedup) {
                    materialized.push_back(make_pair(it.first + val, it.first));
                }
            }

            cout << "sorting materialized" << endl;
            sort(materialized.begin(), materialized.end());
            cout << "sorting materialized done" << endl;

            for (auto it = x_to_y.begin(); it != x_to_y.end();) {
                if (it->second.size() > degree) {
                    // cout << "deleting!" << endl;
                    it = x_to_y.erase(it);
                } else {
                    it++;
                }
            }

            gettimeofday(&s1, NULL);
            cout << "Preprocessing: " << s1.tv_sec - starting.tv_sec + (s1.tv_usec - starting.tv_usec) / 1e6 << endl;

            int counter = 0, c = 0;
            priority_queue<pair<int, int>, vector<pair<int, int>>, greater<pair<int, int>>> pq;
            for (auto &it: x_to_y) {
                int smallest = INT32_MAX;
                int candidate = -1;
                for (int pos = 0; pos < it.second.size(); ++pos) {

                    candidate = y_to_x[it.second[pos]][x_pointers[it.first][pos]];
                    if (candidate < smallest) {
                        smallest = candidate;
                    }
                }
                pq.push(make_pair((it.first + smallest), it.first));
            }
            vector<int> out;
            while ((!pq.empty() || materialized_pointer < materialized.size())) {
                if (pq.size() > pq_size) {
                    pq_size = pq.size();
                }

                pair<int, int> return_output;
                if (!pq.empty()) {
                    return_output = pq.top();
                }


                if (materialized_pointer < materialized.size() && pq.empty()) {
                    materialized_pointer += 1;
                    counter += 1;
                    if (checkpoints.find(counter) != checkpoints.end()) {
                        gettimeofday(&temp, NULL);
                        long long int timediff =
                                temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                        pair<long long int, long long int> p = make_pair(timediff, pq.size());
                        time_space[counter] = p;
                        cout << counter << " tuples in time and space " << timediff << " " << pq.size() << endl;
                    }
                    out.push_back(1);
                    out.push_back(1);
                    continue;
                } else {
                    if (materialized_pointer < materialized.size() &&
                        materialized[materialized_pointer].first < return_output.first) {
                        materialized_pointer += 1;
                        counter += 1;
                        out.push_back(1);
                        out.push_back(1);
                        continue;
                    }
                }
                pq.pop();
                counter += 1;
                out.push_back(return_output.first);
                out.push_back(return_output.second);
                int smallest = (return_output.first - return_output.second);
                for (int pos = 0; pos < x_to_y[return_output.second].size(); ++pos) {
                    if (smallest == y_to_x[x_to_y[return_output.second][pos]][x_pointers[return_output.second][pos]]) {
                        x_pointers[return_output.second][pos] += 1;
                    }
                }

                smallest = INT32_MAX;
                int candidate = -1;
                for (int pos = 0; pos < x_to_y[return_output.second].size(); ++pos) {
                    if (x_pointers[return_output.second][pos] >= y_to_x[x_to_y[return_output.second][pos]].size())
                        continue;
                    c+=1;
                    candidate = y_to_x[x_to_y[return_output.second][pos]][x_pointers[return_output.second][pos]];
                    if (candidate < smallest) {
                        smallest = candidate;
                    }
                }
                if (smallest != INT32_MAX) {
                    pq.push(make_pair((return_output.second + smallest), return_output.second));
                }
            }


            cout << "number of tuples enumerated : " << counter << " " << c << endl;
            gettimeofday(&ending, NULL);
            cout << "materialized size : " << materialized.size() << endl;
            cout << "All: " << ending.tv_sec - starting.tv_sec + (ending.tv_usec - starting.tv_usec) / 1e6 << endl;
            for (auto &it : time_space) {
                cout << it.first << " tuples in time and space " << it.second.first << " " << it.second.second << endl;
            }
            cout << "end executing " << name() << endl;
    }

};

class STAR_TWO_OPT : public Algorithm {

public:
    unsigned long degree;
    int min_pop = INT_MAX;
    int max_pop = 0;
    double avg_pop = 0.0;
    int tot_pop = 0;
    unordered_map<int, int> cdf_pop;

    STAR_TWO_OPT(unsigned long degree_input) {degree = degree_input;}

    virtual std::string name() {
        return "star 2 optimized";
    }

    virtual void execute(std::string dataset, int limit) {
        cout << "executing " << name() << endl;
        timeval starting, ending, s1, t1, s2, t2, temp;
        load(dataset);
        gettimeofday(&starting, NULL);
        unordered_map<long long int, pair<double, long long int>> time_space;
        int pq_size = 0;

        vector<pair<int, int>> materialized;
        unsigned long materialized_pointer = 0;
        for (auto &it : x_to_y) {
            if (it.second.size() <= degree) {
                continue;
            }

            set<int> dedup;
            for (int val : it.second) {
                vector<int> &z = y_to_x[val];
                for (int val_z : z) {
                    dedup.insert(val_z);
                }
            }

            for (int val : dedup) {
                materialized.push_back(make_pair(it.first + val, it.first));
            }
        }

        cout << "sorting materialized" << endl;
        sort(materialized.begin(), materialized.end());
        cout << "sorting materialized done" << endl;

        for (auto it = x_to_y.begin(); it != x_to_y.end();) {
            if (it->second.size() > degree) {
                // cout << "deleting!" << endl;
                it = x_to_y.erase(it);
            } else {
                it++;
            }
        }

        gettimeofday(&s1, NULL);
        cout << "Preprocessing: " << s1.tv_sec - starting.tv_sec + (s1.tv_usec - starting.tv_usec) / 1e6 << endl;

        int counter = 0, c = 0, pop = 0;
        priority_queue<tuple<int, int, int, int>, vector<tuple<int, int, int, int>>, greater<tuple<int, int, int, int>>> pq;
        for (auto &it: x_to_y) {
            for (auto& val : it.second) {
                pq.push(make_tuple(it.first + y_to_x[val][0], it.first, val, 0));
            }
        }
        vector<int> out;
        tuple<int, int, int, int> last = make_tuple(-1,-1,-1,-1);
        cout << "pq size : " << pq.size() << endl;
        while ((!pq.empty() || materialized_pointer < materialized.size())) {
            if (pq.size() > pq_size) {
                pq_size = pq.size();
            }
            tuple<int, int, int, int> return_output;
            if (!pq.empty()) {
                return_output = pq.top();
            }
            if (materialized_pointer < materialized.size() && pq.empty()) {
                materialized_pointer += 1;
                counter += 1;
                if (checkpoints.find(counter) != checkpoints.end()) {
                    gettimeofday(&temp, NULL);
                    double timediff =
                            temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                    pair<double, long long int> p = make_pair(timediff, pq.size());
                    time_space[counter] = p;
                    cout << counter << " tuples in time and space " << timediff << " " << pq.size() << endl;
                }
                continue;
            } else {
                if (materialized_pointer < materialized.size() &&
                    materialized[materialized_pointer].first < std::get<0>(return_output)) {
                    materialized_pointer += 1;
                    counter += 1;
		    if (checkpoints.find(counter) != checkpoints.end()) {
                    gettimeofday(&temp, NULL);
                    long long int timediff =
                            temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                    pair<double, long long int> p = make_pair(timediff, pq.size());
                    time_space[counter] = p;
                    cout << counter << " tuples in time and space " << timediff << " " << pq.size() << endl;
                }
                    continue;
                }
            }
            pq.pop(); pop++; tot_pop++;
            if (std::get<0>(last) != std::get<0>(return_output) || std::get<1>(last) != std::get<1>(return_output)) {
//                cout << std::get<0>(last) << " " <<  std::get<0>(return_output) << " "
//                        << std::get<1>(last) << " " << std::get<1>(return_output) << endl;
                if (checkpoints.find(counter) != checkpoints.end()) {
                    gettimeofday(&temp, NULL);
                    double timediff =
                            temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                    pair<double, long long int> p = make_pair(timediff, pq.size());
                    time_space[counter] = p;
                    cout << counter << " tuples in time and space " << timediff << " " << pq.size() << endl;
                }
		last = return_output;
                counter++;
		min_pop = std::min(min_pop, pop);
		max_pop = std::max(max_pop, pop);
		avg_pop = ((double) tot_pop ) / counter;
		cdf_pop[pop] += 1;
		pop = 0;
		if (counter % 10000000 == 0) {
			cout << "avg pop after " << counter << " tuples have been enumerated : " << avg_pop << endl;
		}
                //out.push_back(1);
                //out.push_back(1);
            }
            unsigned int pos = std::get<3>(return_output);
            if (pos+1 < y_to_x[std::get<2>(return_output)].size()) {
                pq.push(make_tuple(std::get<1>(return_output) + y_to_x[std::get<2>(return_output)].at(pos+1),
                                   std::get<1>(return_output),
                                   std::get<2>(return_output),
                                   pos+1));
            }
        }
        cout << "number of tuples enumerated : " << counter << endl;
        gettimeofday(&ending, NULL);
        cout << "materialized size : " << materialized.size() << endl;
	cout << " max_pop : " << max_pop << endl;
        cout << "All: " << ending.tv_sec - starting.tv_sec + (ending.tv_usec - starting.tv_usec) / 1e6 << endl;
        for (auto& it : time_space) {
            cout << it.first << " tuples in time and space " << it.second.first << " " << it.second.second << endl;
        }
	std::vector<double> cdf;
        cdf.resize(500);
        for (auto& it : cdf_pop) {
            cdf.at(it.first) = (double) it.second;
        }
        for (int  i = 1 ; i < cdf.size() ; ++i) {
            cdf.at(i) += cdf.at(i-1);
        }
        for (auto it : cdf) {
            cout << it/counter << ",";
        }
        cout << endl;
        cout << "end executing " << name() << endl;
    }
};

class STAR_TWO_LEX : public Algorithm {


public:

    STAR_TWO_LEX() {}

    virtual std::string name() {
        return "star 2 lexicographic";
    }

    virtual void execute(std::string dataset, int limit) {
        cout << "executing " << name() << endl;
        timeval starting, ending,sorte,sortb, s1, t1, s2, t2, temp;
        unordered_map<long long int, pair<long long int, long long int>> time_space;
        load(dataset);
        gettimeofday(&starting, NULL);
        vector<pair<int,int>> out;
        vector<int> sorted_x;
        sorted_x.reserve(x_to_y.size());

        for (auto&it : x_to_y) {
            for (auto&val : it.second) {
                y_to_x[val].push_back(it.first);
            }
            sorted_x.push_back(it.first);
        }

        std::sort(sorted_x.begin(), sorted_x.end());

        gettimeofday(&s1, NULL);
        cout << "Preprocessing: " << s1.tv_sec - starting.tv_sec + (s1.tv_usec - starting.tv_usec) / 1e6 << endl;
        int counter = 0; unsigned long max_size = 0;
        for(int it : sorted_x) {
            set<int> dedup;
            vector<int>& y = x_to_y[it];
            for (int val : y) {
                vector<int>& z = y_to_x[val];
                for (int val_z : z) {
                    dedup.insert(val_z);
                }
            }
            max_size = std::max(max_size, dedup.size());
            for (auto testval : dedup) {
                ++counter;
                pair<int, int> entry = make_pair(it, testval);
                out.push_back(entry); // enumerated
                if (checkpoints.find(counter) != checkpoints.end()) {
                    gettimeofday(&temp, NULL);
                    long long int timediff =
                            temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                    pair<long long int, long long int> p = make_pair(timediff, max_size);
                    time_space[counter] = p;
                }
            }
        }
        
	gettimeofday(&sortb, NULL);
        cout << "sorting materialized" << endl;
        sort(out.begin(), out.end(), [](const pair<int,int> &a,
        const pair<int,int> &b)
        {
            return (a.first + a.second < b.first + b.second);
        });
        cout << "sorting materialized done" << endl;
        gettimeofday(&sorte, NULL);
	cout << "Sort time: " << sorte.tv_sec - sortb.tv_sec + (sorte.tv_usec - sortb.tv_usec) / 1e6 << endl;
	cout << "number of tuples enumerated : " << counter << endl;
        gettimeofday(&ending, NULL);
        cout << "All: " << ending.tv_sec - starting.tv_sec + (ending.tv_usec - starting.tv_usec) / 1e6 << endl;
        for (auto &it : time_space) {
            cout << it.first << " tuples in time and space " << it.second.first << " " << it.second.second << endl;
        }
        cout << "end executing " << name() << endl;
    }

};

class STAR_THREE_OPT : public Algorithm {

public:
    unsigned long degree;

    STAR_THREE_OPT(unsigned long degree_input) {degree = degree_input;}

    virtual std::string name() {
        return "star 3 optimized";
    }

    virtual void execute(std::string dataset, int limit) {
        cout << "executing " << name() << endl;
        timeval starting, ending, s1, t1, s2, t2, temp;
        load(dataset);
        gettimeofday(&starting, NULL);
        unordered_map<long long int, pair<long long int, long long int>> time_space;
        int pq_size = 0;

        vector<tuple<int, int, int, int>> materialized_temp;
        vector<tuple<int, int, int, int>> materialized;
        unsigned long materialized_pointer = 0;

        for (auto& it : x_to_y) {
            if (it.second.size() <= degree) {
                continue;
            }
            for (auto& it2 : it.second) {
                for (auto& it3 : y_to_x[it2]) {
                    if (x_to_y[it3].size() > degree) {
                        for (auto &it4 : y_to_x[it2]) {
                            if (x_to_y[it4].size() > degree) {
                                auto t = std::make_tuple(it.first + it3 + it4, it.first, it3, it4);
                                materialized_temp.push_back(t);
                            }
                        }
                    }
                }
            }
        }

        cout << "sorting materialized" << endl;
        sort(materialized_temp.begin(), materialized_temp.end());
        cout << "sorting materialized done" << endl;

        std::tuple<int, int, int, int> last = std::make_tuple(-1,-1,-1,-1);
        for (const auto& it : materialized_temp) {
            if (!(std::get<1>(it) == std::get<1>(last) && std::get<2>(it) == std::get<2>(last)
                    && std::get<3>(it) == std::get<3>(last))) {
                materialized.push_back(it);
                last = it;
            }
        }

        for (auto it = x_to_y.begin(); it != x_to_y.end();) {
            if (it->second.size() > degree) {
                it = x_to_y.erase(it);
            } else {
                it++;
            }
        }

        gettimeofday(&s1, NULL);
        cout << "Preprocessing: " << s1.tv_sec - starting.tv_sec + (s1.tv_usec - starting.tv_usec) / 1e6 << endl;
        int tot_size = 0;
        vector<int> new_candidate;
        for (auto&it : x_to_y) {
            for (auto&val : it.second) {
                ++tot_size;
            }
        }
        new_candidate.reserve(tot_size);
        for (auto&it : x_to_y) {
            for (auto&val : it.second) {
                new_candidate.push_back(0);
            }
        }

        int counter = 0, c=0;
        priority_queue<tuple<unsigned  long, int, int, int, int, int, int, int>, vector<tuple<unsigned  long, int,int, int, int, int, int, int>>,
                greater<tuple<unsigned  long, int, int, int, int, int, int, int>>> pq;
        int vec_counter = 0;
        for (auto &it: x_to_y) {
            for (auto& val : it.second) {
                auto t = make_tuple(it.first + y_to_x[val][0] + y_to_x[val][0],
                                    it.first, y_to_x[val][0], y_to_x[val][0], val, 0 /*index*/, /*y_index*/0, vec_counter);
                new_candidate.at(vec_counter) += 1;
                ++vec_counter;
                pq.push(t);
            }
        }

        vector<tuple<unsigned  long, int, int, int, int, int, int, int>> out;
        tuple<unsigned  long, int, int, int, int, int, int, int> last_tuple = make_tuple(0, 0, 0, 0, 0, 0, 0, 0);
        cout << "pq size : " << pq.size() << " materialized size : " << materialized.size() << endl;
        while ((!pq.empty() || materialized_pointer < materialized.size())) {
            if (pq.size() > pq_size) {
                pq_size = pq.size();
            }
            tuple<unsigned  long, int, int, int, int, int, int, int> return_output;
            if (!pq.empty()) {
                return_output = pq.top();
                pq.pop();
            }
            if (materialized_pointer < materialized.size() && pq.empty()) {
                materialized_pointer += 1;
                counter += 1;
                if (checkpoints.find(counter) != checkpoints.end()) {
                    gettimeofday(&temp, NULL);
                    long long int timediff =
                            temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                    pair<long long int, long long int> p = make_pair(timediff, pq.size());
                    time_space[counter] = p;
                    cout << counter << " tuples in time and space " << timediff << " " << pq.size() << endl;
                }
                continue;
            } else {
                if (materialized_pointer < materialized.size() &&
                    std::get<0>(materialized[materialized_pointer]) < std::get<0>(return_output)) {
                    materialized_pointer += 1;
                    counter += 1;
                    if (checkpoints.find(counter) != checkpoints.end()) {
                        gettimeofday(&temp, NULL);
                        long long int timediff =
                                temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                        pair<long long int, long long int> p = make_pair(timediff, pq.size());
                        time_space[counter] = p;
                        cout << counter << " tuples in time and space " << timediff << " " << pq.size() << endl;
                    }
                    continue;
                }
            }
            const auto x =  std::get<1>(return_output);
            const auto y =  std::get<4>(return_output);
            const auto index = std::get<5>(return_output);
            const auto y_index = std::get<6>(return_output);
            const auto vec_c = std::get<7>(return_output);
            if (!(std::get<1>(last_tuple) == std::get<1>(return_output) && std::get<2>(last_tuple) == std::get<2>(return_output)
                    && std::get<3>(last_tuple) == std::get<3>(return_output))) {
                last_tuple = return_output;
                counter++;
                if (checkpoints.find(counter) != checkpoints.end()) {
                    gettimeofday(&temp, NULL);
                    long long int timediff =
                            temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                    pair<long long int, long long int> p = make_pair(timediff, pq.size());
                    time_space[counter] = p;
                    cout << counter << " tuples in time and space " << timediff << " " << pq.size() << endl;
                }
                // out.push_back(return_output);
            }
            if (index + 1 < y_to_x[y].size()) {
                auto p = make_tuple(x + y_to_x[y][y_index] + y_to_x[y][index+1],
                                    x, y_to_x[y][y_index], y_to_x[y][index+1],  y, index + 1, y_index, vec_c);
                pq.push(p);
            }
            if (new_candidate.at(vec_c) < y_to_x[y].size()) {
                auto p = make_tuple(x + y_to_x[y][new_candidate.at(vec_c)] + y_to_x[y][0],
                                    x, y_to_x[y][new_candidate.at(vec_c)], y_to_x[y][0], y, 0, new_candidate.at(vec_c), vec_c);
                new_candidate.at(vec_c) += 1;
                pq.push(p);
            }
        }
        cout << "number of tuples enumerated : " << counter << endl;
        gettimeofday(&ending, NULL);
        cout << "materialized size : " << materialized.size() << endl;
        cout << "All: " << ending.tv_sec - starting.tv_sec + (ending.tv_usec - starting.tv_usec) / 1e6 << endl;
        for (auto &it : time_space) {
            cout << it.first << " tuples in time and space " << it.second.first << " " << it.second.second << endl;
        }
        cout << "end executing " << name() << endl;
    }
};

class STAR_THREE_LEX : public Algorithm {

public:
    unsigned long degree;

    STAR_THREE_LEX() {}

    virtual std::string name() {
        return "star 3 lex";
    }

    virtual void execute(std::string dataset, int limit) {
        cout << "executing " << name() << endl;
        timeval starting, ending, s1, t1, s2, t2, temp;
        load(dataset);
        gettimeofday(&starting, NULL);
        unordered_map<long long int, pair<double, long long int>> time_space;

        vector<tuple<int, int, int, int>> materialized_temp;
        vector<tuple<int, int, int, int>> materialized;
        unordered_map<int, set<int>> x_to_y_set;
        unordered_map<int, set<int>> y_to_x_set;
        vector<int> sorted_x;

        for (auto& it : x_to_y) {
            sorted_x.push_back(it.first);
            for (auto& val : it.second) {
                x_to_y_set[it.first].insert(val);
            }
        }

        for (auto& it : y_to_x) {
            for (auto& val : it.second) {
                y_to_x_set[it.first].insert(val);
            }
        }

        std::sort(sorted_x.begin(), sorted_x.end());

        for (auto x1 : sorted_x) {
            set<int> dedup;
            vector<int>& y = x_to_y[x1];
            for (int val : y) {
                vector<int>& z = y_to_x[val];
                for (int val_z : z) {
                    dedup.insert(val_z);
                }
            }
            std::vector<int> v(dedup.begin(), dedup.end());
            std::sort(v.begin(), v.end());
            for (auto x2 : v) {
                auto smaller =  x_to_y[x1].size() < x_to_y[x2].size() ? x1 : x2;
                auto larger = x_to_y[x1].size() >= x_to_y[x2].size() ? x1 : x2;
                set<int> dedup2;
                for (auto y_common : x_to_y[smaller]) {
                    if (x_to_y_set[larger].find(y_common) != x_to_y_set[larger].end()) {
                        for (auto x3 : y_to_x[y_common]) {
                            dedup2.insert(x3);
                        }
                    }
                }
                std::vector<int> v2(dedup2.begin(), dedup2.end());
                std::sort(v2.begin(), v2.end());
                for (auto x3 : v2) {
                    auto t = std::make_tuple(x1+x2+x3,x1,x2,x3);
                    materialized.push_back(t);
                    // cout << x1 << " " << x2 << " " << x3 << endl;
		    if (checkpoints.find(materialized.size()) != checkpoints.end()) {
                    gettimeofday(&temp, NULL);
                    double timediff =
                            temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                    pair<double, long long int> p = make_pair(timediff, materialized.size());
                    time_space[materialized.size()] = p;
                    cout << materialized.size() << " tuples in time and space " << timediff << " " << materialized.size() << endl;
                    }
                }
            }

        }
        gettimeofday(&s1, NULL);
        cout << "sorting materialized" << endl;
        sort(materialized.begin(), materialized.end());
        cout << "sorting materialized done" << endl;
        gettimeofday(&s2, NULL);

        cout << "number of tuples enumerated : " << materialized.size() << endl;
        gettimeofday(&ending, NULL);
        cout << "materialized size : " << materialized.size() << endl;
        cout << "Sort time: " << s2.tv_sec - s1.tv_sec + (s2.tv_usec - s1.tv_usec) / 1e6 << endl;
        cout << "All: " << ending.tv_sec - starting.tv_sec + (ending.tv_usec - starting.tv_usec) / 1e6 << endl;
        cout << "end executing " << name() << endl;
    }
};


class UCQ_2_4_PATH : public Algorithm {

public:

    UCQ_2_4_PATH() {}

    priority_queue<tuple<int, int, int, int>, vector<tuple<int, int, int, int>>, greater<tuple<int, int, int, int>>> pq_2_path;
    tuple<int, int, int, int> last_2_path = make_tuple(-1,-1,-1,-1);
    tuple<unsigned long, int, int, int, int> last_4_path = std::make_tuple(0,-1,-1,-1, -1);
    timeval starting;
    long int counter = 0;
    unordered_map<long long int, pair<double, long long int>> time_space;


    unordered_map<int, vector<int>> x_to_x_mat;
    std::vector<int> new_candidate_count_vec_x_to_x;
    unordered_map<int, bool> x_finished;
    std::unordered_map<int, priority_queue<tuple<unsigned long, int, int, int>,
            vector<tuple<unsigned long, int, int, int>>,
            greater<tuple<unsigned long, int, int, int>>>> pq_x_to_y;
    priority_queue<tuple<unsigned long, int, int, int, int>,
            vector<tuple<unsigned long, int, int, int, int>>,
            greater<tuple<unsigned long, int, int, int, int>>> pq_4_path;
    const tuple<int, int, int, int> eoe_2_path = make_tuple(-1,-1,-1,-1);
    const tuple<unsigned long, int, int, int, int> eoe_4_path = make_tuple(0, -1, -1, -1, -1);

    void preprocess_2_path() {
        for (auto &it: x_to_y) {
            for (auto& val : it.second) {
                pq_2_path.push(make_tuple(it.first + y_to_x[val][0], it.first, val, 0));
            }
        }
    }

    void preprocess_4_path() {
        new_candidate_count_vec_x_to_x.reserve(x_to_y.size());
        for (auto&it : x_to_y) {
            x_finished[it.first] = false;
        }
        for(auto& it : x_to_y) {
            for (auto& it2 : it.second) {
                auto t = std::make_tuple(y_to_x[it2][0], it.first, it2, 1);
                pq_x_to_y[it.first].push(t);
            }
        }
        for(auto& it : x_to_y) {
            int new_candidate = -1;
            const auto current = pq_x_to_y[it.first].top();
            pq_x_to_y[it.first].pop();
            new_candidate = std::get<0>(current);
            x_to_x_mat[it.first].push_back(new_candidate);
            const auto y = std::get<2>(current);
            const auto index = std::get<3>(current);
            if (index < y_to_x[y].size()) {
                auto t = std::make_tuple(y_to_x[y].at(index) , it.first, y, index+1);
                pq_x_to_y[it.first].push(t);
            }
            auto local = pq_x_to_y[it.first].top();
            while(!pq_x_to_y[it.first].empty() && new_candidate == std::get<0>(local)) {
                const auto localy = std::get<2>(local);
                const auto localindex = std::get<3>(local);
                if (localindex < y_to_x[localy].size()) {
                    auto localt = std::make_tuple(y_to_x[localy].at(localindex), it.first, localy, localindex + 1);
                    pq_x_to_y[it.first].push(localt);
                }
                pq_x_to_y[it.first].pop();
                local = pq_x_to_y[it.first].top();
            }
        }
        int vec_counter = 0;
        for (auto& it : x_to_y) {
            new_candidate_count_vec_x_to_x.push_back(0);
            unsigned long a = x_to_x_mat[it.first][new_candidate_count_vec_x_to_x.at(vec_counter)]; // a fixed and move over b
            unsigned long b = x_to_x_mat[it.first][new_candidate_count_vec_x_to_x.at(vec_counter)];
            auto t = std::make_tuple(a+b, a, it.first, 1, vec_counter);
            COUT << a+b << " " << a << " for x : " << it.first << endl;
            new_candidate_count_vec_x_to_x.at(vec_counter) += 1;
            pq_4_path.push(t);
            ++vec_counter;
        }

    }

    tuple<int, int, int, int> enumerate_2_path() {
        timeval temp;
        bool should_I_return = false;
        while(!pq_2_path.empty()) {
            tuple<int, int, int, int> return_output;
            return_output = pq_2_path.top();
            pq_2_path.pop();
            if (std::get<0>(last_2_path) != std::get<0>(return_output) ||
                    std::get<1>(last_2_path) != std::get<1>(return_output)) {
                last_2_path = return_output;
                should_I_return = true;
            }
            unsigned int pos = std::get<3>(return_output);
            if (pos+1 < y_to_x[std::get<2>(return_output)].size()) {
                pq_2_path.push(make_tuple(std::get<1>(return_output) + y_to_x[std::get<2>(return_output)].at(pos+1),
                                   std::get<1>(return_output),
                                   std::get<2>(return_output),
                                   pos+1));
            }
            if (should_I_return) {
                return return_output;
            }
        }
        return eoe_2_path;
    }

    tuple<unsigned long, int, int, int, int> enumerate_4_path() {
        bool should_I_return = false;
        while(!pq_4_path.empty()) {
            const auto t = pq_4_path.top();
            pq_4_path.pop();
            const auto index =  std::get<3>(t);
            const auto x =  std::get<2>(t);
            const auto a =  std::get<1>(t);
            const auto vec_counter = std::get<4>(t);

            if (!(std::get<0>(t) == std::get<0>(last_4_path) && std::get<1>(t) == std::get<1>(last_4_path))) {
                should_I_return = true;
                timeval temp;
            }
            last_4_path = t;
            if (!x_finished[x]) {
                if (index == x_to_x_mat[x].size() && !pq_x_to_y[x].empty()) {
                    int new_candidate = -1;
                    const auto current = pq_x_to_y[x].top();
                    pq_x_to_y[x].pop();
                    new_candidate = std::get<0>(current);
                    x_to_x_mat[x].push_back(new_candidate);
                    const auto y = std::get<2>(current);
                    const auto index = std::get<3>(current);
                    if (index < y_to_x[y].size()) {
                        auto t = std::make_tuple(y_to_x[y].at(index) , x, y, index+1);
                        pq_x_to_y[x].push(t);
                    }
                    if (!pq_x_to_y[x].empty()) {
                        auto local = pq_x_to_y[x].top();
                        while(!pq_x_to_y[x].empty() && new_candidate == std::get<0>(local)) {
                            const auto localy = std::get<2>(local);
                            const auto localindex = std::get<3>(local);
                            if (localindex < y_to_x[localy].size()) {
                                auto localt = std::make_tuple(y_to_x[localy].at(localindex), x, localy, localindex + 1);
                                pq_x_to_y[x].push(localt);
                            }
                            pq_x_to_y[x].pop();
                            local = pq_x_to_y[x].top();
                        }
                    } else {
                        x_finished[x] = true;
                    }
                }
                if (pq_x_to_y[x].empty()) {
                    x_finished[x] = true;
                }
            }
            if (index < x_to_x_mat[x].size()) {
                unsigned long new_b = x_to_x_mat[x][index];
                auto tup = std::make_tuple(a + new_b, a, x, index+1, vec_counter);
                pq_4_path.push(tup);
            }
            if (new_candidate_count_vec_x_to_x.at(vec_counter) < x_to_x_mat[x].size()) {
                unsigned long new_a = x_to_x_mat[x][new_candidate_count_vec_x_to_x.at(vec_counter)];
                unsigned long b_for_new_a = x_to_x_mat[x][0];
                new_candidate_count_vec_x_to_x.at(vec_counter) += 1;
                auto new_tup = std::make_tuple(new_a+b_for_new_a,new_a, x, 1, vec_counter);
                pq_4_path.push(new_tup);
            }
            if (should_I_return) {
                return  t;
            }
        }
        return eoe_4_path;
    }

    virtual std::string name() {
        return "ucq 2 4 path";
    }

    virtual void execute(std::string dataset, int limit) {
        cout << "executing " << name() << endl;
        timeval starting, preprocessing;
        load(dataset);
        gettimeofday(&starting, NULL);
        preprocess_2_path();
        preprocess_4_path();
        gettimeofday(&preprocessing, NULL);
        cout << "preprocessing done : " << preprocessing.tv_sec - starting.tv_sec + (preprocessing.tv_usec - starting.tv_usec) / 1e6 << endl;
        bool path2finished = false, path4finished = false;
        tuple<int, int, int, int> path_2_ans;
        tuple<unsigned long, int, int, int, int> path_4_ans;
        path_2_ans = enumerate_2_path();
        path_4_ans = enumerate_4_path();
        while(!path2finished && !path4finished) {
            if (path_2_ans != eoe_2_path && path_4_ans != eoe_4_path) {
                if (std::get<0>(path_2_ans) < std::get<0>(path_4_ans)) {
                    COUT << std::get<0>(path_2_ans) << " 2 path " << std::get<1>(path_2_ans) <<  endl;
                    path_2_ans = enumerate_2_path();
                }
                if (std::get<0>(path_2_ans) > std::get<0>(path_4_ans)) {
                    COUT << std::get<0>(path_4_ans) << " 4 path " << std::get<1>(path_4_ans) << endl;
                    path_4_ans = enumerate_4_path();
                }
                if (std::get<0>(path_2_ans) == std::get<0>(path_4_ans) &&
                        std::get<1>(path_2_ans) < std::get<1>(path_4_ans)) {
                    COUT << std::get<0>(path_2_ans) << " 2 path " << std::get<1>(path_2_ans) <<  endl;
                    path_2_ans = enumerate_2_path();
                }
                if (std::get<0>(path_2_ans) == std::get<0>(path_4_ans) &&
                    std::get<1>(path_2_ans) > std::get<1>(path_4_ans)) {
                    COUT << std::get<0>(path_2_ans) << " 2 path " << std::get<1>(path_2_ans) <<  endl;
                    path_4_ans = enumerate_4_path();
                }
                if (std::get<0>(path_2_ans) == std::get<0>(path_4_ans) &&
                        std::get<1>(path_2_ans) == std::get<1>(path_4_ans)) {
                    path_2_ans = enumerate_2_path();
                    path_4_ans = enumerate_4_path();
                }
                counter++;
                timeval temp;
                if (checkpoints.find(counter) != checkpoints.end()) {
                    gettimeofday(&temp, NULL);
                    double timediff = temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                    pair<double, long long int> p = make_pair(timediff, 0);
                    time_space[counter] = p;
                    cout << counter << " tuples in time and space " << timediff << endl;
                }
            }
            if (path_2_ans == eoe_2_path) {
                cout << "Getting out 2 path finished" << endl;
                path2finished = true;
            }
            if (path_4_ans == eoe_4_path) {
                cout << "Getting out 4 path finished" << endl;
                path4finished = true;
            }
        }
        if (path2finished) {
            while (true) {
                path_4_ans = enumerate_4_path();
                if (path_4_ans == eoe_4_path) {
                    break;
                }
                counter++;
                timeval temp;
                if (checkpoints.find(counter) != checkpoints.end()) {
                    gettimeofday(&temp, NULL);
                    double timediff = temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                    pair<double, long long int> p = make_pair(timediff, 0);
                    time_space[counter] = p;
                    cout << counter << " tuples in time and space " << timediff << endl;
                }
                cout << std::get<0>(path_4_ans) << " 4 path outside " << std::get<1>(path_4_ans) << endl;
            }
        }
        if (path4finished) {
            while (true) {
                path_2_ans = enumerate_2_path();
                if (path_2_ans == eoe_2_path) {
                    break;
                }
                counter++;
                timeval temp;
                if (checkpoints.find(counter) != checkpoints.end()) {
                    gettimeofday(&temp, NULL);
                    double timediff = temp.tv_sec - starting.tv_sec + (temp.tv_usec - starting.tv_usec) / 1e6;
                    pair<double, long long int> p = make_pair(timediff, 0);
                    time_space[counter] = p;
                    cout << counter << " tuples in time and space " << timediff << endl;
                }
                cout << std::get<0>(path_2_ans) << " 2 path outside " << std::get<1>(path_2_ans) <<  endl;
            }
        }
        cout << "total enumerated tuples : " << counter << endl;
    }
};

#endif //RANKEDLIMIT_ALGORITHM_H



