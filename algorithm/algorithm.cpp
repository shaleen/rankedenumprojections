

#include "algorithm.h"

void run_algorithm(AlgorithmType algo, std::string dataset, int limit) {
    switch (algo) {
        case AlgorithmType::BFS_3PATH: {
            BFS_THREEPATH* obj = new BFS_THREEPATH();
            obj->execute(dataset, limit);
            break;
        }
        case AlgorithmType::BFS_4PATH: {
            BFS_FOURPATH* obj = new BFS_FOURPATH();
            obj->execute(dataset, limit);
            break;
        }
        case AlgorithmType::PATH_3: {
            PATH_THREE* obj = new PATH_THREE();
            obj->execute(dataset, limit);
            break;
        }
        case AlgorithmType::PATH_4: {
            PATH_FOUR* obj = new PATH_FOUR();
            obj->execute(dataset, limit);
            break;
        }
        case AlgorithmType::STAR_2LEX: {
            STAR_TWO_LEX* obj = new STAR_TWO_LEX();
            obj->execute(dataset, limit);
            break;
        }
        case AlgorithmType::STAR_2OPT: {
            for (unsigned long degree = 512; degree > 0 ; degree = degree/2) {
                STAR_TWO_OPT* obj = new STAR_TWO_OPT(degree);
                obj->execute(dataset, limit);
            }
            break;
        }
        case AlgorithmType::STAR_2: {
            for (unsigned long degree = 512; degree > 0 ; degree = degree/2) {
                STAR_TWO* obj = new STAR_TWO(degree);
                obj->execute(dataset, limit);
            }
            break;
        }
        case AlgorithmType::STAR_3OPT: {
            for (unsigned long degree = 512; degree > 0 ; degree = degree/2) {
                STAR_THREE_OPT *obj = new STAR_THREE_OPT(degree);
                obj->execute(dataset, limit);
            }

            break;
        }
        case AlgorithmType::STAR_3LEX: {
            STAR_THREE_LEX* obj = new STAR_THREE_LEX();
            obj->execute(dataset, limit);
            break;
        }
	case AlgorithmType::UCQ_24PATH: {
            UCQ_2_4_PATH* obj = new UCQ_2_4_PATH();
            obj->execute(dataset, limit);
            break;
        }
        default:
            std::cout << "Not yet implemented" << std::endl;
    }
}
