#ifndef DM_DBHANDLER_H
#define DM_DBHANDLER_H

#include <iostream>
#include <string>
#include <sstream>

using namespace std;

const int elements = 5;
const int datasize = 4;
const string stringdb[datasize] = {"1 0 1 1 0", "0 1 1 0 1", "1 1  1 0 1", "0 1 0 0 1"};

class Datasets {
    private:
        bool db[datasize][elements];
    public:
        Datasets(const char *filename = NULL) {
            //[TODO] read from file
            if (filename != NULL) {
            }

            for (int i = 0; i < datasize; i++) {
                stringstream ss(stringdb[i]);
                for (int j =0; j < elements && ss.good(); j++) {
                    ss >> db[i][j];
                }
            }
        }

        ~Datasets() {
        }

        void Display() {
            for (int i = 0; i < datasize; i++) {
                for (int j = 0; j < elements; j++) {
                    cout << db[i][j] << " ";
                }
                cout << endl;
            }
        }
};

typdef struct _itemcout {
}

class Candidate {
    private:
}

int main(void)
{
    class Datasets mydatasets;

    mydatasets.Display();

    return 0;
}

#endif
