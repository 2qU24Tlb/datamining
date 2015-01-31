#include <algorithm>
#include <iostream>
#include <fstream>
#include <string>
#include <bitset>
#include <vector>
#include <queue>

using namespace std;

const int elements = 5;
const int datasets = 4;
const string db[datasets] = {"1 0 1 1 0", "0 1 1 0 1", "1 1  1 0 1", "0 1 0 0 1"};

typedef bitset<elements> tidsets;

class DataSets {
    private:
        vector<tidsets> mdb;
    public:
        DataSets(const char *filename) {
            if (filename != NULL) {
                ifstream ifs(filename);

                string line;
                while (getline(ifs, line)) {
                    cout << line << endl;
                }
                ifs.close();
            } else {

            }
        }
        ~DataSets() {
            if (!mdb.empty()) {
                mdb.erase(mDB.begin(), mdb.end());
            }
        }

};

void ConstructDB() {
    vector<tidsets> mdb;

    for (int i = 0; i < datasets; i++) {
        string tempString = db[i];
        remove(tempString.begin(), tempString.end(), ' ');

        bitset<elements> tempTD(tempString);
        mdb.push_back(tempTD);
    }

    cout << mDB.size() << endl;
}

int main(void)
{
    DataSets("../retail.txt");

    return 0;
}
