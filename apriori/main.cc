#ifndef DM_DBHANDLER_H
#define DM_DBHANDLER_H

#include <iostream>
#include <string>
#include <sstream>
#include <set>

using namespace std;

const int elements = 5;
const int datasize = 4;
const string stringdb[datasize] = {"1 0 1 1 0", "0 1 1 0 1", "1 1  1 0 1", "0 1 0 0 1"};
//use a global queue to store FIPS

class Datasets {
    private:
        int db[datasize][elements];

    public:
        Datasets(const char *filename = NULL) {
            //[TODO] read from file
            if (filename != NULL) {
            }
            int k = 0;

            for (int i = 0; i < datasize; i++) {
                k = 0;
                stringstream ss(stringdb[i]);
                for (int j =0; j < elements && ss.good(); j++) {
                    ss >> k; 
                    //case 1: int-style database
                    //db[i][j] = k;
                    //case 2: bool-style databse
                    if (k != 0) {
                        db[i][j] = j;
                    }
                }
            }
        }
        int GetItem(int i, int j) {
            return db[i][j];
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

typedef struct _candidate {
    int item;
    int count;
    struct _candidate *next;
    struct _candidate *sub;
    //struct _candidate *parent;
} Candidate;

class CandidateTable {
    private: 
        Candidate *root;
        class Datasets *my_datasets;

    public:
        CandidateTable(Datasets& datasets) {
            root = NULL;
            my_datasets = &datasets;
        }
        ~CandidateTable() {
            //[TODO] clear all memeory from root;
        }
        
        void GenerateTable(int item) {
            if (root == NULL) {
                root = new Candidate;
                root->item = item;
                root->count = 1;
                root->next = NULL;
                root->sub = NULL;
                return;
            }

            Candidate *tmp = root;

            while (tmp != NULL) {
                if (tmp->item == item) {
                    tmp->count++;
                    return;
                }
                tmp = tmp->next;
            }

            if (tmp == NULL) {
                tmp = new Candidate;
                tmp->item = item;
                tmp->count = 1;
                tmp->next = root->next;
                root->next = tmp;
                tmp->sub = NULL;
            }
        }

        void SelfJoin() {
            //C1
            if (root == NULL) {
                for (int i = 0; i < datasize; i++) {
                    for (int j = 0; j < elements; j++) {
                        GenerateTable(my_datasets->GetItem(i, j));
                    }
                }
            }
        }
        void PruneSelfJoin() {
        }
        void PruneScan() {
        }
        void Display() {
            Candidate *p = root;
            while (p != NULL) {
                cout << p->item << " count:" << p->count << endl;
            }
        }
};

int main(void)
{
    class Datasets my_datasets;
    class CandidateTable my_table(my_datasets);

    my_table.SelfJoin();
    my_table.Display();


    return 0;
}

#endif
