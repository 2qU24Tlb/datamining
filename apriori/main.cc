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
const int minsup = 2;
//[TODO]use a global queue to store FIPS

typedef struct _candidate {
  int item;
  int count;
  struct _candidate *next;
  struct _candidate *sub;
  struct _candidate *parent;
} Candidate;

class Datasets {
private:
  int db[datasize][elements];

public:
  Datasets(const char *filename = NULL) {
    for (int i = 0; i < datasize; i++) {
      for (int j = 0; j < elements; j++) {
        db[i][j] = 0;
      }
    }

    //[TODO] read from file
    if (filename != NULL) {
    }

    BuildDB(filename);
  }
  
  void BuildDB(const char *filename = NULL)
  {
    int k = 0;
    int pos = 0;
 
    for (int i = 0; i < datasize; i++) {
      pos = 0;
      stringstream ss(stringdb[i]);
      for (int j = 0; j < elements && ss.good(); j++) {
        ss >> k; 
        //case 1: int-style database
        //db[i][j] = k;
        //case 2: bool-style databse
        if (k != 0) {
          db[i][pos++] = j+1;
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

class CandidateTable {
private: 
  Candidate *root;
  class Datasets *my_datasets;

public:
  CandidateTable(Datasets& datasets) {
    root = new Candidate;
    Candidate *cur = NULL;
    
    //use root as empty head
    root->item = 0;
    root->count = elements;
    root->next = NULL;
    root->sub = NULL;
    root->parent = NULL;

    for (int i = 0; i < elements; i++) {
      Candidate *new_item = new Candidate;

      new_item->item = i + 1;
      new_item->count = 0;
      new_item->next = NULL;
      new_item->sub = NULL;
      new_item->parent = NULL;

      if (cur == NULL) {
        cur = new_item;
        root->sub = cur;
      } else {
        cur->next = new_item;
        cur = cur->next;;
      }
    }

    my_datasets = &datasets;
  }

  ~CandidateTable() {
    //[TODO] clear all memeory from root;
  }

  void Start() {
    SelfJoin(1);
    // for (int i = 1; i < elements; i++) {
    //   SelfJoin(i);
    // }
  }
  
  void SelfJoin(int level) {
    if (level == 1) {
      for (int i = 0; i < datasize; i++) {
        for (int j = 0; j < elements; j++) {
          Level1Scan(my_datasets->GetItem(i, j));
        }
      }
      SupportLevel1Prune();
      return;
    } 

  }
  

  void Level1Scan(int item) {
    if (item == 0) {
      return;
    }

    Candidate *tmp = root->sub;

    while (tmp != NULL) {
      if (tmp->item == item) {
        tmp->count++;
        return;
      }
      tmp = tmp->next;
    }
  }

  //if root is infrequent?
  void SupportLevel1Prune()
  {
    Candidate *tmp = root->sub;
    Candidate *it = tmp->next;
    
    while (it != NULL) {
      if (it->count < minsup) {
        tmp->next = it->next;
        delete it;
        it = tmp->next;
      } else {
        tmp = it;
        it = tmp->next;
      }
    }
  }

  void PruneSelfJoin() {
  }
  void PruneScan() {
  }
  void Display() {
    Candidate *p = root->sub;
    while (p != NULL) {
      cout << p->item << " count:" << p->count << endl;
      p = p->next;
    }
  }
};

int main(void)
{
  class Datasets my_datasets;
  class CandidateTable my_table(my_datasets);

  my_datasets.Display();
  my_table.Start();
  my_table.Display();

  return 0;
}

#endif
