#ifndef DM_DBHANDLER_H
#define DM_DBHANDLER_H

#include <iostream>
#include <string>
#include <sstream>
#include <queue>
#include <stack>

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
    SelfJoin();
  }
  
  void GenerateCandidate(Candidate *parent) {
    if (parent == NULL) {
      cout << "Error: " << "parent is null!" <<endl;
      return;
    }
    Candidate *previos = parent->next;
    Candidate *cur = NULL; 
    
    while (previos != NULL) {

      Candidate *new_item = new Candidate;
      new_item->item = previos->item;
      new_item->count = 0;
      new_item->next = NULL;
      new_item->sub = NULL;
      new_item->parent = parent;

      if (parent->sub == NULL) {
        parent->sub = new_item;
        cur = parent->sub;
      } else {
        cur->next = new_item;
        cur = new_item;
      }

      previos = previos->next;
    }
  }
  
  void SelfJoin() {
    Candidate *p = NULL;
    queue<Candidate* > my_queue;
    my_queue.push(root->sub);
    PruneLevel1();
    
    while (!my_queue.empty()) {
      p = my_queue.front();
      my_queue.pop();

      while (p != NULL) {
        if (p->next != NULL) {
          GenerateCandidate(p);
          my_queue.push(p->sub);
        }
        p = p->next;
      }
    }
  }
  
  void ScanLevel1(int item) {
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

  void PruneLevel1()
  {
    Candidate *tmp = root->sub;
    Candidate *it = tmp->next;
    
    for (int i = 0; i < datasize; i++) {
      for (int j = 0; j < elements; j++) {
        ScanLevel1(my_datasets->GetItem(i, j));
      }
    }
    
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

  void PruneGeneration() {
  }
  void PruneScan() {
  }

  void Display() {
    Candidate *p = NULL;
    queue<Candidate* > my_queue;
    stack<Candidate* > parents;
    my_queue.push(root->sub);

    while (!my_queue.empty()) {
      p = my_queue.front();
      my_queue.pop();
      while (p != NULL) {
        if (p->sub != NULL) {
          my_queue.push(p->sub);
        }
        
        if (p->parent == NULL) {
          cout << "item: " << p->item << " count: " << p->count << endl;
        } else {
          while (p != NULL) {
            parents.push(p);
            p = p->parent;
          }
          cout << "item: ";
          while (!parents.empty()) {
            p = parents.top();
            cout << p->item;
            parents.pop();
          }
          cout << " count: "<< p->count << endl;
        }
        p = p->next;
      }
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
