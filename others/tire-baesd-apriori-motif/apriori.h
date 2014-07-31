#ifndef TIRE_BASED_APRIORI_APRIORI
#define TIRE_BASED_APRIORI_APRIORI

#include <string>
#include "tire.h"

const int minsup = 4;
const int MOTIF_LENGTH = 4;
//[TODO] add normIC

typedef struct FrequentItems {
  string candidates;
  int count;
  FrequentItems *next;
} FItems; 

class Apriori_2 {
 private:
  FItems *root;
  FItems *prev_root;
  WindowTire *tire;
 public:
  Apriori_2(string *db, int length) {
    root = new FItems;
    root->count = 0;
    root->next = NULL;
    
    prev_root = NULL;
    tire = new WindowTire(db, length);
  }

  ~Apriori_2() {
  }

  void Start() {
    //tire->Display();
    FirstScan();
    
    while (root->next != NULL) {
      SelfJoin();
      Prune();
    }

    Display();
  }
  
  FItems *ConstructNode(string candidates, int support) {
    FItems *tmp = new FItems;
    tmp->candidates = candidates;
    tmp->count = support;
    tmp->next = NULL;
    
    return tmp;
  }
  
  void FirstScan() {
    FItems *p = root;
    int support = 0;

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 4; j++) {
        string tmp = to_string(i) + to_string(j);
       
        support = tire->SearchCount(tmp);
        
        if (support >= minsup) {
          p->next = ConstructNode(tmp, support);
          p = p->next;
        }
      }
    }
  }
  
  void InsertNode(FItems *new_root, FItems *new_node) {
    if (new_root == NULL) {
      new_root = new_node;
      return;
    }

    FItems *tmp = new_root;
   
    while (tmp->next != NULL) {
      tmp = tmp->next;
    }
    
    tmp->next = new_node;
  }
  
  void SelfJoin() {
    FItems *p = root->next;
    FItems *q = NULL;
    FItems *new_root = NULL;
    FItems *new_node = NULL;
    
    while (p != NULL) {
      q = root->next;
      if ((p->candidates.size() + q->candidates.size()) > MOTIF_LENGTH) {
        break;
      }
      while (q != NULL) {
        new_node = ConstructNode(p->candidates+q->candidates, 0);
        if (new_root == NULL) {
          new_root = new_node;
        } else {
          InsertNode(new_root, new_node);
        }
        q = q->next;
      }
      p = p->next;
    }
    
    if (prev_root != NULL) {
      p = prev_root->next;
      while (p != NULL) {
        q = p;
        p = p->next;
        delete q; 
      }
    }
    prev_root = root->next;
    
    root->next = new_root;
  }
  
  void Prune() {
    FItems *p = root;
    FItems *q = p->next;
    
    while (q != NULL) {
      q->count = tire->ApproximateSearch(q->candidates); 
      
      if (q->count == 0) {
        p->next = q->next;
        delete q;
        q = p->next;
      } else {
        p = q;
        q = q->next;
      }
    }
  }
  
  void Display() {
    FItems *p = prev_root;
    
    while (p != NULL) {
      cout << p->candidates <<":" << p->count << endl; 
      p = p->next;
    }
  }
};

#endif 
