#ifndef TIRE_BASED_APRIORI_TIRE
#define TIRE_BASED_APRIORI_TIRE

#include <iostream>
#include <string>
#include <queue>

#define WINDOW_LENGTH 6
#define ERROR_RATE 1

using namespace std;

typedef struct trie_node {
  trie_node *children[4];
  bool *seq; 
  char chacter;
} TNode;

class WindowTire {
 private:
  TNode *root;
  int db_length;

 public:
  WindowTire(string *db, int length) {
    db_length = length;
    root = new TNode;

    for (int i = 0; i < 4; i++) {
      root->children[i] = NULL;
    }

    for (int i = 0; i < length; i++) {
      Insert(i, db[i]);
    }
  }

  ~WindowTire() {
  }
  
  TNode *ConstructNode() {
    TNode *tmp = new TNode;
    
    for (int i = 0; i < 4; i++) {
      tmp->children[i] = NULL;
    }
    
    tmp->seq = new bool[db_length];
    
    return tmp;
  }
  
  void InnerInsert(int id, string seq) {
    TNode *p = root;
    int length = seq.size();
    int i = 0;
    int index = 0;
    char chacter;
    
    while (i < length) {
      chacter = seq[i++];
      index = chacter - '0'; 

      if (p->children[index] == NULL) {
        p->children[index] = ConstructNode();
      }
      p = p->children[index];
      p->chacter = chacter;
      p->seq[id] = 1;
    }
  }
  
  void Insert(int id, string seq) {
    string new_seq (WINDOW_LENGTH, '0');

    for (unsigned i = 0; i <= (seq.size() - WINDOW_LENGTH); i++) {
      new_seq.replace(0, 6, seq, i, 6);
      InnerInsert(id, new_seq);
    }
  }
  
  int SumCount(bool *seq) {
   
    int count = 0;
    
    for (int i = 0; i < db_length; i++) {
      if (seq[i] == 1) {
        ++count;
      }
    }
    
    return count;
  }

  int SearchCount(string seq) {
    int count = 0;
    TNode *p = root;
    
    for (unsigned i = 0; i < seq.size(); i++) {
      p = p->children[seq[i] - '0'];

      if (p == NULL) {
        return 0;
      }
    }
    
    count = SumCount(p->seq);

    return count;
  }
  //[TODO] prune tire tree
  
  TNode *SearchNode(string seq) {
    TNode *p = root;

    for (unsigned i = 0; i < seq.size(); i++) {
      p = p->children[seq[i] - '0'];

      if (p == NULL) {
        return NULL;
      }
    }
    
    return p;
  }
  
  void SetOrSeq(bool *seq1, bool *seq2) {
    for (int i = 0; i < db_length; i++) {
      if (seq2[i] == 1) {
        seq1[i] = 1;
      }
    }
  }
  
  int ApproximateSearch(string seq) {
    string new_seq(seq);
    bool result[db_length] = {0};
    TNode *p = NULL;
    int count = 0;
    
    for (unsigned i = 0; i < seq.size(); i++) {
      for (int j = 0; j < 4; j++) {
        new_seq = seq;
        new_seq.replace(i, 1, to_string(j));
        p = SearchNode(new_seq);
        if (p != NULL) {
          SetOrSeq(result, p->seq);
        }
      }
    }

    count = SumCount(result);

    return count;
  }
  
  void Display() {
    queue<TNode* > my_queue;
    TNode *tmp = NULL;
    
    my_queue.push (root);
    
    while (!my_queue.empty()) {
      tmp = my_queue.front();
      my_queue.pop();
      
      if (tmp == root) {
        cout << "root:";
      } else {
        cout << tmp->chacter << ":";
      }

      for (int i = 0; i < 4; i++) {
        if (tmp->children[i] != NULL) {
          my_queue.push(tmp->children[i]);
          cout << tmp->children[i]->chacter;
        }
      }
      cout << endl;
    }
  }
};

#endif
