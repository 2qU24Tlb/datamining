#include <iostream>
#include <string>
#include "apriori.h"

#define DB_SIZE 5

using namespace std;

const string DB[DB_SIZE] = {"GAAGATCC", "GAAAAGCT", "GAAGACCA", "CCCGATAT", "GAAGAAAG"}; 

//A - 0
//C - 1
//G - 2
//T - 3
string CharToInt(const string orig) {
  string new_string ("0", orig.size());
  
  for (unsigned i = 0; i < orig.size(); i++) {
    switch (orig[i]) {
    case 'A':
      new_string[i] = '0';
        break;
    case 'C':
      new_string[i] = '1';
      break;
    case 'G':
      new_string[i] = '2';
      break;
    case 'T':
      new_string[i] = '3';
      break;
    default:
      cout << "Error! DB should only contain A C G T!" << endl;
    }
  }
  
  return new_string;
}

int main (void) 
{
  string newDB[DB_SIZE]; 
  
  for (int i = 0; i < DB_SIZE; i++) {
    newDB[i] = CharToInt(DB[i]);
  }
  
  Apriori_2 alg(newDB, DB_SIZE);
  
  alg.Start();

  //alg.Display();

  return 0;
}
