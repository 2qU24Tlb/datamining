#include <algorithm>
#include <iostream>
#include <string>
#include <bitset>
#include <vector>

using namespace std;

const int elements = 5;
const int datasets = 4;
const string db[datasets] = {"101 10", "01101", "11101", "01001"};

typedef bitset<elements> tidsets;


void constructDB()
{
  vector<tidsets> mDB;

  for (int i = 0; i < datasets; i++) {
    string tempString = db[i];
    remove(tempString.begin(), tempString.end(), ' ');

    bitset<elements> tempTD(tempString);
    mDB.push_back(tempTD);
  }

  cout << mDB.size() << endl;
}

int main(void)
{
  constructDB();
  return 0;
}
