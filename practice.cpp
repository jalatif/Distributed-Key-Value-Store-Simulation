#include <iostream>
#include "Application.h"


using namespace std;

static const char alphanum2[] =
        "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";

class Dummy{
public:
    int my;
    Dummy();
};

Dummy::Dummy() {
    my = 10;
}
int main(int argc, char *argv[]){

    Dummy d;
    printf("Value = %d\n", d.my);

    FILE *fp = fopen(argv[1],"r");

    int m1, m2;
    fscanf(fp,"MAX_NNB: %d", &m1);
    fscanf(fp,"\nSINGLE_FAILURE: %d", &m2);

    fclose(fp);

    cout << "M1 = " << m1 << endl;
    cout << "M2 = " << m2 << endl;
    cout << "Size = " << sizeof(alphanum2) << " Length = " << sizeof(alphanum2) / sizeof(alphanum2[0]) << " First item = " << alphanum2[10]<< endl;
    std::vector<int> v1, v2;
    v1.push_back(1);v1.push_back(5);v1.push_back(3);v1.push_back(4);
    v2 = v1;

    vector<int>::iterator it;

    int i = 0;
    for (it = v2.begin(); it != v2.end(); it++){
        cout << i << "th Element = " << *it << endl;
        i++;
    }
    bool b1 = (v1!=v2);
    cout << "Equal = " << b1 << endl;
    v1[2] = 6;

    i = 0;
    for (it = v2.begin(); it != v2.end(); it++){
        cout << i << "th Element = " << *it << endl;
        i++;
    }
    b1 = (v1!=v2);
    cout << "Equal = " << b1 << endl;

}