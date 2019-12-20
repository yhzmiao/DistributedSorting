#include <cstdio>
#include <algorithm>

using namespace std;

int main() {
	char a[2][10];

	a[0][0] = '1';
	a[0][1] = '2';
	a[0][2] = '3';
	a[0][3] = '\0';
	a[1][0] = '3';
	a[1][1] = '2';
	a[1][2] = '1';
	a[1][3] = '\0';
	
	printf("%s\t%s\n", a[0], a[1]);
	swap(a[0], a[1]);
	printf("%s\t%s\n", a[0], a[1]);
	return 0;
}
