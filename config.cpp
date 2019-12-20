#include <cstdio>
#include <iostream>
#include <cstdlib>

using namespace std;

int main() {
	FILE *conf = fopen("config.txt", "r");

	FILE *codegen = fopen("dsort.cpp", "w");
	FILE *datagen = fopen("datagen.sh", "w");
	FILE *dataval = fopen("dataval.sh", "w");
	FILE *prun = fopen("mpirun.sh", "w");

	char readbuffer[1000];
	int size = 0;
	int node = 0;
	int das = 0;
	char road[255];
	for (int i = 0; i < 6; ++ i) {
		fgets(readbuffer, 255, conf);
		
		if (i == 0) {
			das = readbuffer[5] == 't';
		}

		if (i == 1) {
			int j = 0;
			while (readbuffer[j] != '=')
				++ j;
			for (j ++; readbuffer[j] != '\n' && readbuffer[j]; ++ j) {
				size = size * 10 + readbuffer[j] - '0';
			}
		}

		if (i == 4) {
			int j = 0;
			while (readbuffer[j] != '=')
				++ j;
			j ++;
			int k = 0;
			for (k = j; readbuffer[k] != '\n' && readbuffer[k]; ++ k)
				road[k - j] = readbuffer[k];
			road[k] = 0;
		}

		if (i == 5) {
			int j = 0;
			while (readbuffer[j] != '=')
				++ j;
			for (j ++; readbuffer[j] != '\n' && readbuffer[j]; ++ j) {
				node = node * 10 + readbuffer[j] - '0';
			}
		}
	}
	int cnt = 0;
	int linenumber;

	FILE *codereader;
	if (das + node >= 5) {
		codereader = fopen("distributed_sorting.cpp", "r");
		linenumber = 21;
	}
	else {
		codereader = fopen("distributed_sorting_small.cpp", "r");
		linenumber = 17;
	}
	
	while (fgets(readbuffer, 1000, codereader) != NULL) {
		cnt ++;
		if (cnt == linenumber)
			fprintf(codegen, "#define DATATOSORT %d\n", size);
		else 
			fprintf(codegen, "%s", readbuffer);
	}
	
	fprintf(datagen, "./gensort -a %d %stoSort.txt\n", size, road);
	fprintf(dataval, "./valsort %ssorted.txt\n", road);
	fprintf(prun, "prun -np %d -script $PRUN_ETC/prun-openmpi ./dsort\n", node);

	fclose(conf);
	fclose(codereader);
	fclose(codegen);
	fclose(datagen);
	fclose(dataval);
	fclose(prun);
}
