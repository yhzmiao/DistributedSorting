#include "mpi.h"
#include <cstdio>
#include <cstdlib>
#include <queue>
#include <algorithm>
#include <unistd.h>

using namespace std;

//max array size 1GB
#define MAXARRSIZE 10000000
//max chunk 5000: about 5TB
#define MAXCHUNK 5000
//master 0
#define MASTER 0
//data to sort 10G
#define DATATOSORT 100000000

char data_to_sort[MAXARRSIZE * 10];
char data_buffer[MAXARRSIZE][255];
int sorted_order[MAXARRSIZE];

int chunk_allocate[MAXCHUNK];
//int data_tag[MAXCHUNK];
//int task_size[MAXCHUNK];
char que_buffer[MAXCHUNK][255];

struct node {
	char *str;
	int id; //the number of file

	node(char *str = NULL, int id = 0): str(str), id(id){}
}data_pointer[MAXARRSIZE];

bool operator< (node x, node y) {
	for (int i = 0; x.str[i] && y.str[i]; ++ i) {
		if (x.str[i] > y.str[i]) {
			return 1;
		}
		else if (x.str[i] < y.str[i]) {
			return 0;
		}
	}
	return 1;
}

bool cmp(node x, node y) {
	for (int i = 0; x.str[i] && y.str[i]; ++ i) {
		if (x.str[i] < y.str[i]) {
			return 1;
		}
		else if (x.str[i] > y.str[i]) {
			return 0;
		}
	}
	return 1;

}

int main (int argc, char *argv[]) { 
	int node_number, task_id, len, dest, source, chunk_size;
	int tag_tasks = -1;
	char hostname[MPI_MAX_PROCESSOR_NAME], readbuffer[255];
	double start_time, end_time;
	MPI_Status status;

	// init
	start_time = MPI_Wtime();
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &node_number);
	MPI_Comm_rank(MPI_COMM_WORLD, &task_id);
	MPI_Get_processor_name(hostname, &len);
	printf ("MPI task %d has started...\n", task_id);
	chunk_size = MAXARRSIZE;

	//Master task
	if (task_id == MASTER){

		//FILE for all chunks
		FILE *fp[MAXCHUNK];
		char file_address[50] = "/var/scratch/dsys1906/tmp/portion0000.txt";
		int number_of_chunk = DATATOSORT / MAXARRSIZE;

		int cnt = 1;
		for (int i = 0; i < number_of_chunk; ++ i) {
			file_address[33] += i / 1000;
			file_address[34] += (i % 1000) / 100;
			file_address[35] += (i % 100) / 10;
			file_address[36] += i % 10;

			fp[i] = fopen(file_address, "w");
			chunk_allocate[i] = cnt;
			//task_size[cnt] ++;
			cnt += 1;
			if (cnt >= node_number) {
				cnt = 1;
			}

			file_address[33] = file_address[34] = file_address[35] = file_address[36] = '0';
		}

		// send each node its number of tasks to solve
		//for (int i = 1; i < task_number; ++ i) {
		//	MPI_Send(&task_size[i], 1, MPI_INT, i, tag_tasks, MPI_COMM_WORLD);
		//}

		// send each task its portion of the array
		FILE *fp_tosort = fopen("/var/scratch/dsys1906/toSort.txt", "r");
		FILE *fp_read = fopen("/var/scratch/dsys1906/toSort.txt", "r");
		for (int i = cnt = 0; i < number_of_chunk; i ++) {
			// read the unsorted data
			int tosend;
			double disk_start, disk_end;
			disk_start = MPI_Wtime();
			for (tosend = 0; tosend < chunk_size; ++ tosend) {
				if (fgets(readbuffer, 255, fp_tosort) == NULL)
					break;
				for (int j = tosend * 10; j < tosend * 10 + 10; ++ j)
					data_to_sort[j] = readbuffer[j - tosend * 10];
			}
			disk_end = MPI_Wtime();

			printf("Disk IO time: %.5lf\n", disk_end - disk_start);
			dest = chunk_allocate[i];

			// recieve the data first
			if (i >= node_number - 1) {
				int torecv;
				//cnt = 0;
				for (torecv = 0; torecv < chunk_size; ++ torecv) {
					if (fgets(data_buffer[torecv], 255, fp_read) == NULL)
						break;
				}
				MPI_Recv(sorted_order, torecv, MPI_INT, dest, dest, MPI_COMM_WORLD, &status);
				printf("Received sorted order from Node %d!\n", dest);

				// fault tolerant
			
				// reorder data
				//for (int i = 0; i < torecv; ++ i) {
				//	while (sorted_order[i] != i) {
				//		//printf("^^^^%d\n", sorted_order[i]);
				//		swap(data_buffer[i], data_buffer[sorted_order[i]]);
				//		swap(sorted_order[i], sorted_order[sorted_order[i]]);
				//	}
				//}
				//printf("Tagged!\n");
				//put to portion.txt
				for (int i = 0; i < torecv; ++ i) {
					fprintf(fp[cnt], "%s", data_buffer[sorted_order[i]]);
				}
				cnt ++;
			}

			// deliver data
			//double send_start, send_end;
			MPI_Send(&tosend, 1, MPI_INT, dest, dest, MPI_COMM_WORLD);
			//send_start = MPI_Wtime();
			MPI_Send(data_to_sort, tosend * 10, MPI_CHAR, dest, dest, MPI_COMM_WORLD);
			//send_end = MPI_Wtime();
			printf("Send %d data to Node %d!\n", tosend, dest);
		}
		fclose(fp_tosort);
		
		printf("Master finished data delivery!\n");
		
		// get sorted order
		for (int i = cnt; i < number_of_chunk; i ++) {
			int torecv;
			//double recv_start, recv_end;
			//recv_start = MPI_Wtime();
			for (torecv = 0; torecv < chunk_size; ++ torecv) {
				if (fgets(data_buffer[torecv], 255, fp_read) == NULL)
					break;
			}
			source = chunk_allocate[i];

			// recv data
			//printf("hahaha%d\n", source);
			MPI_Recv(sorted_order, torecv, MPI_INT, source, source, MPI_COMM_WORLD, &status);
			//recv_end = MPI_Wtime();
			printf("Received sorted order from Node %d!\n", source);

			// fault tolerant
			// reorder data
			//for (int i = 0; i < torecv; ++ i) {
				//printf("^^^^%d\n", sorted_order[i]);
			//	while (sorted_order[i] != i) {
					//printf("^^^^%d\n", sorted_order[i]);
			//		swap(data_buffer[i], data_buffer[sorted_order[i]]);
			//		swap(sorted_order[i], sorted_order[sorted_order[i]]);
			//	}
			//}
			//put to portion.txt
			for (int i = 0; i < torecv; ++ i) {
				fprintf(fp[cnt], "%s", data_buffer[sorted_order[i]]);
			}
			cnt ++;
		}
		
		fclose(fp_read);

		// end all the node
		for (int i = 1; i < node_number; ++ i) {
			int val = -1;
			MPI_Send(&val, 1, MPI_INT, i, i, MPI_COMM_WORLD);
		}
		for (int i = 0; i < number_of_chunk; ++ i) {
			fclose(fp[i]);
		}

		printf("All node stopped!\n");

		// merge the result
		priority_queue<node> pque;
		FILE *fp_result = fopen("/var/scratch/dsys1906/sorted.txt", "w");

		while (!pque.empty()) {
			pque.pop();
		}

		for (int i = 0; i < number_of_chunk; ++ i) {
			file_address[33] += i / 1000;
			file_address[34] += (i % 1000) / 100;
			file_address[35] += (i % 100) / 10;
			file_address[36] += i % 10;

			fp[i] = fopen(file_address, "r");
			fgets(que_buffer[i], 255, fp[i]);
			pque.push(node(que_buffer[i], i));

			file_address[33] = file_address[34] = file_address[35] = file_address[36] = '0';
		}

		// k_way merge sort
		double merge_start, merge_end;
		merge_start = MPI_Wtime();
		while (!pque.empty()) {
			int next_chunk = pque.top().id;

			fprintf(fp_result, "%s", pque.top().str);
			pque.pop();
			
			if (fgets(que_buffer[next_chunk], 255, fp[next_chunk]) != NULL) {
				pque.push(node(que_buffer[next_chunk], next_chunk));
			}
		}
		merge_end = MPI_Wtime();
		printf("master uses %.5lfs to merge all the result!\n", merge_end - merge_start);

		for (int i = 0; i < number_of_chunk; ++ i) {
			fclose(fp[i]);
		}

		fclose(fp_result);
		end_time = MPI_Wtime();
		printf("Total running time: %.5lf\n", end_time - start_time);
	}
	
	// sort on each node
	if (task_id != MASTER) {
		// get data from master
		while (1) {
			int datasize;
			MPI_Recv(&datasize, 1, MPI_INT, MASTER, task_id, MPI_COMM_WORLD, &status);
			//printf("%d\n", datasize);
			if (datasize == -1) {
				break;
			}
			double recv_start, recv_end;
			recv_start = MPI_Wtime();
			MPI_Recv(data_to_sort, datasize * 10, MPI_CHAR, MASTER, task_id, MPI_COMM_WORLD, &status);
			recv_end = MPI_Wtime();

			printf("Receiving time usage %.5lf\n", recv_end - recv_start);
			
			//for (int i = 0; i < 100; ++ i) {
			//	printf("%c", data_to_sort[i]);
			//}
			//printf("\n");
			for (int i = 0; i < datasize; ++ i) {
				data_pointer[i].str = &data_to_sort[i * 10];
				data_pointer[i].id = i;
			}

			double sort_start, sort_end;
			sort_start = MPI_Wtime();
			sort(data_pointer, data_pointer + datasize, cmp);
			//sleep(10);
			sort_end = MPI_Wtime();

			printf("Node %d sorted %d data with time of %.5lfs!\n", task_id, datasize, sort_end - sort_start);

			// send order to master
			for (int i = 0; i < datasize; ++ i) {
				sorted_order[i] = data_pointer[i].id;
			}
			//printf("Finish sorting %d data on Node %d!\n", datasize, task_id);
			MPI_Send(sorted_order, datasize, MPI_INT, MASTER, task_id, MPI_COMM_WORLD);
			//printf("Node %d sent sorted_order to MASTER!\n", task_id);
		}
	}
	MPI_Finalize();
	return 0;
}

