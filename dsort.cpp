#include "mpi.h"
#include <cstdio>
#include <cstdlib>
#include <queue>
#include <algorithm>
#include <string>
#include <functional>
#include <unistd.h>

using namespace std;

//max array size 1GB
#define MAXARRSIZE 10000000
//max node 1000
#define MAXNODE 1000
//max chunk
#define MAXCHUNK 1000
//master 0
#define MASTER 0
//data to sort 10G
#define DATATOSORT 100000000

//char data_to_sort[MAXARRSIZE * 10];
char data_buffer[MAXARRSIZE * 200];
//int sorted_order[MAXARRSIZE];

//int chunk_allocate[MAXCHUNK];
//int data_tag[MAXCHUNK];
//int task_size[MAXCHUNK];
int rest_val[MAXNODE], deque_val[MAXNODE];
char que_buffer[MAXNODE][200];
int broken_node[MAXNODE];

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
	int node_number, task_id, len, dest, source, chunk_size, chunk_number, package_size;
	char hostname[MPI_MAX_PROCESSOR_NAME], readbuffer[200];
	double start_time, end_time;
	MPI_Status status;
	MPI_Request request;

	hash <string> h;

	// init
	start_time = MPI_Wtime();
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &node_number);
	MPI_Comm_rank(MPI_COMM_WORLD, &task_id);
	MPI_Get_processor_name(hostname, &len);
	printf ("MPI task %d has started...\n", task_id);
	chunk_size = MAXARRSIZE;
	chunk_number = (DATATOSORT - 1) / MAXARRSIZE + 1;
	package_size = chunk_size / (node_number - 1);

	//configure
	char road[200];
	int fault_tolerance, road_size = 0;
	FILE *fconfig = fopen("config.txt", "r");
	
	for (int i = 0; i < 6; ++ i) {
		fgets(readbuffer, 200, fconfig);

		if (i == 2) {
			fault_tolerance = readbuffer[14] == 't';
		}

		if (i == 4) {
			int j;
			for (j = 0; readbuffer[j] != '='; ++ j);
			j ++;
			int k;
			for (k = j; readbuffer[k] != '\n' && readbuffer[k] != 0; k ++)
				road[k - j] = readbuffer[k];
			road[k - j] = 0;
			road_size = k - j;
		}
	}

	//Master task
	if (task_id == MASTER){
		// send each task its portion of the array
		strcpy(road + road_size, "toSort.txt");
		//printf("%s\n", road);
		FILE *fp_read = fopen(road, "r");
		dest = 1;
		double deliver_start, deliver_end;
		deliver_start = MPI_Wtime();
		
		for (int i = 0; i < chunk_number; ++ i) {
		//while (fgets(readbuffer, 255, fp_read) != NULL) {
			if (!broken_node[dest]) {
				int tosend;
				for (tosend = 0; tosend < chunk_size; ++ tosend) {
					if (fgets(&data_buffer[tosend * 200], 200, fp_read) == NULL)
						break;
				}
				if (fault_tolerance && i < node_number) {
					int flag = 0;
					double loop_start, loop_now;
					MPI_Isend(&tosend, 1, MPI_INT, dest, dest, MPI_COMM_WORLD, &request);
					loop_start = MPI_Wtime();
					while (!flag) {
						MPI_Test(&request, &flag, &status);
						loop_now = MPI_Wtime();
						if (loop_now - loop_start > 5)
							break;
					}

					if (!flag) {
						printf("Node %d is broken!\n", dest);
						broken_node[dest] = 1;
						MPI_Cancel(&request);
						MPI_Request_free(&request);
					}
				}
				else {
					MPI_Send(&tosend, 1, MPI_INT, dest, dest, MPI_COMM_WORLD);
				}
				if (fault_tolerance) {
					string str(data_buffer);
					int hashval = h(str);
					int received = 0;
					while (!received) {
						MPI_Send(data_buffer, 200 * tosend, MPI_CHAR, dest, dest, MPI_COMM_WORLD);
						MPI_Send(&hashval, 1, MPI_INT, dest, dest, MPI_COMM_WORLD);
						MPI_Recv(&received, 1, MPI_INT, dest, dest, MPI_COMM_WORLD, &status);
					}
				}
				else {
					MPI_Send(data_buffer, 200 * tosend, MPI_CHAR, dest, dest, MPI_COMM_WORLD);
				}
			}
			else {
				i --;
			}

			//Fault tolerance
			//string str(readbuffer);
			//MPI_Send()
			dest += 1;
			if (dest >= node_number)
				dest = 1;
		}
		fclose(fp_read);

		deliver_end = MPI_Wtime();
		printf("Master finished data delivery in %.5lfs!\n", deliver_end - deliver_start);

		for (int i = 1; i < node_number; ++ i) {
			int tosend = -1;
			MPI_Send(&tosend, 1, MPI_INT, i, i, MPI_COMM_WORLD);
		}

		// k_way merge sort
		priority_queue<node> pque;
		strcpy(road + road_size, "sorted.txt");
		FILE *fp_result = fopen(road, "w");

		while(!pque.empty()) {
			pque.pop();
		}

		for (int i = 1; i < node_number; ++ i) {
			//int tosend = 1;
			//MPI_Send(&tosend, 1, MPI_INT, i, i, MPI_COMM_WORLD);
			MPI_Recv(&rest_val[i], 1, MPI_INT, i, i, MPI_COMM_WORLD, &status);
			if (rest_val[i]) {
				MPI_Recv(&data_buffer[(i - 1) * rest_val[i] * 200], rest_val[i] * 200, MPI_CHAR, i, i, MPI_COMM_WORLD, &status);
				deque_val[i] = 0;
				pque.push(node(&data_buffer[(i - 1) * rest_val[i] * 200], i));
			}
		}

		double merge_start, merge_end;
		merge_start = MPI_Wtime();
		while (!pque.empty()) {
			int next_chunk = pque.top().id;

			fprintf(fp_result, "%s", pque.top().str);
			pque.pop();

			deque_val[next_chunk] ++;

			//all of a package dequed
			if (deque_val[next_chunk] == rest_val[next_chunk]) {
				// get a new one
				MPI_Recv(&rest_val[next_chunk], 1, MPI_INT, next_chunk, next_chunk, MPI_COMM_WORLD, &status);
				//printf("MASTER recieved another package from Node %d!\n", next_chunk);
				if (rest_val[next_chunk]) {
					//MPI_Recv(&data_buffer[(next_chunk - 1) * rest_val[next_chunk] * 200], rest_val[next_chunk] * 200, MPI_CHAR, next_chunk, next_chunk, MPI_COMM_WORLD, &status);
					if (fault_tolerance) {
						int hashval;
						int received = 0;
						while (!received) {
							//MPI_Recv(data_buffer, torecv * 200, MPI_CHAR, MASTER, task_id, MPI_COMM_WORLD, &status);
							MPI_Recv(&data_buffer[(next_chunk - 1) * rest_val[next_chunk] * 200], rest_val[next_chunk] * 200, MPI_CHAR, next_chunk, next_chunk, MPI_COMM_WORLD, &status);
							MPI_Recv(&hashval, 1, MPI_INT, next_chunk, next_chunk, MPI_COMM_WORLD, &status);
							string str(data_buffer);
							received = h(str) == hashval;
							MPI_Send(&received, 1, MPI_INT, next_chunk, next_chunk, MPI_COMM_WORLD);
						}
					}
					else {
						MPI_Recv(&data_buffer[(next_chunk - 1) * rest_val[next_chunk] * 200], rest_val[next_chunk] * 200, MPI_CHAR, next_chunk, next_chunk, MPI_COMM_WORLD, &status);
						//MPI_Recv(data_buffer, torecv * 200, MPI_CHAR, MASTER, task_id, MPI_COMM_WORLD, &status);
					}

					deque_val[next_chunk] = 0;
					pque.push(node(&data_buffer[(next_chunk - 1) * rest_val[next_chunk] * 200], next_chunk));
				}
			}

			else {
				pque.push(node(&data_buffer[((next_chunk - 1) * rest_val[next_chunk] + deque_val[next_chunk]) * 200], next_chunk));
			}


			/*
			   if (!node_finish[next_chunk]) {
			   int tosend = 1;
			   MPI_Send(&tosend, 1, MPI_INT, next_chunk, next_chunk, MPI_COMM_WORLD);
			   int rest_val;
			   MPI_Recv(&rest_val, 1, MPI_INT, next_chunk, next_chunk, MPI_COMM_WORLD, &status);
			   if (rest_val) {
			   MPI_Recv(data_buffer, 200, MPI_CHAR, next_chunk, next_chunk, MPI_COMM_WORLD, &status);
			   pque.push(node(que_buffer[next_chunk], next_chunk));
			   }
			   else {
			   node_finish[next_chunk] = 1;
			   }
			   }
			   */
		}
		merge_end = MPI_Wtime();
		printf("master uses %.5lfs to merge all the result!\n", merge_end - merge_start);
		//Finish task
		fclose(fp_result);
		/*
		   for (int i = 1; i < node_number; ++ i) {
		   int tosend = -1;
		   MPI_Send(&tosend, 1, MPI_INT, i, i, MPI_COMM_WORLD);
		   }
		   */
		end_time = MPI_Wtime();
		printf("Total running time: %.5lf\n", end_time - start_time);
		}

		// sort on each node
		if (task_id != MASTER) {
			// get data from master
			//int nowsize = 0;
			int nowchunk = 0;
			FILE *fp[MAXCHUNK];
			while (1) {
				int torecv;
				MPI_Recv(&torecv, 1, MPI_INT, MASTER, task_id, MPI_COMM_WORLD, &status);
				if (torecv == -1)
					break;

				if (fault_tolerance) {
					int hashval;
					int received = 0;
					while (!received) {
						MPI_Recv(data_buffer, torecv * 200, MPI_CHAR, MASTER, task_id, MPI_COMM_WORLD, &status);
						MPI_Recv(&hashval, 1, MPI_INT, MASTER, task_id, MPI_COMM_WORLD, &status);
						string str(data_buffer);
						received = h(str) == hashval;
						MPI_Send(&received, 1, MPI_INT, MASTER, task_id, MPI_COMM_WORLD);
					}
				}
				else {
					MPI_Recv(data_buffer, torecv * 200, MPI_CHAR, MASTER, task_id, MPI_COMM_WORLD, &status);
				}
				for (int i = 0; i < torecv; ++ i) {
					data_pointer[i].str = &data_buffer[i * 200];
					data_pointer[i].id = i;
				}
				//printf("%d\n", nowsize);
				char filename[50] = "tmp/portion000000.txt";
				filename[11] += task_id / 100;
				filename[12] += (task_id % 100) / 10;
				filename[13] += task_id % 10;

				filename[14] += nowchunk / 100;
				filename[15] += (nowchunk % 100) / 10;
				filename[16] += nowchunk % 10;

				strcpy(road + road_size, filename);

				fp[nowchunk] = fopen(road, "w");

				sort(data_pointer, data_pointer + torecv, cmp);

				for (int i = 0; i < torecv; ++ i) {
					fprintf(fp[nowchunk], "%s", data_pointer[i].str);
				}

				fclose(fp[nowchunk]);
				nowchunk ++;
				printf("Node %d recieves %d chunk!\n", task_id, nowchunk);
			}

			//merge and return the data
			//merge sort
			priority_queue<node> pque;

			while (!pque.empty()) {
				pque.pop();
			}

			//file pointer
			for (int i = 0; i < nowchunk; ++ i) {
				char filename[50] = "tmp/portion000000.txt";
				filename[11] += task_id / 100;
				filename[12] += (task_id % 100) / 10;
				filename[13] += task_id % 10;

				filename[14] += i / 100;
				filename[15] += (i % 100) / 10;
				filename[16] += i % 10;

				strcpy(road + road_size, filename);
				fp[i] = fopen(road, "r");

				if(fgets(que_buffer[i], 200, fp[i]) != NULL) {
					pque.push(node(que_buffer[i], i));
				}
			}

			//answer the master
			int cnt;
			while (1) {
				int torecv = package_size;

				cnt = 0;
				while (!pque.empty()) {
					int next_chunk = pque.top().id;
					strncpy(&data_buffer[cnt * 200], pque.top().str, 200);
					pque.pop();

					if (fgets(que_buffer[next_chunk], 200, fp[next_chunk]) != NULL) {
						pque.push(node(que_buffer[next_chunk], next_chunk));
					}
					cnt ++;
					if (cnt == torecv)
						break;
				}

				//MPI_Recv(&torecv, 1, MPI_INT, MASTER, task_id, MPI_COMM_WORLD, &status);

				//if (pque.empty()) {
				if (!cnt) {
					int tosend = 0;
					MPI_Send(&tosend, 1, MPI_INT, MASTER, task_id, MPI_COMM_WORLD);
					break;
				}

				MPI_Send(&cnt, 1, MPI_INT, MASTER, task_id, MPI_COMM_WORLD);
				
				//MPI_Send(data_buffer, cnt * 200, MPI_CHAR, MASTER, task_id, MPI_COMM_WORLD);
				
				if (fault_tolerance) {
					string str(data_buffer);
					int hashval = h(str);
					int received = 0;
					while (!received) {
						MPI_Send(data_buffer, cnt * 200, MPI_CHAR, MASTER, task_id, MPI_COMM_WORLD);
						MPI_Send(&hashval, 1, MPI_INT, MASTER, task_id, MPI_COMM_WORLD);
						MPI_Recv(&received, 1, MPI_INT, MASTER, task_id, MPI_COMM_WORLD, &status);
					}
				}
				else {
					MPI_Send(data_buffer, cnt * 200, MPI_CHAR, MASTER, task_id, MPI_COMM_WORLD);
				}

			}

			for (int i = 0; i < nowchunk; ++ i) {
				fclose(fp[i]);
			}
			}
			MPI_Finalize();
			return 0;
		}

