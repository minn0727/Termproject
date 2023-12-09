#include "mpi.h"
#include <iostream>
#include <fstream>
#include <string.h>
#include <time.h>
#include <cstdlib>
#include <stdlib.h>
#include <cassert>
#define INF (-1)
using namespace std;

long original_len;//length of the unsorted data
int rank;//rank of the current process
int proc_number;//number of processes

//Comparison function used by qsort
int compare_dbls(const void* arg1, const void* arg2)
{
	double a1 = *(double *) arg1;
	double a2 = *(double *) arg2;
	if (a1 < a2) return -1;
	else if (a1 == a2) return 0;
	else return 1;
}

// Sort the array in place
void qsort_dbls(double *array, int array_len)
{
	qsort(array, (size_t)array_len, sizeof(double), compare_dbls);
} 

//Function to find what bucket an element belongs to
//based on how many procs there are
int find_bucket(double num, int p_num)
{
	int x;
	for(x=1; x < p_num+1; x++)
	{
		double bucket_range =(double)x / (double)p_num;
		if(num <= bucket_range)
		{
			return x - 1; //return bucket number
		}
	}
	assert(0);
}


//load the unsorted data
double *data_loading(char *dir)
{
	fstream read;
	string temp;
	long line=0;
	read.open(dir, ios::in);
	getline(read, temp);
	long size=atol(temp.c_str());
	cout<<size<<endl;
	original_len=size;
	double *array=new double[size+1];
	long i=0;

	getline(read, temp);
	char *str_arr=new char[temp.size()+1];
	strcpy(str_arr, temp.c_str());
	char *list=strtok(str_arr, ",");
	while(NULL!=list)
	{
		array[i++]=(double)atol(list) /(double)(2147483647) ;
		list=strtok(NULL, ",");
	}
	read.close();
	return array;
}

//IncOrder for qsort
int IncOrder(const void *e1, const void *e2)
{
	return (*((long *)e1)-*((long *)e2));
}

int main(int argc, char **argv)
{
	if(argc!=2)
	{
		cout<<"Please check your input"<<endl;
		exit(0);
	}
	double *original;//the unsorted data
	double exec_time;

	MPI_Init(&argc, &argv);
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &proc_number);

	if(0==rank)	
		original=data_loading(argv[1]);
	exec_time=-MPI_Wtime();

	int *proc_bucket_count = (int*)calloc(proc_number, sizeof(int));
	int sub_count[1];
	double t1 = MPI_Wtime();
	if(0 == rank)
	{
		for(int i = 0; i < original_len; i++)
		{
			int bucket = find_bucket(original[i], proc_number);
			proc_bucket_count[bucket]++;
		}
	}
	double t2 = MPI_Wtime();

	//Scatter Bucket Counts
	MPI_Scatter(proc_bucket_count, 1, MPI_INT, sub_count, 1, MPI_INT, 0, MPI_COMM_WORLD);

	double *proc_buckets = (double*)calloc(sub_count[0], sizeof(double));
	int *displs = (int*)calloc(proc_number, sizeof(int));
	double *final = (double*)calloc(original_len, sizeof(double));
	int *index = (int*)calloc(proc_number, sizeof(int));

	if(0 == rank)
	{
		displs[0] = 0;
		for(int i = 1; i < proc_number; i++)
		{
			displs[i] = proc_bucket_count[i - 1] + displs[i - 1];
		}

		for(int j = 0; j < original_len; j++)
		{
			int bucket = find_bucket(original[j], proc_number);
			final[displs[bucket] + index[bucket]] = original[j];
			index[bucket]++;
		}
	}

	MPI_Scatterv(final, proc_bucket_count, displs, MPI_DOUBLE, proc_buckets, sub_count[0], MPI_DOUBLE, 0, MPI_COMM_WORLD);
	double t3 = MPI_Wtime();
	qsort_dbls(proc_buckets, sub_count[0]);
	double t4 = MPI_Wtime();
	MPI_Gatherv(proc_buckets, sub_count[0], MPI_DOUBLE, final, proc_bucket_count, displs, MPI_DOUBLE, 0, MPI_COMM_WORLD);
	double t5 = MPI_Wtime();

	exec_time+=MPI_Wtime();
	cout<<"time of rank "<<rank<<":"<<exec_time<<endl;
	if(0==rank) {
		int sorted = 1;
		int k;
		for(k = 0; k < original_len - 2; k++){
			if(final[k] > final[k+1]){
				sorted = 0;
			}
		}

		if(sorted == 1){
			printf("\nSORTING CORRECT\n");
		}else{
			printf("\nSORTING NOT CORRECT\n");
		}
		cout << "Total execution time: " << exec_time << endl;
		cout << "Time to divide: " << t2 - t1 << endl;
		cout << "Time to distribute: " << t3 - t2 << endl;
		cout << "Time to sort: " << t4 - t3 << endl;
		cout << "Time to gather: " << t5 - t4 << endl;
	}

	//print the sorted data on rank 0
//  	if(0==rank)
//  	{
//  		for(long i=0;i<original_len;i++)
//  		{
//  			cout<<final[i]<<" ";
//  		}
//  		cout<<endl;
//  	}

	MPI_Finalize();
	return 0;
}
