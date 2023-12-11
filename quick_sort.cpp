#include "mpi.h"
#include <iostream>
#include <time.h>
#include <cstdlib>
#include <fstream>
#include <string.h>
#include <cmath>
using namespace std;

long original_len;//length of the unsorted data
int rank;//rank of the curren process
int proc_number;//number of processes
long *final;
	double exec_time, t1=0, t2=0, t3=0, tdistr=0, tsort=0, tgather=0;

int hoare_partition(long *arr, int low, int high)
{
    int middle = floor((low+high)/2);
    long pivot = arr[middle];
    int j,temp;
    // move pivot to the end
    temp=arr[middle];  
    arr[middle]=arr[high];
    arr[high]=temp;

    int i = (low - 1);
    for (j=low;j<=high-1;j++)
	{
        if(arr[j] < pivot)
		{
            i++;
            temp=arr[i];  
            arr[i]=arr[j];
            arr[j]=temp;	
        }
    }
    // move pivot back
    temp=arr[i+1];  
    arr[i+1]=arr[high];
    arr[high]=temp; 

    return (i+1);
}

void quicksort(long *arr, int first, int last)
{
	if(first < last)
	{
		int pivot = hoare_partition(arr, first, last);
		quicksort(arr, first, pivot - 1);
		quicksort(arr, pivot + 1, last);
	}
}

void merge(long *arr_1, long *arr_2, long *res, int size_1, int size_2)
{
	res = new long[size_1 + size_2];
	int i = 0, j = 0, k = 0;

	while(i < size_1 && j < size_2)
	{
		if(arr_1[i] < arr_2[j])
		{
			res[k] = arr_1[i];
			k++;
			i++;
		}
		else
		{
			res[k] = arr_2[j];
			k++;
			j++;
		}

		if(i == size_1)
		{
			while(j < size_2)
			{
				res[k] = arr_2[j];
				k++;
				j++;
			}
		}
		else if(j == size_2)
		{
			while(i < size_1)
			{
				res[k] = arr_1[i];
				k++;
				i++;
			}
		}	
	}
}


//loading the unsorted data
long *data_loading(char* dir)
{
	fstream read;
	string temp;
	long line=0;
	read.open(dir, ios::in);
	getline(read,temp);
	long size=atol(temp.c_str());
	cout<<size<<endl;
	original_len=size;
	long *array=new long[size+1];
	long i=0;

	getline(read,temp);
	char *str_arr=new char[temp.size()+1];
	strcpy(str_arr, temp.c_str());
	char *list=strtok(str_arr, ",");
	while(NULL!=list)
	{
		array[i++]=atol(list);
		list=strtok(NULL, ",");
	}
	read.close();
	return array;
}

int main(int argc, char *argv[])
{
	if(argc!=2)
	{
		cout<<"Please check your input"<<endl;
		exit(0);
	}
	long proc_data_size;//size of data on the current process
	long *proc_data;//data on the current process
	long *send_data;
	long *original;//the unsortd data
	long *final;
	MPI_Status status;
	
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &proc_number);

		original = data_loading(argv[1]);
		proc_data_size = original_len / proc_number;
	exec_time = -MPI_Wtime();
	t1 = MPI_Wtime();
	MPI_Bcast(&proc_data_size, 1, MPI_LONG, 0, MPI_COMM_WORLD);
	proc_data=new long[proc_data_size];
	final = new long[original_len];

			t2 = MPI_Wtime();
	if(0 == rank)
	{
		if(proc_number > 1)
		{
			for(int i = 0; i < proc_number - 1; i++)
			{
				MPI_Send(&original[(i + 1) * proc_data_size], proc_data_size, MPI_LONG, i + 1, 0, MPI_COMM_WORLD);
			}

			for(int i = 0; i < proc_data_size; i++)
			{
				proc_data[i] = original[i];
			}

			tsort -= MPI_Wtime();
			quicksort(proc_data, 0, proc_data_size - 1);
			tsort += MPI_Wtime();
			for(int i = 0; i < proc_number; i++)
			{
				if(i > 0)
				{
					long temp_proc_data[proc_data_size];
					tgather -= MPI_Wtime();

					MPI_Recv(temp_proc_data, proc_data_size, MPI_LONG, i, 777, MPI_COMM_WORLD, &status);
					tgather += MPI_Wtime();

					long temp_final[i * proc_data_size];
					for(int j = 0; j < i*proc_data_size; j++)
					{
						temp_final[j] = final[j];
					}
					int temp_final_size = i * proc_data_size;

					tsort -= MPI_Wtime();
					merge(temp_proc_data, temp_final, final, proc_data_size, temp_final_size);
					tsort += MPI_Wtime();
				}
				else
				{
					for(int j = 0; j < proc_data_size; j++)
					{
						final[j] = proc_data[j];
					}
				}
			}
		}
		else
		{
			tsort -= MPI_Wtime();
			quicksort(original, 0, original_len - 1);
			tsort += MPI_Wtime();
			for(int i = 0; i < original_len; i++)
			{
				final[i] = original[i];
			}
		}
	}
	t3 = MPI_Wtime();


	tdistr += t3 - t2 - tsort - tgather;
	if(rank != 0)
	{
		tdistr -= MPI_Wtime();
		send_data = new long[proc_data_size];
		MPI_Recv(send_data, proc_data_size, MPI_LONG, 0, 0, MPI_COMM_WORLD, &status);
		tdistr += MPI_Wtime();
		tsort -= MPI_Wtime();
		quicksort(send_data, 0, proc_data_size - 1);
		tsort += MPI_Wtime();
		tgather -= MPI_Wtime();
		MPI_Send(send_data, proc_data_size, MPI_LONG, 0, 777, MPI_COMM_WORLD);
		tgather += MPI_Wtime();
	}
	exec_time+=MPI_Wtime();
	cout<<"time of proc"<<rank<<":"<<exec_time<<endl;

	if(0 == rank)
	{

		cout << "Total execution time: " << exec_time << endl;
		cout << "Time to divide: " << t2 - t1 << endl;
		cout << "Time to distribute: " << tdistr << endl;
		cout << "Time to sort: " << tsort << endl;
		cout << "Time to gather: " << tgather << endl;

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
