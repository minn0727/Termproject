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
MPI_Group entire_group;//the entire group
MPI_Comm entire_comm;//the entire communicator
long *sorted;//the sorted data
double t1, t2, t3, t4, t5;

int partition(long *arr, int low, int high){
    int pivot = arr[high];
    int i = (low - 1);
    int j,temp;
    for (j=low;j<=high-1;j++){
	if(arr[j] < pivot){
	     i++;
             temp=arr[i];  
             arr[i]=arr[j];
             arr[j]=temp;	
	}
    }
    temp=arr[i+1];  
    arr[i+1]=arr[high];
    arr[high]=temp; 
    return (i+1);
}

int hoare_partition(long *arr, int low, int high){
    int middle = floor((low+high)/2);
    int pivot = arr[middle];
    int j,temp;
    // move pivot to the end
    temp=arr[middle];  
    arr[middle]=arr[high];
    arr[high]=temp;

    int i = (low - 1);
    for (j=low;j<=high-1;j++){
        if(arr[j] < pivot){
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

void quicksort(long *number,int first, int last){
    if(first<last){
        int pivot_index = partition(number, first, last);
        quicksort(number,first,pivot_index-1);
        quicksort(number,pivot_index+1,last);
    }
}

int quicksort_recursive(long* arr, int arrSize, int currProcRank, int maxRank, int rankIndex) {
    MPI_Status status;

    // Calculate the rank of the Cluster which I'll send the other half
    int shareProc = currProcRank + pow(2, rankIndex);
    // Move to lower layer in the tree
    rankIndex++;

    // If no Cluster is available, sort sequentially by yourself and return
    if (shareProc > maxRank) {
        MPI_Barrier(MPI_COMM_WORLD);
	    quicksort(arr, 0, arrSize-1 );
        return 0;
    }
    // Divide array in two parts with the pivot in between
    int j = 0;
    int pivotIndex;
    pivotIndex = hoare_partition(arr, j, arrSize-1 );

    // Send partition based on size(always send the smaller part), 
    // Sort the remaining partitions,
    // Receive sorted partition
    if (pivotIndex <= arrSize - pivotIndex) {
        MPI_Send(arr, pivotIndex , MPI_LONG, shareProc, pivotIndex, MPI_COMM_WORLD);
	    quicksort_recursive((arr + pivotIndex+1), (arrSize - pivotIndex-1 ), currProcRank, maxRank, rankIndex); 
        MPI_Recv(arr, pivotIndex , MPI_LONG, shareProc, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    }
    else {
        MPI_Send((arr + pivotIndex+1), arrSize - pivotIndex-1, MPI_LONG, shareProc, pivotIndex + 1, MPI_COMM_WORLD);
        quicksort_recursive(arr, (pivotIndex), currProcRank, maxRank, rankIndex);
        MPI_Recv((arr + pivotIndex+1), arrSize - pivotIndex-1, MPI_LONG, shareProc, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    }
	return 0;
}

/*
//IncOrder for qsort
int IncOrder(const void *e1, const void *e2)
{
	return (*((long *)e1)-*((long *)e2));
}
*/

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

/*
//the main process of parellel quick sort
void parallel_quick_sort(long *p_data, long p_data_size)
{
	int comm_proc_rank;//the rank of process to communicate with
	long *data_left;//data left on the current process
	long *data_send;//data to be sent
	long *data_recv;//data received
	long *data_merge;//data after merging
	long left_size, send_size, recv_size, merge_size;
	int dimension=(int)ceil(log(proc_number)/log(2));
	int mask=proc_number;
	long pivot;
	int comm_rank;

	//initializing merge_data
	merge_size=p_data_size;
	data_merge=new long[merge_size];
	memcpy(data_merge, p_data, merge_size*sizeof(long));

	//the iteration
	for(int i=dimension;i>0;i--)
	{	
		int comm_num=proc_number/mask;
		static int *group_ranks;
		group_ranks=(int *)malloc(mask*sizeof(int));	
		MPI_Group new_group;
		MPI_Comm new_comm;
		if(rank<mask)
		{
			for(int j=0;j<mask;j++)
			{
				group_ranks[j]=j;
			}
			MPI_Group_incl(entire_group, mask, group_ranks, &new_group);
			MPI_Comm_create(entire_comm, new_group, &new_comm);
			if(0==rank)
			{
				pivot=data_merge[merge_size/2];
			}
			MPI_Bcast(&pivot, 1, MPI_LONG, 0, new_comm);
			if(new_comm!=MPI_COMM_NULL)
			{
			
				MPI_Comm_free(&new_comm);
				MPI_Group_free(&new_group);
			}
		}
		else
		{
			int start=(int)(rank/mask)*mask;
			for(int j=0;j<mask;j++)
			{
				group_ranks[j]=start+j;
			}
			
			MPI_Group_incl(entire_group, mask, group_ranks, &new_group);
			MPI_Comm_create(entire_comm, new_group, &new_comm);
			if(start==rank)
			{
				pivot=data_merge[merge_size/2];
			}
			MPI_Bcast(&pivot, 1, MPI_LONG, 0, new_comm);
			if(new_comm!=MPI_COMM_NULL)
			{
			
				MPI_Comm_free(&new_comm);
				MPI_Group_free(&new_group);
			}
		}

		//begin partition
		long left_i=0, send_i=0;
		for(int i=0;i<proc_number;i+=mask)
		{
			//higer position
			if(rank>=i&&rank<i+mask/2)
			{
				for(long j=0;j<merge_size;j++)
				{
					if(data_merge[j]<=pivot) left_i++;
					else send_i++;
				}
				data_send=new long[send_i];
				data_left=new long[left_i];
				left_size=left_i;
				send_size=send_i;
				send_i=0;
				left_i=0;
				for(long j=0;j<merge_size;j++)
				{
					if(data_merge[j]<=pivot) data_left[left_i++]=data_merge[j];
					else data_send[send_i++]=data_merge[j];
				}
				comm_rank=rank+mask/2;
				
			}
			//lower position
			else if(rank>=i+mask/2&&rank<i+mask)
			{
				for(long j=0;j<merge_size;j++)
				{
					if(data_merge[j]<=pivot) send_i++;
					else left_i++;
				}
				data_send=new long[send_i];
				data_left=new long[left_i];
				left_size=left_i;
				send_size=send_i;
				send_i=0;
				left_i=0;
				for(long j=0;j<merge_size;j++)
				{
					if(data_merge[j]<=pivot) data_send[send_i++]=data_merge[j];
					else data_left[left_i++]=data_merge[j];
				}
				comm_rank=rank-mask/2;
			}
		}

		
		MPI_Status status;

		//send and receive the data size
		MPI_Sendrecv(&send_size, 1, MPI_LONG, comm_rank, 0, &recv_size, 1, MPI_LONG, comm_rank, 0, entire_comm, &status);
		data_recv=new long[recv_size];
		//send and receive data
		MPI_Sendrecv(data_send, send_size, MPI_LONG, comm_rank, 0, data_recv, recv_size, MPI_LONG, comm_rank, 0, entire_comm, &status);

		//merge
		merge_size=left_size+recv_size;
		data_merge=new long[merge_size];
		long pos=0, left_pos=0, recv_pos=0;
		while(left_pos<left_size) data_merge[pos++]=data_left[left_pos++];
		while(recv_pos<recv_size) data_merge[pos++]=data_recv[recv_pos++];
		mask=mask>>1;

	}

	qsort(data_merge, merge_size, sizeof(long), IncOrder);
	t4 = MPI_Wtime();


	//Gather the results to rank 0
	int *recv_cnt=new int[proc_number];
	int *displs=new int[proc_number];
	long *sorted=new long[original_len];
	MPI_Gather(&merge_size, 1, MPI_LONG, recv_cnt, 1, MPI_LONG, 0, entire_comm);
	displs[0]=0;
	for(int i=1;i<proc_number;i++)
	{
		displs[i]=displs[i-1]+recv_cnt[i-1];
	}
	
	MPI_Gatherv(data_merge, merge_size, MPI_LONG, sorted, recv_cnt, displs, MPI_LONG, 0, entire_comm);
	t5 = MPI_Wtime();

	//print the result on rank 0
//	if(0==rank)
//	{
//		for(long i=0;i<original_len;i++)
//		{
//			cout<<sorted[i]<<" ";
//		}
//	}

}
*/

int main(int argc, char **argv)
{
	if(argc!=2)
	{
		cout<<"Please check your input"<<endl;
		exit(0);
	}
	long proc_data_size;//size of data on the current process
	long *proc_data;//data on the current process
	long *original;//the unsortd data
	double exec_time;

	MPI_Init(&argc, &argv);
	exec_time=-MPI_Wtime();
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &proc_number);
	MPI_Comm_group(MPI_COMM_WORLD, &entire_group);
	entire_comm=MPI_COMM_WORLD;
	t1 = MPI_Wtime();
	int rankPower = 0;
	while(pow(2, rankPower) <= rank)
		rankPower++;

	if(rank == 0) {
		original = data_loading(argv[1]);
		quicksort_recursive(original, original_len, rank, proc_number - 1, rankPower);
	} else {
        // All other Clusters wait for their subarray to arrive,
        // they sort it and they send it back.
        MPI_Status status;
        int subarray_size;
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        // Capturing size of the array to receive
        MPI_Get_count(&status, MPI_INT, &subarray_size);
	    int source_process = status.MPI_SOURCE;     
        long subarray[subarray_size];
        MPI_Recv(subarray, subarray_size, MPI_LONG, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        quicksort_recursive(subarray, subarray_size, rank, proc_number - 1, rankPower);
        MPI_Send(subarray, subarray_size, MPI_LONG, source_process, 0, MPI_COMM_WORLD);
	}



















	/*
	if(0==rank)
	{
		original=data_loading(argv[1]);
		proc_data_size=original_len/proc_number;
	}
	MPI_Bcast(&original_len, 1, MPI_LONG, 0, MPI_COMM_WORLD);
	MPI_Bcast(&proc_data_size, 1, MPI_LONG, 0, MPI_COMM_WORLD);
	t2 = MPI_Wtime();
	proc_data=new long[proc_data_size];
	MPI_Scatter(original, proc_data_size, MPI_LONG, proc_data, proc_data_size, MPI_LONG, 0, MPI_COMM_WORLD);
	t3 = MPI_Wtime();
	

	parallel_quick_sort(proc_data, proc_data_size);
	*/
	exec_time+=MPI_Wtime();
	cout<<"time of proc"<<rank<<":"<<exec_time<<endl;
	if(0==rank) {
		cout << "Total execution time: " << exec_time << endl;
//		cout << "Time to divide: " << t2 - t1 << endl;
//		cout << "Time to distribute: " << t3 - t2 << endl;
//		cout << "Time to sort: " << t4 - t3 << endl;
//		cout << "Time to gather: " << t5 - t4 << endl;
	}

	MPI_Finalize();
	return 0;
}
