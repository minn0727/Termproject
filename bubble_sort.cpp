#include "mpi.h"
#include <iostream>
#include <fstream>
#include <string.h>
#include <time.h>
#include <cstdlib>
using namespace std;

long original_len;//the length of the data to be sorted
int N ;
int *localArray;
int *array;
int *result;
int rank;//rank of the current processor
int proc_number;//number of processors
int start, size;
int leftrank = -1;
int rightrank = -1;
double t1, t2, t3, t4, tdist, tsort;

//load the unsorted data
int *data_loading(char *dir)
{
	fstream read;
	string temp;
	int line=0;
	read.open(dir, ios::in);
	getline(read, temp);
	int size=atoi(temp.c_str());
	cout<<size<<endl;
	N=size;
	int *array=new int[size+1];
	int i=0;

	getline(read, temp);
	char *str_arr=new char[temp.size()+1];
	strcpy(str_arr, temp.c_str());
	char *list=strtok(str_arr, ",");
	while(NULL!=list)
	{
		array[i++]=atoi(list);
		list=strtok(NULL, ",");
	}
	read.close();
	return array;
}

void splitArray(){
  int nmin, nleftrank, nnum;
  nmin=N/proc_number;
  nleftrank=N%proc_number;
  int k=0;
  for (int i = 0; i < proc_number; i++) {
     nnum = (i < nleftrank) ? nmin + 1 : nmin;
     if(i==rank){
       start=k+1;
       size=nnum;
       localArray=(int *)malloc((size+2)*sizeof(int));
       for(int j=1;j<size+1;j++)
        localArray[j]=array[k+j-1];
     }
  k+=nnum;
  }
}
void sendAndSort(){
  MPI_Status status;
  for(int i=0;i<N;i++){
    int sleftrank=0,srightrank=0;
    if(i%2==0){
		double ta = MPI_Wtime();
      for(int j=0;j<proc_number;j++){
        if(j==rank){
            int flag=(start+size-1)%2;
            if(flag==1 && rank < proc_number-1){
              srightrank++;
              MPI_Send(&localArray[size] ,1 ,MPI_INT ,rightrank ,0 ,MPI_COMM_WORLD);
              MPI_Recv(&localArray[size+1] ,1 ,MPI_INT ,rightrank ,1 ,MPI_COMM_WORLD ,&status);
            }/*else*/ if((start%2)==0 && rank > 0){
              sleftrank++;
              MPI_Send(&localArray[1],1,MPI_INT,leftrank,1,MPI_COMM_WORLD);
              MPI_Recv(&localArray[0],1,MPI_INT,leftrank,0,MPI_COMM_WORLD,&status);
            }
        }
      }
	  double tb = MPI_Wtime();
      for(int k=1-sleftrank;k<size+srightrank;k+=2)
        if(localArray[k]>localArray[k+1]){
          localArray[k+1]^=localArray[k];
          localArray[k]^=localArray[k+1];
        }
	  double tc = MPI_Wtime();
	  tdist += tb - ta;
	  tsort += tc - tb;
    }
    else{
		double ta = MPI_Wtime();
      for(int j=0;j<proc_number;j++){
        if(j==rank){
            int flag=(start+size-1)%2;
            if((start%2)==1 && rank > 0){
              sleftrank++;
              MPI_Send(&localArray[1],1,MPI_INT,leftrank,1,MPI_COMM_WORLD);
              MPI_Recv(&localArray[0],1,MPI_INT,leftrank,0,MPI_COMM_WORLD,&status);

            }/*else*/ if(flag==0 && rank < proc_number-1){
              srightrank++;
              MPI_Send(&localArray[size],1,MPI_INT,rightrank,0,MPI_COMM_WORLD);
              MPI_Recv(&localArray[size+1],1,MPI_INT,rightrank,1,MPI_COMM_WORLD,&status);
            }
        }
      }
	  double tb = MPI_Wtime();
      for(int k=(start%2)+1-(sleftrank*2);k<size+srightrank;k+=2)
        if(localArray[k]>localArray[k+1]){
          localArray[k]^=localArray[k+1];
          localArray[k+1]^=localArray[k];
          localArray[k]^=localArray[k+1];
        }
	  double tc = MPI_Wtime();
	  tdist += tb - ta;
	  tsort += tc - tb;
    }
  }
}
void collectFromWorkers(){
  MPI_Status status;
  result=	(int *)malloc(N*sizeof(int));
  int buffer[2];
  for(int i=1;i<proc_number;i++){
    MPI_Recv(buffer,2,MPI_INT,i,0,MPI_COMM_WORLD,&status);
    int iStart=buffer[0];
    int iSize=buffer[1];
    MPI_Recv(&result[iStart-1],iSize,MPI_INT,i,1,MPI_COMM_WORLD,&status);
  }
  for(int i=start;i<start+size;i++)
    result[i-1]=localArray[i];
}
void reportToMaster(){
  int buffer[2];
  buffer[0]=start;
  buffer[1]=size;
  MPI_Send(buffer,2,MPI_INT,0,0,MPI_COMM_WORLD);
  MPI_Send(&localArray[1],size,MPI_INT,0,1,MPI_COMM_WORLD);
}

int main(int argc, char **argv)
{
	if(argc!=2)
	{
		cout<<"Please check your input"<<endl;
		exit(0);
	}
	MPI_Init(&argc,&argv);
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);
	MPI_Comm_size(MPI_COMM_WORLD,&proc_number);


		array = data_loading(argv[1]);
  if(rank==0){
    rightrank=rank+1;
  }else if(rank==proc_number-1){
    leftrank=rank-1;
  }else{
    rightrank=rank+1;
    leftrank=rank-1;
  }

	double ti= MPI_Wtime();
	t1 = MPI_Wtime();
  splitArray();
  t2 = MPI_Wtime();

  sendAndSort();

  t3 = MPI_Wtime();
  if(rank==0){
    collectFromWorkers();
  }else{
    reportToMaster();
  }
  t4 = MPI_Wtime();
	double tf= MPI_Wtime();
	if(rank==0){
		cout << "Total execution time: " << tf - ti << endl;
		cout << "Time to divide: " << t2 - t1 << endl;
		cout << "Time to distribute: " << tdist << endl;
		cout << "Time to sort: " << tsort << endl;
		cout << "Time to gather: " << t4 - t3 << endl;
		free(result);
	}
  MPI_Finalize();
  free(array);
  free(localArray);
  return 0;
}

