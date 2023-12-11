# Parallel Sorting Algorithms Using MPI
Parallel Bubblesort, Quicksort, Bucketsort implementation using MPI

## Steps
1. Download
```
git clone https://github.com/minn0727/Termproject.git
```
2. (opt) Make an input file
```
cd input_file_generator
./data_generation [# of items to be generated] [output txt file dir]
cd ..
```
3. Run
```
mpirun [binary] -n [# of procs] [input file directory]
```
(e.g. bubble sort, 4 procs)
```
mpirun bubble_sort -n 4 ./input/largeset.txt
```
