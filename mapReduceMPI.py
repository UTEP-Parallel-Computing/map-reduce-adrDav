#!/usr/bin/env python3
'''
Assignment #3 - Map Reduce MPI
Parallel Computing
Code Implementation: Adrian Avendano
'''
import time
from mpi4py import MPI
import re

word_list = ["hate", "love", "death", "night", "sleep", "time", "henry", "hamlet", "you", "my", "blood", "poison", "macbeth", "king", "heart", "honest"]

file_list = ["shakespeare1.txt","shakespeare2.txt", "shakespeare3.txt",
"shakespeare4.txt", "shakespeare5.txt", "shakespeare6.txt", "shakespeare7.txt", "shakespeare8.txt"]

def num_words_file(curr_word, curr_file):
    # counter for words
    counter = 0
    # open file
    with open(curr_file, 'r') as f:
        # iterate through the lines of the file
        for line in f:
            # length of the list returned gives total words on line
            counter += len(re.findall(curr_word, line, re.IGNORECASE))
    return counter

def count_words(word_list):
    # some of the code was taken from professors pruitt examples (i.e., gatherDict.py, distributeList.py)

    # get the world communicator
    comm = MPI.COMM_WORLD

    # get our rank (process #)
    rank = comm.Get_rank()

    # get the size of the communicator in # processes
    size = comm.Get_size()

    # create dictionary for processes.
    dict_processes = {}

    # set up of unique dictionaries for each process
    for word, count in dict_processes.items():
        dict_processes[word] = count * rank
    
    # local list for this process
    local_list = []

    # distributes the work
    if rank is 0:
        first_distribution = word_list[:8//size]

        for process in range(1, size):
            # start of slice
            start_index = int(8/size * process)
            # end of slice
            end_index = int(8/size * (process + 1))

            if process is 1:
                # creation of the slice to send
                slice_send = word_list[start_index:end_index]
                slice_send += first_distribution
            else:
                slice_send = word_list[start_index:end_index]
            # send slice
            comm.send(slice_send, dest=process, tag=0)
    # else receive message
    else:
        # receive message from thread 0 with a tag of 0
        local_list = comm.recv(source=0, tag=0)

    # create local dictionary to store words and value.
    local_dict_words  = {}
    # iterate through the local list
    for item in local_list:
        # iterate through the list of words
        for word in word_list:
            # initialize local dictionary
            local_dict_words[word] = 0

        # iterate through the list of files
        for i in range(len(file_list)):
            for word in word_list:
                local_dict_words[word] += num_words_file(word, file_list[i])
    
    # receive dictionaries
    if rank is 0:
        for process in range(1, size):
            # receive message from thread 0 with a tag of 0
            rec_dict = comm.recv(source=0, tag=0)
            for key, val in rec_dict.items():
                if key in rec_dict:
                    dict_processes[key] += val
                else:
                    dict_processes[key] = val
    else:
        comm.send(local_dict_words, dest=0, tag=0)
    
    return dict_processes

def main():
    start_time = time.perf_counter()
    dict_words = count_words(word_list)
    end_time = time.perf_counter()

    rank = MPI.COMM_WORLD.Get_rank()

    if rank is 0:
        print(f'Total time was {end_time - start_time}')
        print(dict_words)
        print(f"Total word count: {sum(dict_words.values())}")


if __name__ == '__main__':
    # execute only if run as a script
    main()

