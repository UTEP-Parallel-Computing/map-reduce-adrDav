#!/usr/bin/env python3
'''
Assignment #2 - Map Reduce
Parallel Computing
Code Implementation: Adrian Avendano
'''
import time
import pymp
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

def count_words(word_list, num_threads):
    # create shared dictionary
    shared_dict_words = pymp.shared.dict()
    
    # setting all values inside dictionaries to zero
    for word in word_list:
        shared_dict_words[word] = 0
        
    print()
    print(f"Starting count_words function with {num_threads} thread(s)")
    start_time = time.perf_counter()
    
    with pymp.Parallel(num_threads) as p:
        # create local dictionary
        local_dict_words = {}
        
        # setting all values inside dictionaries to zero
        for word in word_list:
            local_dict_words[word] = 0
        
        # iterate through the list of files
        for i in p.range(len(file_list)):
            for word in word_list:
                local_dict_words[word] += num_words_file(word, file_list[i])
        
        # acquire lock for region
        lock = p.lock
        
        for word in word_list:
            # acquiring lock to avoid inaccurate result
            lock.acquire()
            shared_dict_words[word] += local_dict_words[word]
            # releasing lock to avoid deadlock
            lock.release()
            
    end_time = time.perf_counter()
    print(f"Elapsed time for thread #{num_threads}: {end_time-start_time}")
    print(shared_dict_words)
    print(f"Total word count: {sum(shared_dict_words.values())}")
        
    return shared_dict_words

def main():
    for i in range(4):
        count_words(word_list, 2**i)
    
    
if __name__ == '__main__':
    # execute only if run as a script
    main()

