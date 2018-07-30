import os
import sys
from tqdm import tqdm

INPUT = 'C:\\Users\\qtdwp0\\Desktop\\cumulativechange\\all_route_history\\combined.txt'
OUTPUT = 'C:\\Users\\qtdwp0\\Desktop\\cumulativechange\\output.txt'
SECTION_D = [125, 129, 130, 131, 132, 134, 135, 136, 137, 138, 141]
SECTION_A = [28, 57, 58, 13, 42, 47, 48, 18, 19, 17, 23]


def is_mc(a):
    if (a[43] in ['A', 'C', 'F', 'S']):
        return True
    else:
        return False
        
        
def is_4003(a):
    if (a[43] in ['B', 'I']):
        return True
    else:
        return False

        
def process_and_store_mc(a):
    for (m,n) in zip(SECTION_A, SECTION_D):
        a[n] = a[m]
    _event_storage.update({a[1]+a[2]: a})
    return a


def process_4003(a):
    _event_storage.update({a[1]+a[2]: a})
    return a


def propagate(a):
    propagated_line = a
    if a[1]+a[2] in _event_storage:
        stored_event = _event_storage[a[1]+a[2]]
        for i in SECTION_D:
            propagated_line[i] = stored_event[i]
    return propagated_line
        

        
def process_line(b):
    array_line = b.split('|')
    processed_line = ''
    if is_mc(array_line):
        processed_line = process_and_store_mc(array_line)
    elif is_4003(array_line):
        processed_line = process_4003(array_line)
    else:
        processed_line = propagate(array_line)
    return '|'.join(processed_line)

    
def clear_output():
    try:
        os.remove(OUTPUT)
        print('File Removed!')
    except:
        print('Cannot remove file, perhaps it does not exist')
 
 
 
_event_storage = dict()
clear_output()
with open(INPUT, 'r') as i, \
     open(OUTPUT, 'a') as o:
    for line in tqdm(i):
        o.write(process_line(line))