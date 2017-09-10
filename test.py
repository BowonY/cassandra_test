# client program
import sys
import urllib.request
import json
import time
import multiprocessing

base_url = 'http://13.59.55.140:8080/helloworld-rs/calculator'
existing = []
NEW = 'new'
OLD = 'old'
funcs = ['add', 'subtract', 'multiply', 'divide']

def reset():
    url = '%s/reset' % base_url
    urllib.request.urlopen(url=uri).read()

def test(log_file, func, start, end):
    fp = open(log_file, 'w')
    
    j = 0
    for i in range(start,end):
        if i%10 == 0:
            j+=1
            request(fp, func, j, j, NEW) # new integers
            existing.append((j,j))
            print(j,j, NEW)
        else:
            request(fp, func, j, j, OLD) 
            print(j,j, OLD)

def request(fp, func, val1, val2, label):
    url = '%s/%s/%f/%f' % (base_url, func, val1, val2)
    print(url)
    start = time.time()
    response = urllib.request.urlopen(url=url).read()
    end = time.time()
    msg = 'sec:%.2f,\tval1:%f\tval2:%f\t%s\n' \
        % (end-start, val1, val2, label)
    fp.write(msg)

if __name__ == '__main__':
    timestamp = time.time()
    fn = ['./log/log_%d_%s.txt' % (timestamp, func) for func in funcs]
    processes = [multiprocessing.Process(target=test,
            args=(fn[i], funcs[i], 0, 10))
            for i in range(len(funcs))]

    for p in processes:
        p.start()
    
    for p in processes:
        p.join()
