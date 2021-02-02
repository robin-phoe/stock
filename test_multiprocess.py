from multiprocessing import Pool
import os, time, random
from test_multiprocess_fun import long_time_task

def long_time_task(name):
    #print('flag')
    print('Run task %s (%s)...' % (name, os.getpid()))
    start = time.time()
    time.sleep(random.random() * 3)
    end = time.time()
    print('Task %s runs %0.2f seconds.' % (name, (end - start)))

if __name__=='__main__':
    print('Parent process %s.' % os.getpid())
    p = Pool(20)
    for i in range(20):
        p.apply_async(long_time_task, (i,))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
