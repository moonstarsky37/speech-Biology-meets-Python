import pandas as pd
import numpy as np
import multiprocessing
import os, threading, time, sys
import datetime, timeit
thread_count = int(os.cpu_count()-2)
process_id = int(sys.argv[1])
process_count = int(sys.argv[2])

class ProcessNullThread (threading.Thread):
    def __init__(self, nulllist, user_log, user_log_lock, remove_list, remove_list_lock, print_lock):
        threading.Thread.__init__(self)
        self.user_log = user_log
        self.user_log_lock = user_log_lock
        self.remove_list = remove_list
        self.remove_list_lock = remove_list_lock
        self.nulllist = nulllist
        self.print_lock = print_lock
    def run(self):
        viewJobs = self.user_log.query("action==\"viewJob\"").drop_duplicates(subset=["uid", "invoice"])
        co_run = 64
        nulllist = []
        for i in range(int(len(self.nulllist)/co_run)):
            tmp = []
            for j in range(co_run):
                tmp.append(self.nulllist[i*co_run + j])
            nulllist.append(tmp)
        if not len(self.nulllist)%4==0:
            nulllist.append(self.nulllist[-1*(len(self.nulllist)%co_run):])
        for k in nulllist:
            tmp_uid = [int(self.user_log[i:i+1]['uid']) for i in k]
            tmp_invoice = [int(self.user_log[i:i+1]['invoice']) for i in k]
            checkJobNos = []
            for i in range(len(k)):
                tmp = viewJobs[(viewJobs['uid'] == tmp_uid[i]) & (viewJobs['invoice'] == tmp_invoice[i])].head(1)
                if len(tmp) > 0:
                    checkJobNos.append([tmp['jobNo']])
                else:
                    checkJobNos.append([])
            for i in range(len(k)):
                if len(checkJobNos[i]) == 0:
                    self.remove_list_lock.acquire()
                    self.remove_list.append(k[i])
                    self.remove_list_lock.release()
                else:
                    fillJobNo = float(checkJobNos[i][0])
                    self.user_log_lock.acquire()
                    self.user_log.set_value(k[i],'jobNo', fillJobNo)
                    self.user_log_lock.release()

i = 100000 * (process_id-1)
j = process_id
while True:
    try:
        user_log = pd.read_csv('./test.csv',sep='|',skiprows=i,nrows=100000)
    except:
        print(i)
        break
    print("Log File Load Completed, i={}".format(i))
    user_log.columns = ['uid','action','jobNo','invoice','dateTime','source','url','deviceType']
    user_log.dropna(subset=['uid','action','invoice'],inplace=True)
    nulllist = user_log.loc[user_log['jobNo'].isnull()].index.values.tolist()
    remove_list = []
    user_log_lock = threading.Lock()
    remove_list_lock = threading.Lock()
    print_lock = threading.Lock()
    pool = []
    print_lock.acquire()
    for s in range(thread_count):
        start = int(s*len(nulllist)/thread_count)
        end = int((s+1)*len(nulllist)/thread_count if s+1 <= thread_count else len(nulllist))
        pool.append(ProcessNullThread(nulllist[start:end], user_log, user_log_lock, remove_list, remove_list_lock, print_lock))
        end=""
        print("New Thread - start: {}, end: {}\n".format(start, end))

    print_lock.release()
    start_time = timeit.default_timer()
    for process in pool:
        process.start()
    time.sleep( 1 )
    for process in pool:
        process.join()
    end_time = timeit.default_timer()
    print("run time: {}".format(end_time - start_time))
    user_log = user_log.drop(user_log.index[remove_list])

    new_user_log = pd.get_dummies(user_log[['uid','action','jobNo','invoice']],prefix=['action'])
    new_user_log = new_user_log.groupby(['uid','jobNo','invoice'],as_index=False).sum().reset_index(drop=True)
    new_user_log['jobNo'] = np.round(new_user_log['jobNo']).astype(np.int32)
    new_user_log.to_csv('./test'+str(j)+'.csv',index = False)
    j+=process_count
    i+=(100000*process_count)
