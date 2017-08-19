import pandas as pd
import numpy as np
import timeit
start = timeit.default_timer()
i = 0
j = 1
while True:
    try:
        user_log = pd.read_csv('./test.csv',sep='|',skiprows=i,nrows=100000)
    except:
        break
    print("Log File Load Completed, i={}".format(i))
    start_time = timeit.default_timer()
    print("No multi-Thread - start: ")
    user_log.columns = ['uid','action','jobNo','invoice','dateTime','source','url','deviceType']
    user_log.dropna(subset=['uid','action','invoice'],inplace=True)
    nulllist = user_log.loc[user_log['jobNo'].isnull()].index.values.tolist()
    remove_list = []
    for _i in nulllist:
        tmp_uid = int(user_log[_i:_i+1]['uid'])
        tmp_invoice = int(user_log[_i:_i+1]['invoice'])
        checkJobNo = user_log.loc[user_log['action'].isin(['viewJob']) &
             user_log['invoice'].isin([tmp_invoice]) &
             user_log['uid'].isin([tmp_uid])]\
            .drop_duplicates(subset=['uid','action','invoice'])['jobNo'].head(1)
        if len(checkJobNo) == 0:
            remove_list.append(_i)
        else:
            fillJobNo = float(checkJobNo)
            user_log.set_value(_i,'jobNo', fillJobNo)
    user_log = user_log.drop(user_log.index[remove_list])
    new_user_log = pd.get_dummies(user_log[['uid','action','jobNo','invoice']],prefix=['action'])
    new_user_log = new_user_log.groupby(['uid','jobNo'],as_index=False).sum().reset_index(drop=True)
    new_user_log['jobNo'] = np.round(new_user_log['jobNo']).astype(np.int32)
    new_user_log.to_csv('./test_no_multy'+str(j)+'.csv',index = False)    
    j+=1
    i+=100000
end_time = timeit.default_timer()
print("run time: {}".format(end_time - start_time))
