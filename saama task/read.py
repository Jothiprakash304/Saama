import pandas as pd
from multiprocessing import Process,Lock


def func(A1,l):
    a=A1
    if a is not None:
        with l:
            a['Domain']=['ECG']*472
            a['EGCAT']=['Measurent']*472
            a['EGDTC']=a['Date']
            a['EGDY']=a['Rel day of ECG to Start of Trt']
            a['EGEVAL']=a['Evaluation Method']
            a['EGMETHOD']=['12 LEAD STANDARD']*472
            a['EGORRES']=a['Rel day of ECG to End of Trt']
            a['EGPOS']=a['Position of Subject During ECG']
            a['EGTESTCD']=a['Ventricular Rate']
            a['STUDYID']=a['Clinical Trial Number']
            a['USUBJID']=a['Subject Number'].astype(str)+'||'+a['Clinical Trial Number'].astype(str)
            a['VISIT']=a['Visit']
            a['VISITDY']=['0']*472
            a['VISITNUM']=['0']*472
        
    new=a[['Domain','EGCAT','EGDTC','EGDY','EGEVAL','EGMETHOD','EGORRES','EGPOS','EGTESTCD','STUDYID','USUBJID','VISIT','VISITDY','VISITNUM']]
    return new.to_csv('output/new.csv',index=False) 

def func_2(A2,l):
    a=A2
    if a is not None:
        with l:
            a['Domain']=['ECG']*472
            a['EGCAT']=['Measurent']*472
            a['EGDTC']=pd.to_datetime(a[['Day of ECG', 'Month of ECG', 'Year of ECG']].astype(str).apply('-'.join,axis=1), format='%d-%m-%Y')
            a['EGDY']=a['Rel day of ECG to Start of Trt']
            a['EGEVAL']=a['Evaluation Method']
            a['EGMETHOD']=['12 LEAD STANDARD']*472
            a['EGORRES']=a['Rel day of ECG to End of Trt']
            a['EGPOS']=a['Position of Subject During ECG']
            a['EGTESTCD']=a['QT Interval']
            a['STUDYID']=a['Clinical Trial Number']
            a['USUBJID']=a['Subject Number'].astype(str)+'||'+a['Clinical Trial Number'].astype(str)
            a['VISIT']=a['Visit']
            a['VISITDY']=['0']*472
            a['VISITNUM']=['0']*472

    new=a[['Domain','EGCAT','EGDTC','EGDY','EGEVAL','EGMETHOD','EGORRES','EGPOS','EGTESTCD','STUDYID','USUBJID','VISIT','VISITDY','VISITNUM']]
    return new.to_csv('output/new1.csv',index=False) 
def func_3(A3,l):
    a=A3
    if a is not None:
        with l:
            a['Domain']=['ECG']*472
            a['EGCAT']=['Measurent']*472
            a['EGDTC']=pd.to_datetime(a[['Day of ECG', 'Month of ECG', 'Year of ECG']].astype(str).apply('-'.join,axis=1), format='%d-%m-%Y')
            a['EGDY']=a['Rel day of ECG to Start of Trt']
            a['EGEVAL']=a['Evaluation Method']
            a['EGMETHOD']=['12 LEAD STANDARD']*472
            a['EGORRES']=a['Rel day of ECG to End of Trt']
            a['EGPOS']=a['Position of Subject During ECG']
            a['EGTESTCD']=a['QTc Interval Calc Bazett']
            a['STUDYID']=a['Clinical Trial Number']
            a['USUBJID']=a['Subject Number'].astype(str)+'||'+a['Clinical Trial Number'].astype(str)
            a['VISIT']=a['Visit']
            a['VISITDY']=['0']*472
            a['VISITNUM']=['0']*472
    new=a[['Domain','EGCAT','EGDTC','EGDY','EGEVAL','EGMETHOD','EGORRES','EGPOS','EGTESTCD','STUDYID','USUBJID','VISIT','VISITDY','VISITNUM']]
    return new.to_csv('output/new2.csv',index=False)
def func_4(A1,A2,A3):
    a1=A1
    a2=A2
    a3=A3
    combined=pd.concat([a1,a2,a3],ignore_index=True)
    combined.to_csv('output/output_data.csv',index=False)

if __name__=='__main__':
    A1=pd.read_csv('input_data/ecg1.csv')
    A2=pd.read_csv('input_data/ecg2.csv')
    A3=pd.read_csv('input_data/ecg3.csv')
    l=Lock()
    
    p1=Process(target=func,args=(A1,l))
    p2=Process(target=func_2,args=(A2,l))
    p3=Process(target=func_3,args=(A3,l))


    p1.start()
    p2.start()
    p3.start()
    p1.join()
    p2.join()
    p3.join()

    a1=pd.read_csv('output/new.csv')
    a2=pd.read_csv('output/new1.csv')
    a3=pd.read_csv('output/new2.csv')

    p4=Process(target=func_4,args=(a1,a2,a3,))
    p4.start()
    p4.join()




        






