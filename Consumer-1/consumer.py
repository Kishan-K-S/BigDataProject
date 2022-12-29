import sys
import os 
import json 
import datetime
import time



if(len(sys.argv)==2 and sys.argv[1]=='--from-beginning'):
    n =int(input("Enter the number of topics"))

    entry =[]

    for i in range(0,n):
        data = input("Enter Topic Name:")
        entry.append(data)
    entry3=[]

    filenameno = "./consumer_first.json"
    with open(filenameno,'w') as file:
        json.dump(entry3,file)
    while(True):
        entry4=[]
        for i in entry:
            entry2=[]
            path = "C:\\Users\\sbana\\OneDrive\\Desktop\\BIG-DATA_PROJ\\Broker1\\data\\{}".format(i)
            if(os.path.exists(path)):
                filename = "C:\\Users\\sbana\\OneDrive\\Desktop\\BIG-DATA_PROJ\\Broker1\\data\\{}\\partition1\\partition1.json".format(i)
                filename1="C:\\Users\\sbana\\OneDrive\\Desktop\\BIG-DATA_PROJ\\Broker1\\data\\{}\\partition2\\partition2.json".format(i)
                filename2 ="C:\\Users\\sbana\\OneDrive\\Desktop\\BIG-DATA_PROJ\\Broker1\\data\\{}\\partition3\\partition3.json".format(i)
                with open(filename,'r') as file:
                    data = json.load(file)
                with open(filename1,'r') as file1:
                    data2= json.load(file1)
                with open(filename2,'r') as file2:
                    data3= json.load(file2)
                entry2.append(data)
                entry2.append(data2)
                entry2.append(data3)
                time.sleep(4)
                entry4.append(entry2)
                # with open(filenameno,'w') as file:
                #     json.dump(entry2,file)
        time.sleep(4)
        with open(filenameno,'w') as file:
            json.dump(entry4,file)

else:
    previous_time = datetime.datetime(2022,11,29)
    current_time = datetime.datetime.now()
    diff = int((current_time-previous_time).seconds)
    n =int(input("Enter the number of topics"))

    entry =[]

    for i in range(0,n):
        data = input("Enter Topic Name:")
        entry.append(data)
    entry3=[]

    filenameno = "./consumer_imm.json"
    with open(filenameno,'w') as file:
        json.dump(entry3,file)
    while(True):
        entry4=[]
        for i in entry:
            entry2=[]
            path = "C:\\Users\\sbana\\OneDrive\\Desktop\\BIG-DATA_PROJ\\Broker1\\data\\{}".format(i)
            if(os.path.exists(path)):
                filename = "C:\\Users\\sbana\\OneDrive\\Desktop\\BIG-DATA_PROJ\\Broker1\\data\\{}\\partition1\\partition1.json".format(i)
                filename1="C:\\Users\\sbana\\OneDrive\\Desktop\\BIG-DATA_PROJ\\Broker1\\data\\{}\\partition2\\partition2.json".format(i)
                filename2 ="C:\\Users\\sbana\\OneDrive\\Desktop\\BIG-DATA_PROJ\\Broker1\\data\\{}\\partition3\\partition3.json".format(i)
                with open(filename,'r') as file:
                    data = json.load(file)
                    final=[]
                    for i in range(0,len(data)):
                    
                        for k in data[i]:
                            if(int(k)>diff):
                                final.append(data[i])  
                                        
                with open(filename1,'r') as file1:
                    data2= json.load(file1)
                    final1=[]
                    for i in range(0,len(data2)):
                    
                        for k in data2[i]:
                        
                            if(int(k)>diff):
                                final1.append(data2[i])  
                            
                    
                with open(filename2,'r') as file2:
                    data3= json.load(file2)
                    
                    final2=[]
                    for i in range(0,len(data3)):
                    
                        for k in data3[i]:
                            if(int(k)>diff):
                                final2.append(data3[i])    
                                        
                entry2.append(final)
                entry2.append(final1)
                entry2.append(final2)
                time.sleep(4)
                entry4.append(entry2)
                # with open(filenameno,'w') as file:
                #     json.dump(entry2,file)
        time.sleep(4)
        with open(filenameno,'w') as file:
            json.dump(entry4,file)