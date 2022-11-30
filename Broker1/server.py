import socket 
import threading
import os 
import json
import datetime
from zookeeper import *
import shutil





def return_path(leader_number,path = "C:\\Users\\sbana\\OneDrive\\Desktop\\BIG-DATA_PROJ"):
    path=path+"\\Broker{}\\data".format(leader_number)
    return path


entry3 = []
HEADER = 64
PORT = 5051
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
previous_time = datetime.datetime(2022,11,29)
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

ports=[5051,5052,5053]

switch=1
xPort=1
for i in range(len(ports)):
    if(ports[i]!=PORT and check_if_open(ports[i])):
        switch=0
        xPort=i+1
        break
if(switch==1):
    print("Only node running")
else:
    print("Replicating data...")
    shutil.copytree(return_path(xPort), return_path(PORT-5050),dirs_exist_ok=True)
    print("Replication Completed")




def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")

    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == DISCONNECT_MESSAGE:
                connected = False

            print(f"[{addr}] {msg}")
            key,value = msg.split(':')
            print(key)
            print(value)
            path = "./data/{}".format(key)
            isExist = os.path.exists(path)
            if(isExist==False):
                os.mkdir(path)
                path1 = "./data/{}/partition1".format(key)
                path2 = "./data/{}/partition2".format(key)
                path3 = "./data/{}/partition3".format(key)
                
                os.mkdir(path1)
                os.mkdir(path2)
                os.mkdir(path3)
                filename = './data/{}/partition1/partition1.json'.format(key)
                filename2 ='./data/{}/partition2/partition2.json'.format(key)
                filename3 ='./data/{}/partition3/partition3.json'.format(key)
                with open(filename,'w') as file:
                     json.dump(entry3,file)
                with open(filename2,'w') as file:
                     json.dump(entry3,file)   
                with open(filename3,'w') as file:
                     json.dump(entry3,file)    
            filename = './data/{}/partition1/partition1.json'.format(key)
            filename2 ='./data/{}/partition2/partition2.json'.format(key)
            filename3 ='./data/{}/partition3/partition3.json'.format(key)  
            current_time = datetime.datetime.now()

            entry = {int((current_time-previous_time).seconds):str(value)}
            if(msg_length%3==0): 
             
             with open(filename,'r') as file:
                 data = json.load(file)
   
             data.append(entry)

             with open(filename,"w") as file:
                json.dump(data,file)
            if(msg_length%3==1): 
             data=[]
             with open(filename2,'r') as file:
                 data = json.load(file)
   
             data.append(entry)

             with open(filename2,"w") as file:
                json.dump(data,file)
            if(msg_length%3==2): 
             
             with open(filename3,'r') as file:
                 data = json.load(file)
   
             data.append(entry)

             with open(filename3,"w") as file:
                json.dump(data,file)
            
            

            
            conn.send("Msg received".encode(FORMAT))

    conn.close()
    print('hello')
        

def start():
    server.listen()
    print(f"[LISTENING] Server is listening on {SERVER}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")


print("[STARTING] server is starting...")
start()