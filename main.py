import socket 
import threading
import os 
import json
import datetime
from zookeeper import *

entry3 = []
HEADER = 64
PORT = 5059
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)

FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
previous_time = datetime.datetime(2022,11,29)
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)
ports=[5051,5052,5053]
req_port=ports[0]
server.listen()
print(f"[LISTENING] Server is listening on {SERVER}")



def start():
    server.listen()
   
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_request, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")






def handle_request(conn,addr):
    while True:
        HEADER_message = conn.recv(HEADER)
        msg_len = HEADER_message.decode(FORMAT)
        msg = conn.recv(int(msg_len))

        
        try:
            client.send(HEADER_message)
            client.send(msg)
        except:
            key=1
            for i in range(len(ports)):
                if(check_if_open(ports[i])):
                    key=0
                    req_port=ports[i]
                    break
            if key==1:
                print("all nodes are down")
                conn.close()
                break
            else:
                try:
                    client.close()
                    
                except:
                    pass
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect((SERVER,req_port))
                client.send(HEADER_message)
                client.send(msg)
        conn.send("Msg received".encode(FORMAT))

start()