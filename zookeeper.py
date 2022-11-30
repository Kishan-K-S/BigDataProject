import socket



def check_if_open(port_no):
    sock =socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    result=sock.connect_ex((socket.gethostbyname(socket.gethostname()),port_no))
    sock.close()
    return result==0


