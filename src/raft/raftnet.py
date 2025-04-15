# raftnet.py
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
import message
import raftconfig
import threading

class RaftNet:
    def __init__(self, nodenum:int):
        # My own address
        self.nodenum = nodenum
        print("Started raftnet", self.nodenum, "with address", raftconfig.SERVERS[self.nodenum])

    
    def send(self, destination: int, message:bytes):
        #
        sock = socket(AF_INET, SOCK_STREAM)

        try:
            sock.connect(raftconfig.SERVERS[destination])
            message.send_message(sock, message)
        except Exception as e:
            print('An error occured when sending message', message, 'to', destination, ":", e)
        finally:
            sock.close()
    
    # receive and return any message that was sent to me
    def receive(self) -> bytes:
        sock = socket(AF_INET, SOCK_STREAM)
        
        try:
            sock.bind(raftconfig.SERVERS[self.nodenum])
            sock.listen()

            client, addr = sock.accept()
            msg = message.recv_message(client)

            return msg.decode('utf-8')
        except Exception as e:
            print('An error occured while receiving message on address', raftconfig.SERVERS[self.nodenum], ":", e)
        finally:
            sock.close()

class RaftNetActor:
    def __init__(self, nodenum: int):
        # My own address
        self.nodenum = nodenum
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        self.sock.bind(raftconfig.SERVERS[nodenum])
        self.sock.listen()
        print("Started actor", self.nodenum, "with address", raftconfig.SERVERS[self.nodenum])
    
    # Servers must be able to send a message to another server
    def send(self, destination: int, msg: bytes):

        sock = socket(AF_INET, SOCK_STREAM)
        try:
            sock.connect(raftconfig.SERVERS[destination])
            # We send a message...
            message.send_message(sock, msg) # This is from Project 1/Warmup
            # .. and then we just forget about it
        except IOError as e:
            print(e)
        finally:
            sock.close()
    
    # A server must be able to respond to a message received from another
    # servere
    def receive(self) -> bytes:
        client, addr = (None,None)
        try:
            client, addr = self.sock.accept()
            msg = message.recv_message(client)
            return msg
        except IOError as e:
            print(e)
        finally:
            client.close()


def console(nodenum):
    net = RaftNet(nodenum)

    def receiver():
        while True:
            msg = net.receive()
            print('Received:', msg)
    threading.Thread(target=receiver).start()
    
    while True:
        cmd = input(f"Node {nodenum} > ")
        dest, message = cmd.split()
        net.send(int(dest), message.encode('utf-8'))

if __name__ == '__main__':
    import sys
    console(int(sys.argv[1]))
