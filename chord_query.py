
import sys
import pickle
import socket

class ChordQuery(object):
    def __init__(self, port_of_existing_node, key):
        self.port_of_existing_node = port_of_existing_node
        self.key = key
        self.query()

    def query(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(('localhost', self.port_of_existing_node))
            sock.sendall(pickle.dumps('look_up_key {}'.format(self.key)))
            print('Received response = {}'.format(pickle.loads(sock.recv(4096))))
                  
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python chord_query.py port_number_of_existing_node key")
        exit(1)

    cq = ChordQuery(int(sys.argv[1]), str(sys.argv[2]))

    
