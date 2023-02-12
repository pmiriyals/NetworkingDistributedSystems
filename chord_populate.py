
import sys
import pickle
import socket
import csv
import time

DELIMITER = ':'

class ChordPopulate(object):
    def __init__(self, port_of_existing_node, file_name):
        self.port_of_existing_node = port_of_existing_node
        self.file_name = file_name
        self.populate()

    def generate_hash(str):
        sha1 = hashlib.sha1()
        sha1.update(str.encode('utf-8'))
        result = sha1.hexdigest()
        return int(result, 16)

    def populate(self):
        print('Retrieving file {} and sending KV pairs to node listening at port = {}'.format(self.file_name, self.port_of_existing_node))
        count = 1
        dict = {}
        with open(self.file_name, newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                count = count + 1
                dict['{}:{}'.format(row['Player Id'], row['Year'])] = row
            print('total size of dictionary being sent = {}'.format(len(str(dict))))
            self.send_dict(dict)

    def send_dict(self, dict):
        print('Sending dictionary...')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(('localhost', self.port_of_existing_node))
            sock.sendall(pickle.dumps('dictionary {}'.format(dict)))
            #print('Received response = {}'.format(pickle.loads(sock.recv(4096))))
        

    def insert_key_val(self, key, val):
        print('Inserting key = {} and val = {}'.format(key, val))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(('localhost', self.port_of_existing_node))
            sock.sendall(pickle.dumps('insert_key_val {} {}'.format(key, val)))
            print('Received response = {}'.format(pickle.loads(sock.recv(4096))))
                  
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python chord_populate.py port_number_of_existing_node file_name")
        exit(1)

    cp = ChordPopulate(int(sys.argv[1]), str(sys.argv[2]))
