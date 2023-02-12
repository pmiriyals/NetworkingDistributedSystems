
import sys
import pickle
import hashlib
import threading
import socket
import time
import ast
from datetime import datetime

TIME_FORMAT = '%H:%M:%S.%f'
NODE_NAME_FORMAT = '{}:{}'
M = 3  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2**M
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43545  # for testing use port numbers on localhost at TEST_BASE+n
SLEEP_TIME_IN_SECS = 5
BATCH_SIZE = 10

"""
def generate_hash(str):
    result = hashlib.md5(str.encode())
    x = int(result.hexdigest(), 16)
    return x
    
def generate_hash(str):
    sha1 = hashlib.sha1()
    sha1.update(str.encode('utf-8'))
    result = sha1.hexdigest()
    return int(result, 16)
"""
def generate_hash(str):
    sha1 = hashlib.sha1()
    sha1.update(str.encode('utf-8'))
    result = sha1.hexdigest()
    return int(result, 16)

def in_range(id, start, end):
	start = start % NODES
	end = end % NODES
	id = id % NODES
	if start < end:
		return start <= id and id < end
	return start <= id or id < end

class Address:
    def __init__(self, endpoint, port):
        self.endpoint = endpoint
        self.port = int(port)
        self.hash_val = generate_hash(NODE_NAME_FORMAT.format(self.endpoint, self.port))

    def __str__(self):
        return '{}:{}'.format(self.endpoint, self.port)

    def get_hash(self):
        return self.hash_val

def connection(func):
    def inner(self, *args, **kwargs):
        self._mutex.acquire()
        self.create_connection()
        ret = func(self, *args, **kwargs)
        self.close_connection()
        self._mutex.release()
        return ret
    return inner

class RemoteNode(object):
    def __init__(self, remote_addr=None):
        self.my_address = remote_addr
        self._mutex = threading.Lock()

    def __str__(self):
        return 'Remote {}'.format(self.my_address)

    def create_connection(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.my_address.endpoint, self.my_address.port))

    def close_connection(self):
        self.sock.close()
        self.sock = None

    def get_id(self, offset = 0):
        return (self.my_address.get_hash() + offset) % NODES

    def send_message(self, message):
        self.sock.sendall(pickle.dumps(message))

    def recv_message(self):
        raw_data = self.sock.recv(BUF_SZ)
        return pickle.loads(raw_data)

    @connection
    def get_remote_node(self, message):
        self.send_message(message)
        response = self.recv_message()
        addr = Address(response[0], response[1])

        return RemoteNode(addr)

    def find_successor(self, id):
        return self.get_remote_node('find_successor {}'.format(id))

    def successor(self):
        return self.get_remote_node('successor')

    def predecessor(self):
        return self.get_remote_node('get_predecessor')

    def closest_preceding_node(self, id):
        return self.get_remote_node('closest_preceding_node {}'.format(id))

    def ping(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.my_address.endpoint, self.my_address.port))
            s.sendall(pickle.dumps('ping'))
            s.close()
            return True
        except socket.error:
            return False

    @connection
    def notify(self, node):
        self.send_message('notify {} {}'.format(node.my_address.endpoint, node.my_address.port))

    @connection
    def look_up_key(self, key):
        self.send_message('final_look_up_key {}'.format(key))

        return self.recv_message()

    @connection
    def insert_key_value(self, key, value):
        self.send_message('final_insert_key_val {} {}'.format(key, value))

        return self.recv_message()

"""
Takes a port number of an existing node (or 0 to indicate it should start a new network).
This program joins a new node into the network using a system-assigned port number for itself.
The node joins and then listens for incoming connections (other nodes or queriers).
You can use blocking TCP for this and pickle for the marshaling.
"""
class ChordNode(object):
    def __init__(self, my_address, remote_node_address=None):
        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener_socket.bind(('localhost', 0))  
        self.listener_socket.listen(BACKLOG)
        self.my_address = Address('localhost', int(self.listener_socket.getsockname()[1]))
        #self.my_address = my_address
        self.n_id = self.my_address.get_hash() % NODES
        self.threads = {}
        self.finger = {}
        for i in range(1, NODES+1):
            self.finger[i] = None
        self.predecessor_node = None
        self.kv_store = {}
        print('n_id = {} for endpoint = {} joining network using remote node with endpoint = {}'.format(self.n_id, self.my_address, remote_node_address))
        self.join(remote_node_address)

    def get_id(self, offset = 0):
        return (self.my_address.get_hash() + offset) % NODES

    def join(self, remote_node_address=None):
        if remote_node_address:
            remote_node = RemoteNode(remote_node_address)
            self.finger[1] = remote_node.find_successor(self.get_id())
            print('Successor node upon joining the network = {}'.format(self.finger[1]))
        else:
            self.finger[1] = self

    def put_key_value(self, key, value):
        self.kv_store[key] = value
        
    def get_key_hash(self, key):
        return generate_hash(key) % NODES

    def get_key(self, key):
        if key in self.kv_store:
            return self.kv_store[key]
        else:
            return '-1'

    def __str__(self):
        return 'node id = {}, endpoint = {}'.format(self.n_id, self.my_address.endpoint + ':' + str(self.my_address.port))

    def stabilize(self):
        while True:
            if self.predecessor() != None:
                print('In stabilize: n_id = {}, predecessor = ({})'.format(self.n_id, self.predecessor().__str__()))

            if self.successor() != None:
                print('In stabilize: n_id = {}, successor = ({})'.format(self.n_id, self.successor().__str__()))

            succ = self.successor()

            if succ == self and self.predecessor() != None:
                self.finger[1] = self.predecessor()
            else:
                node = succ.predecessor()
                if node != None and in_range(node.get_id(), self.get_id(), succ.get_id()) and (self.get_id() != succ.get_id()) and (node.get_id() != self.get_id()) and (node.get_id() != succ.get_id()):
                    self.finger[1] = node
            self.successor().notify(self)
            time.sleep(SLEEP_TIME_IN_SECS)

    def successor(self):
        return self.finger[1]

    def notify(self, remote):
        if (self.predecessor() == None or self.predecessor() == self) or (((in_range(remote.get_id(), self.predecessor().get_id(), self.get_id())) and (self.predecessor().get_id() != self.get_id()) and (remote.get_id() != self.predecessor().get_id()) and (remote.get_id() != self.get_id()))):
            self.predecessor_node = remote

            for key in self.kv_store.keys():
                if self.get_key_hash(key) <= remote.get_id():
                    remote.insert_key_value(key, self.kv_store[key])

    def insert_key_value(self, key, value):
        print('INSERT key: {}'.format(key))
        self.put_key_value(key, value)

    def predecessor(self):
        return self.predecessor_node

    def fix_fingers(self):
        index = 1
        while True:
            index = index + 1
            if index > M:
                index = 1
            self.finger[index-1] = self.find_successor(self.get_id(1 << (index-1)))
            time.sleep(SLEEP_TIME_IN_SECS)

    def pr_finger_table(self):
        for index in range(1, M+1):
            if self.finger[index] != None:
                print('Node ID = {} with Finger Entry[{}]: remote node id = {} and remote node address = {}'.format(self.get_id(), index, self.finger[index].get_id(), self.finger[index].my_address))
            else:
                print('Node ID = {} with Finger Entry[{}]: None'.format(self.get_id(), index))

    def check_predecessor(self):
        while True:
            #print('Check predecessor')
            if self.predecessor() != None:
                if self.predecessor().my_address.get_hash() != self.my_address.get_hash():
                    if self.predecessor().ping() == False:
                        print('Predecessor ping returned False')
                        self.predecessor_node = None
            time.sleep(SLEEP_TIME_IN_SECS)
        
    def process_dictionary(self, dict):
        count = 0
        for key in dict:
            value = dict[key]
            hash_key = self.get_key_hash(key)
            node = self.find_successor(hash_key)
            print('Target node address: {} and target node id = {}'.format(node.my_address, node.get_id()))
            if node.get_id() == self.get_id():
                self.insert_key_value(key, value)
            else:
                print('inserting in remote node')
                node.insert_key_value(key, value)
            count = count + 1
            if count % BATCH_SIZE == 0:
                time.sleep(1)
        
    def run(self):

        while True:
            try:
                sock, addr = self.listener_socket.accept()
            except socket.error:
                print("Listener socket accept error")

            raw_data = bytearray()
            while True:
                data = sock.recv(BUF_SZ)
                if not data: break
                raw_data.extend(data)
                if len(data) < BUF_SZ: break
  
            request_received = pickle.loads(raw_data)
            if request_received:
                req = request_received.split()
                cmd = req[0]
                print('cmd = {}'.format(cmd))
                remaining_req = request_received[len(cmd)+1:]
                resp = ''
                #print('Received command = {} and remaining req = {}'.format(cmd, remaining_req))

                if cmd == 'dictionary':
                    dict = ast.literal_eval(remaining_req)
                    upload_dict_thread = threading.Thread(target=self.process_dictionary, args=[dict])
                    upload_dict_thread.start()
                    #self.process_dictionary(dict)
                    resp = 'UPLOADED'

                if cmd == 'insert_key_val':
                    key = req[1]
                    value = ''.join(req[2:])
                    hash_key = self.get_key_hash(key)
                    node = self.find_successor(hash_key)
                    #print('Target node address: {} and target node id = {}'.format(node.my_address, node.get_id()))
                    if node.get_id() == self.get_id():
                        self.insert_key_value(key, value)
                    else:
                        node.insert_key_value(key, value)
                    resp = 'INSERTED'

                if cmd == 'final_insert_key_val':
                    key = req[1]
                    value = ''.join(req[2:])
                    self.insert_key_value(key, value)
                    resp = 'INSERTED'

                if cmd == 'look_up_key':
                    key = req[1]
                    hash_key = self.get_key_hash(key)
                    print('lookup hash key = {}'.format(hash_key))
                    node = self.find_successor(hash_key)
                    print('Target node address: {} and target node id = {}'.format(node.my_address, node.get_id()))
                    if node.get_id() == self.get_id():
                        resp = self.look_up_key(key)
                    else:
                        resp = node.look_up_key(key)

                if cmd == 'final_look_up_key':
                    key = req[1]
                    resp = self.look_up_key(key)

                if cmd == 'get_finger_table':
                    self.pr_finger_table()
                    resp = 'Finger table printed'

                if cmd == 'successor':
                    succ = self.successor()
                    resp = pickle.dumps((succ.my_address.endpoint, succ.my_address.port))

                if cmd == 'get_predecessor':
                    if self.predecessor_node != None:
                        pred = self.predecessor()
                        resp = (pred.my_address.endpoint, pred.my_address.port)

                if cmd == 'find_successor':
                    succ = self.find_successor(int(remaining_req))
                    resp = (succ.my_address.endpoint, succ.my_address.port)

                if cmd == 'closest_preceding_node':
                    closest = self.closes_preceding_node(int(remaining_req))
                    resp = (closest.my_address.endpoint, closest.my_address.port)

                if cmd == 'notify':
                    npredecessor = Address(remaining_req.split(' ')[0], int(remaining_req.split(' ')[1]))
                    self.notify(RemoteNode(npredecessor))
                    
                sock.sendall(pickle.dumps(resp))

    def pr_now(self):
        return datetime.now().strftime(TIME_FORMAT)

    def find_successor(self, id):
        """ Ask this node to find id's successor = successor(predecessor(id))"""
        #print('find_successor called by node_id = {} for key: {} at timestamp = {}'.format(self.n_id, str(id), self.pr_now()))

        if (in_range(id, self.get_id(), self.successor().get_id()) and (self.n_id != self.successor().get_id()) and (id != self.n_id)):
            return self.successor()
        else:
            remote = self.closest_preceding_node(id)
            if self.my_address.get_hash() != remote.my_address.get_hash():
                return remote.find_successor(id)
            else:
                #print('returning self')
                return self

    def closest_preceding_node(self, id):
        for index in range(M+1, 0, -1):
            if (self.finger[index] != None and in_range(self.finger[index].get_id(), self.get_id(), id) and self.get_id != id and self.finger[index].get_id() != self.get_id() and self.finger[index].get_id() != id):
                return self.finger[index]
        return self

    def look_up_key(self, key):
        print("Look up for key = {}".format(key))
        val = self.get_key(key)
        if (val != '-1'):
            print('Key found')
        else:
            print('Key does not exist')
        return val

    def inesrt_key_value(self, key, value):
        print('Insert key = {} and value = {}'.format(key, value))
        self.put_key_value(key, value)

    def start(self):
        self.threads['run'] = threading.Thread(target=self.run)
        self.threads['fix_fingers'] = threading.Thread(target=self.fix_fingers)
        self.threads['stabilize'] = threading.Thread(target=self.stabilize)
        self.threads['check_predecessor'] = threading.Thread(target=self.check_predecessor)
        for key in self.threads:
            self.threads[key].start()

        print('started all threads successfully')
    
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python chord_node.py port_number\nEnter port_number as 0 to start a new network")
        exit(1)
    #my_addr = Address('localhost', TEST_BASE+5)
    remote_addr = None
    if int(sys.argv[1]) != 0:
        remote_addr = Address('localhost', int(sys.argv[1]))
    cn = ChordNode(remote_addr)
    cn.start()
    
