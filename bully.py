"""
CPSC 5520, Seattle University
Lab 2: Bully
Author: Sai Lavanya Kanakam

Commented out PROBE message code. Was not able to fully validate the solution.

Execution details: My next birthday and SUID are hard coded in main method. Following are sample commands,
python lab2.py [gcd_host_name] [gcd_port_id]
python lab2.py localhost 42024 

"""
import pickle
import socket
import sys
import selectors
import time
from random import randrange
from datetime import datetime, timedelta, date
from enum import Enum
import threading

LISTENER_HOST = 'localhost' # 127.0.0.1
LISTENER_PORT = 0 # library allocate any free port
BUF_SIZE_IN_BYTES = 1024
ADDR_FAMILY = socket.AF_INET
SOCK_TYPE = socket.SOCK_STREAM
REQ_QUEUE_SIZE = 10
CHECK_INTERVAL_IN_SECS = 0.5
ASSUME_FAILURE_TIMEOUT_IN_SECS = 2
MSG_JOIN = 'JOIN'
TIME_FORMAT = '%H:%M:%S.%f'
KEEP_RUNNING = True

class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer
    #ELECTION_IN_PROGRESS = 'ELECTION_IN_PROGRESS' # Implies election is in progress

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'
    SEND_OK_ON_PROBE = 'OK_ON_PROBE'
    SEND_PROBE = 'PROBE'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)

class Lab2(object):
    """
    Class to dynamically elect a leader
    """

    def __init__(self, gcd_address, next_birthday, su_id):
        """
        Constructs a Lab2 object to talk to the given Group Coordinator Daemon.

        :param gcd_address: tuple of (host, port) of GCD daemon
        :param next_birthday: datetime object of my next birthday
        :param su_id: my SeattleU ID
        """
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        days_to_birthday = (next_birthday - datetime.now()).days
        self.pid = (days_to_birthday, int(su_id))
        self.members = {}
        self.states = {} # {peer: State}
        self.bully = None # None means election is pending, otherwise this will be pid of leader
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()

    def probe_leader(self):
        """Occassionally probes the leader"""
        while KEEP_RUNNING:
            print('Executing thread...bully = {}'.format(self.bully))
            if self.bully != None and self.bully != self.pid:
                print('Inside thread if condition...')
                peer_sock_for_probe = self.get_connection((self.bully, self.members[self.bully]))
                self.set_state(State.SEND_OK_ON_PROBE, peer_sock_for_probe)
            time.sleep(randrange(5000, 15000)/1000)

    def run(self):
        """Starts the work for this process."""
        print('Starting work for process ID: "{}" on address: "{}"'.format(self.pid, self.listener_address))
        self.join_group()
        self.selector.register(self.listener, selectors.EVENT_READ)
        self.start_elections('joining group...')
        probe_thread = threading.Thread(target=self.probe_leader)
        #probe_thread.daemon = True
        #probe_thread.start()
        while KEEP_RUNNING:
            for key, mask in self.selector.select(timeout=CHECK_INTERVAL_IN_SECS):
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                else:
                    self.send_message(key.fileobj)
            self.check_timeouts()
        #probe_thread.join()      

    def accept_peer(self):
        """Accepts the connection from a peer process"""
        try:
            peer, address = self.listener.accept()
            print('{}: connection accepted at {}'.format(self.pr_sock(peer), self.pr_now()))
            peer.setblocking(False)
            self.set_state(State.WAITING_FOR_ANY_MESSAGE, self.listener) # TBD
            self.set_state(State.WAITING_FOR_ANY_MESSAGE, peer)
            self.selector.register(peer, selectors.EVENT_READ)
        except Exception as err:
            print(err)

    def check_timeouts(self):
        """
        Sees if I have been waiting long enough without getting any OK responses to my ELECTION messages to declare victory myself or
        if I got one or more OK's have I waited long enough to start another election
        """
        count_of_peers_waiting_for_ok = 0
        count_of_peers_expired_while_waiting_for_ok = 0
        for peer in self.states:
            if peer == self.listener:
                continue
            if self.get_state(peer) == State.WAITING_FOR_OK:
                count_of_peers_waiting_for_ok = count_of_peers_waiting_for_ok + 1
                if self.is_expired(peer):
                    print('Peer socket has expired {}'.format(peer.getsockname()))
                    count_of_peers_expired_while_waiting_for_ok = count_of_peers_expired_while_waiting_for_ok + 1
                    self.set_quiescent(peer)
                    
        if (count_of_peers_expired_while_waiting_for_ok > 0 and count_of_peers_expired_while_waiting_for_ok == count_of_peers_waiting_for_ok):
            # there is at least one peer which is waiting for ok and all the peers that are waiting for ok has expired
            self.declare_victory('none of the higher processes have responded within timeout period')
        elif self.get_state(self.listener) == State.WAITING_FOR_VICTOR and self.is_expired(self.listener) and self.is_election_in_progress():
            self.start_elections('after waiting long enough for victory message upon receiving OK from a higher process')

    def is_expired(self, peer=None, threshold = ASSUME_FAILURE_TIMEOUT_IN_SECS):
        """Verifies if the time period for waiting for a message has expired or not"""
        state, timestamp = self.get_state(peer, True)
        state_start_time = datetime.strptime(timestamp, TIME_FORMAT)
        wait_end_time = (state_start_time + timedelta(seconds=threshold))
        current_time = datetime.strptime(self.pr_now(), TIME_FORMAT)
        
        return wait_end_time < current_time 
            
    def join_group(self):
        """JOIN the group by talking to GCD"""
        with socket.socket(ADDR_FAMILY, SOCK_TYPE) as gcd_sock:
            address = (self.gcd_address[0], self.gcd_address[1])
            msg_data = (self.pid, self.listener_address)
            print('JOIN GCD on {} with message data {}'.format(address, msg_data))
            gcd_sock.connect(address)
            self.members = self.message(gcd_sock, (MSG_JOIN, msg_data))
            print('Received response from GCD: {}\n'.format(self.members))

    def is_election_in_progress(self):
        """If my process does not have a leader then election is in progress otherwise not."""
        return self.bully == None

    def set_leader(self, new_leader):
        """
        Makes a note of the new leader.
        param: new_leader: This is the newly elected leader 
        """
        print('Leader is: "{}"'.format(new_leader))
        self.bully = new_leader

    def update_members(self, their_idea_of_membership):
        """
        Updates internal list of members from all other incoming messages with a member list payload.
        1. Overwrites listener address for already-known members.
        2. Adds previously unknown members.
        3. Keeps members that are not in incoming message payload.

        param: their_idea_of_membership: list of members received from peers.
        """
        self.members.update(their_idea_of_membership)

    def start_elections(self, reason):
        """        
        1. Retrives list of higher process IDs.
        2. If I am the highest process then declare victory
        3. If not, create a connection to higher processe's listener and set state of this connection to send ELECTION msg.
        4. If unable to create a successful connection to at least one higher process then declare victory.
        
        param: reason: contains message as to why we are starting elections
        """
        print('Starting elections upon {}\n'.format(reason))
        higher_process_ids = self.get_higher_process_ids()
        if not higher_process_ids:
            self.declare_victory(' not finding any other higher process IDs...')
            return

        connected_to_at_least_a_higher_proc = False
        
        for higher_proc_id in higher_process_ids:
            member_addr = self.members[higher_proc_id]
            print('Connecting to {}'.format(member_addr))
            peer_sock = self.get_connection((higher_proc_id, member_addr))
            if not peer_sock:
                print('Skipping connecting to a peer while starting elections...')
                continue
            connected_to_at_least_a_higher_proc = True
            self.set_state(State.SEND_ELECTION, peer_sock)

        if not connected_to_at_least_a_higher_proc:
            self.declare_victory(' failed to connect to higher process IDs = {}...'.format(higher_process_ids))
        else:
            self.set_leader(None) # we are in an election

    def get_higher_process_ids(self):
        """Prepares a list of all process IDs that are higher than my process ID"""
        higher_process_ids = []
        for process_id in self.members:
            if process_id == self.pid:
                continue
            if self.is_higher_pid(self.pid, process_id):
                higher_process_ids.append(process_id)

        return higher_process_ids

    def get_highest_pid(self, pid_to_addrs):
        """
        Find the highest process IDs out of list of PIDs. Used to determine the leader when a Victory message is received by me.
        
        param: pid_to_addrs: list of members (dictionary of process IDs to member address)
        """
        h_proc_id = None
        for proc_id in pid_to_addrs:
            if not h_proc_id:
                h_proc_id = proc_id
                continue
            if self.is_higher_pid(h_proc_id, proc_id):
                h_proc_id = proc_id
        return h_proc_id

    def is_higher_pid(self, base_pid, target_pid_to_compare):
        """
        Compares two process IDs (base vs target) and determines if target PID is higher than base PID

        param: base_pid: This is the base process ID against which the target is compared to.
        param: target_pid_to_compare:  This is the target process ID which is used to determine if it is higher than the base or not.
        """
        try:
            base_bday, base_suid = base_pid
            target_bday, target_suid = target_pid_to_compare
        except (ValueError, TypeError):
            raise ValueError('Malformed message data, expected ((days_to_bd, su_id), (host, port))')
        if not (type(base_bday) is int and type(base_suid) is int and type(target_bday) is int and type(target_suid) is int):
            raise ValueError('Malformed process id, expected (days_to_next_birthday, student_id)')
        return (((base_bday == target_bday) and (base_suid <= target_suid)) or (base_bday < target_bday))


    def declare_victory(self, reason):
        """
        Will create connections with all peers to send a victory message (COORDINATOR).
        Skips peers for which we are unable to establish connection.
        Marks myself as the leader.

        param: reason: reason for declaring victory
        """
        print('Declaring victory upon {} '.format(reason))
        for peer_pid in self.members:
            if peer_pid == self.pid:
                continue
            peer_sock = self.get_connection((peer_pid, self.members[peer_pid]))

            if not peer_sock:
                print('Skipping peer while declaring victory as unable to connect to her...')
                continue
                
            self.set_state(State.SEND_VICTORY, peer_sock)
        self.set_leader(self.pid)

    def get_connection(self, member):
        """
        1. Creates a TCP network socket
        2. Connects to the peer's listening server
        3. Registers the non-blocking socket to be monitored by the selector for write events.

        param: member: peer process (pid, addr) to which we need to establish a connection to send/receive messages.
        return: peer non-blocking socket
        """
        try:
            peer_sock = socket.socket(ADDR_FAMILY, SOCK_TYPE)
            peer_pid, peer_addr = member
            peer_sock.connect(peer_addr)
            peer_sock.setblocking(False)
            self.selector.register(peer_sock, selectors.EVENT_WRITE)
            return peer_sock
        except ConnectionRefusedError as err:
            print('Received "{}" upon attempting to connect to {}...'.format(err, member))

    def receive_message(self, peer):
        """
        Receive a queued message from a given peer (based on its current state)...
        1. Upon receiving an ELECTION message, switch socket mode to send OK message to the peer.
        2. Upon receiving OK message from a peer, change my state to Waiting for victory message
        3. Upon receiving victory message, make a note of the leader

        param: peer: Peer process who is sending the incoming message
        return: returns the entire message received by this process.
        """
        state = self.get_state(peer)
        print('{}: receiving {} [{}]'.format(self.pr_sock(peer), state.value, self.pr_now()))
        try:
            msg = self.receive(peer)
            try:
                msg_name, msg_data = msg
                if msg_name == State.SEND_ELECTION.value:
                    self.set_state(State.SEND_OK, peer, True)
                    self.update_members(msg_data)
                elif msg_name == State.SEND_OK.value:
                    self.set_state(State.WAITING_FOR_VICTOR, self.listener)
                    self.set_quiescent(peer)               
                elif msg_name == State.SEND_PROBE.value:
                    self.set_state(State.SEND_OK_ON_PROBE, peer, True)
                elif msg_name == State.SEND_VICTORY.value:
                    self.set_state(State.QUIESCENT, self.listener) # resetting my current state
                    self.set_quiescent(peer)
                    self.update_members(msg_data)
                    hpid = self.get_highest_pid(msg_data)
                    self.set_leader(hpid) # The leader is the highest proc_id from the list of members sent by the leader
            except (ValueError, TypeError):
                raise ValueError('Malformed message data, expected (MSG_NAME, dict((bday, pid):(host, port)))')
            return msg
        except ConnectionError as err:
            print('Closing {}'.format(err))
            self.set_quiescent(peer)
            return
        except socket.timeout:
            print('Socket timedout...')
        except Exception as err:
            return

    @staticmethod
    def receive(peer, buffer_size=1024):
        """Retrieve a pickled message from a peer"""
        raw = peer.recv(buffer_size)
        try:
            return pickle.loads(raw)
        except Exception:
            print(bytes('Expected a pickled message, got ' + str(raw)[:100] + '\n', 'utf-8'))
            raise
        
    def send_message(self, peer):
        """
        Send the queued message to the given peer (based on its current state)....
        1. If sending a victory message then make sure we remove all other processes which are higher than this process (removing non-active processes),
           so that peer can make a note of the leader.
        2. If it is a probe message then modify the message name to OK and remove the member information.
        3. If we have sent an OK message to a peer then start an election if one is not already in progress.

        param: peer: send a message to this peer process
        """
        state = self.get_state(peer)
        msg_name = state.value
        msg_data = {}
        if state == State.SEND_VICTORY:
            higher_pids = self.get_higher_process_ids()
            for pid in self.members:
                if pid not in higher_pids:
                    msg_data[pid] = self.members[pid]
        elif state == State.SEND_OK_ON_PROBE:
            msg_data = None
            msg_name = State.SEND_OK
        else:
            msg_data = self.members

        print('{}: sending {} [{}]'.format(self.pr_sock(peer), msg_name, self.pr_now()))
        try:
            self.send(peer, msg_name, msg_data)
        except ConnectionError as err:
            print('Closing {}'.format(err))
            self.set_quiescent(peer)
            return
        except Exception as err:
            print(err)

        if (state == State.SEND_OK or state == State.SEND_VICTORY):
            self.set_quiescent(peer)

        if state == State.SEND_OK and not self.is_election_in_progress():
            self.start_elections('in response to receiving ELECTION msg')
        # check to see if we want to wait for response immediately
        elif state == State.SEND_ELECTION:
            self.set_state(State.WAITING_FOR_OK, peer, switch_mode=True)

    @classmethod # uses the class to know which receive method to call
    def send(cls, peer, message_name, message_data=None, wait_for_reply=False, buffer_size=BUF_SIZE_IN_BYTES):
        """Sending a picked message to the peers"""
        send_data = (message_name, message_data)
        try:
            peer.sendall(pickle.dumps(send_data))
        except Exception:
            print(bytes('Failed to send a pickled message ' + str(send_data)[:100] + '\n', 'utf-8'))
            raise

    def set_quiescent(self, peer):
        """Erase any memory of this peer"""
        self.set_state(State.QUIESCENT, peer)
        self.selector.unregister(peer)
        peer.shutdown(socket.SHUT_RDWR)
        peer.close()

    @staticmethod
    def message(sock, send_data, buffer_size=1024):
        """
        Pickles and sends the given message to the given socket and unpickles the returned value and returns it.

        :param sock: socket to message/recv
        :param send_data: message data (anything pickle-able)
        :param buffer_size: number of bytes in receive buffer
        :return: message response (unpickled--pickled data must fit in buffer_size bytes)
        """
        sock.sendall(pickle.dumps(send_data))
        raw = sock.recv(buffer_size)
        try:
            return pickle.loads(raw)
        except Exception:
            print(bytes('Expected a pickled message, got ' + str(raw)[:100] + '\n', 'utf-8'))
            raise

    def set_state(self, state, peer=None, switch_mode=False):
        """
        Sets the state of a peer

        param: state: state of the peer
        param: peer: peer process whose state is being set
        param: switch_mode: If the event mode needs to be changed or not
        """
        self.states[peer] = (state, self.pr_now())
        if switch_mode:
            key = self.selector.get_key(peer)
            event = selectors.EVENT_WRITE if key.events == selectors.EVENT_READ else selectors.EVENT_READ
            self.selector.modify(peer, event)
    
    def get_state(self, peer=None, details=False):
        """
        Look up current state in table.

        :param peer: socket connected to peer process (None means self)
        :param detail: if True, then the state and Timestamp are both returned
        :return: either the state or (state, timestamp) depending on detail (not found gives (QUIESCENT, None))
        """
        if peer is None:
            peer = self
        status = self.states[peer] if peer in self.states else (State.QUIESCENT, None)
        return status if details else status[0]

    @staticmethod
    def start_a_server():
        """
        Called by constructor to start a listening server
        :return: Tuple of listening socket, listener address (host, port)
        """        
        try:
            listener_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener_sock.bind((LISTENER_HOST, LISTENER_PORT))
            listener_sock.listen(REQ_QUEUE_SIZE)
            listener_sock.setblocking(False)
        except Exception as err:
            print('Failure while starting a listener: {}\n'.format(err))
            listener_sock.close()
            raise
        return (listener_sock, listener_sock.getsockname())

    @staticmethod
    def pr_now():
        """Printing helper for current timestamp."""
        return datetime.now().strftime(TIME_FORMAT)

    def pr_sock(self, sock):
        """Printing helper for given socket."""
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock)

    @staticmethod
    def cpr_sock(sock):
        """Static version of helper for printing given socket."""
        l_port = sock.getsockname()[1]
        try:
            r_port = sock.getpeername()[1]
        except OSError:
            r_port = '???'
        return '{}->{} ({})'.format(l_port, r_port, id(sock))

    def pr_leader(self):
        """Printing helper for current leader's name."""
        return 'unknown' if self.bully is None else ('self' if self.bully == self.pid else self.bully)

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python lab2.py GCDHOST GCDPORT")
        exit(1)
    host, port, su_id = sys.argv[1:]
    my_next_bday = datetime(2020, 4, 2, 8, 0)
    # my_su_id = 4089370
    lab2 = Lab2((host, port), my_next_bday, int(su_id))
    lab2.run()
