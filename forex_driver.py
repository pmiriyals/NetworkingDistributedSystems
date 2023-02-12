"""
CPSC 5520, Seattle University
Lab 3: Pub/Sub
Author: Sai Lavanya Kanakam

Execution details: This is the main class with driver function creating subscriber and starting detection of arbitrages.
python lab3.py 50530

"""

import sys
from fxp_bytes_subscriber import Subscriber
from bellman_ford import BellmanFord

class Main:
    def driver(self, forex_server_port):
        subscriber = Subscriber(BellmanFord(), ('localhost', forex_server_port)) # Subscribes to provider upon initialization
        subscriber.start_a_listener_to_run_arbitrages() # Starts a listener and detects arbitrages
        

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python lab3.py FOREX_SERVER_PORT")
        exit(1)
    forex_server_port = int(sys.argv[1])
    main = Main()
    main.driver(forex_server_port)
    
