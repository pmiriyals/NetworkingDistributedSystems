"""
CPSC 5520, Seattle University
Lab 3: Pub/Sub
Author: Sai Lavanya Kanakam
"""
import socket
import time
import ipaddress
from datetime import datetime, timedelta
from array import array
import threading
from bellman_ford import BellmanFord

SUBSCRIBER_ADDRESS = ('localhost', 50630)
BUF_SIZE = 4096
QUOTE_SIZE = 32
MICROS_PER_SECOND = 1_000_000
SUBSCRIPTION_RENEWAL_TIME_IN_SECS = 15
MAX_SUBSCRIPTION_TIME_IN_SECS = 600 # 10 mins
MAX_RENEWALS = 10_000 # Can request for renewals for up to 10K times
UTF_ENCODING = 'utf-8'

class Subscriber(object):
    """
    1. Subscribes to the forex publishing service,
    2. Renews subscription every 15 seconds
    3. Ignores out of sequence messages from forex provider
    4. Passes the quotes to bellman ford arbitrage detection alg.
    """
    def __init__(self, bf_arbitrage_detector, forex_provider_server_address):
        self.bellman_ford = bf_arbitrage_detector
        self.server_address = forex_provider_server_address
        subscriber_thread = threading.Thread(target=self.subscribe)
        subscriber_thread.start() # This thread is responsible for subscribing and renewing subscriptions

    def subscribe(self):
        """Subscribes to forex provider and then keeps track of renewals."""
        start_time = datetime.utcnow()
        for renewal in range(MAX_RENEWALS):
            # Create a UDP socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            msg = self.get_serialized_address_message()
            sock.sendto(msg, self.server_address)
            print('Successfully subscribed to price feed running at host = {} and port = {}'.format(*self.server_address))
            time.sleep(SUBSCRIPTION_RENEWAL_TIME_IN_SECS)
            print('Renewing subscription, renewal attempt = {}'.format(renewal+1))
            if start_time + timedelta(seconds=MAX_SUBSCRIPTION_TIME_IN_SECS) < datetime.utcnow():
                print('Exiting the subscriber renewal thread as max subscriber duration has passed')
                break

    def get_serialized_address_message(self) -> bytes:
        """Serilizing subscriber address according to the specs"""
        ip_addr = socket.gethostbyname('localhost')
        ip_bytes = ipaddress.ip_address(ip_addr).packed
        port = array('H', [SUBSCRIBER_ADDRESS[1]])
        port.byteswap() # big endian format
        port_bytes = port.tobytes()
        
        msg = bytes()
        msg += ip_bytes[0:4]
        msg += port_bytes[0:2]

        return msg
    
    def deserialize_quotes(self, raw_quotes, price_feeds_tracker):
        """Deserilizing quotes sent by the forex provider"""
        
        for index in range(0, len(raw_quotes), QUOTE_SIZE):
            quote = raw_quotes[index:index+QUOTE_SIZE]
            cur_timestamp = self.deserialize_timestamp_by_epoch(quote[0:8])

            raw_currencies = quote[8:14]
            market = raw_currencies.decode(UTF_ENCODING)
            from_cur = market[0:3]
            to_cur = market[3:6]

            if market in price_feeds_tracker:
                existing_feed = price_feeds_tracker[market]
                if existing_feed['timestamp'] > cur_timestamp:
                    print('ignoring out-of-sequence message')
                    continue

            raw_price = quote[14:22]
            price = array('d')
            price.frombytes(raw_price)
            price_feeds_tracker[market] = {'timestamp':cur_timestamp, 'from_cur':from_cur, 'to_cur':to_cur, 'price':price[0]}

        return price_feeds_tracker

    def deserialize_timestamp_by_epoch(self, raw_timestamp):
        """Construct datetime in UTC from epoch given by the server"""
        epoch = datetime(1970, 1, 1)
        timestamp = array('Q')
        timestamp.frombytes(raw_timestamp)
        timestamp.byteswap()
        total_seconds_since_epoch = timestamp[0]/MICROS_PER_SECOND
        cur_timestamp = epoch + timedelta(seconds=total_seconds_since_epoch)
        return cur_timestamp

    def start_a_listener_to_run_arbitrages(self):
        """Listens to quotes from forex provider and invokes arbitrage detection"""
        print('starting up on {} port {}'.format(*SUBSCRIBER_ADDRESS))
        listener_start_time = datetime.utcnow()
        price_feeds_tracker = {}
        # Create a UDP socket
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(SUBSCRIBER_ADDRESS)  # subscriber binds the socket to the publishers address
            while True:
                data = sock.recv(BUF_SIZE)
                print('received {} bytes'.format(len(data)))
                price_feeds_tracker = self.deserialize_quotes(data, price_feeds_tracker)
                for market in price_feeds_tracker:
                    feed = price_feeds_tracker[market]
                    print('{} {} {} {}'.format(feed['timestamp'], feed['from_cur'], feed['to_cur'], feed['price']))
                self.bellman_ford.arbitrage(price_feeds_tracker)
                if listener_start_time + timedelta(seconds=MAX_SUBSCRIPTION_TIME_IN_SECS) < datetime.utcnow():
                    print('Stopping the listener as max subscription time has passed')
                    break

