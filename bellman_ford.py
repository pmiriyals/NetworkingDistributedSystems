"""
CPSC 5520, Seattle University
Lab 3: Pub/Sub
Author: Sai Lavanya Kanakam
"""
from math import log
from datetime import datetime, timedelta
import copy

STALE_FEED_IN_SECS = 1.5
CONVERSION_RATE_KEY_FORMAT = '{}#{}'
SOURCE_CURRENCY = 'USD'
TOTAL_MONEY = 100
TOLERANCE = 1e-12

class BellmanFord(object):
    """
    1. Constructs graph based on feeds provided by the subscriber
    2. Continues to update the graph as and when it receives new feeds
    3. Deletes stale quotes from the graphs
    4. Detects arbitrage opportunies and prints details
    """
    def __init__(self):
        self.graph = {} # {'from_currency': {'to_cur1': {'price': quote, 'timestamp': timestamp}}}

    def arbitrage(self, price_feeds_tracker):
        """Updates graph in memory and then run arbitrage detection"""
        self.update_graph(price_feeds_tracker)
        self.run_arbitrage()

    def update_graph(self, price_feeds_tracker):
        """
        Constructs/updates the graph which is used to detect arbitrage opportunities

        param: price_feeds_tracker {'USD/GBP': {'from_cur': 'USD', 'to_cur': 'GBP', 'price':71.023, 'timestamp':'2019-10-29 05:37:31.063339'}}
        """
        #print('Updating the graph with new price feeds...')
        for market in price_feeds_tracker:
            feed = price_feeds_tracker[market]
            from_cur = feed['from_cur']
            to_cur = feed['to_cur']
            if from_cur not in self.graph:
                self.graph[from_cur] = {}
            self.graph[from_cur][to_cur] = {'price': feed['price'], 'timestamp': feed['timestamp']} # timestamp is later used to remove stale quotes
        self.sanitize_quotes()

    def sanitize_quotes(self):
        """
        Creates a deep copy of the graph and removes stale quotes
        """
        sanitized_graph = copy.deepcopy(self.graph)
        for from_cur in self.graph:
            for to_cur in self.graph[from_cur]:
                if self.graph[from_cur][to_cur]['timestamp'] + timedelta(seconds=STALE_FEED_IN_SECS) < datetime.utcnow():
                    del sanitized_graph[from_cur][to_cur]
            if sanitized_graph[from_cur] == {}:
                del sanitized_graph[from_cur]
        self.graph = sanitized_graph

    def init_dist_and_pred(self, source_cur):
        distance = {}
        predecessor = {}
        for from_cur in self.graph:
            distance[from_cur] = float('inf')
            predecessor[from_cur] = None
            for to_cur in self.graph[from_cur]:
                distance[to_cur] = float('inf')
                predecessor[to_cur] = None
                
        distance[source_cur] = 0
        return distance, predecessor

    def negative_log_convertor(self):
        ''' Log of each rate in graph and negate it'''
        neg_log_graph = {}
        for from_cur in self.graph:
            neg_log_graph[from_cur] = {}
            for to_cur in self.graph[from_cur]:
                neg_log_graph[from_cur][to_cur] = -log(self.graph[from_cur][to_cur]['price'], 10)
                if to_cur not in neg_log_graph:
                    neg_log_graph[to_cur] = {}
                neg_log_graph[to_cur][from_cur] = log(self.graph[from_cur][to_cur]['price'], 10)
        return neg_log_graph

    def get_negative_loop(self, predecessor, source_cur):
        """
        Traces back negative loop using a source currency and predecessors list
        param: predecessor This is a map of predecessors {current_node: parent_node, ...}
        param: source_cur The starting currency to be used to detect the chain, exampled: USD
        """
        cycle = [source_cur]
        next_cur = source_cur
        while True:
            next_cur = predecessor[next_cur]
            if next_cur == None:
                return []
            elif next_cur not in cycle:
                cycle.append(next_cur)
            else:
                cycle.append(next_cur)
                cycle = cycle[cycle.index(next_cur):]
                return cycle

    def run_arbitrage(self):
        """
        This is the core method which is used to detect arbitrage opportunities
        1. Builds a negative log graph based on the exchange rates received from forex provider
        2. Relaxes all the edges |V-1| times
        3. Looks for any possible negative cycle and confirms arbitrge opportunity if possible
        4. Returns upon first cycle detection
        """
        distance, predecessor = self.init_dist_and_pred(SOURCE_CURRENCY)
        neg_log_graph = self.negative_log_convertor()
        for _ in range(len(self.graph)-1):
            for from_cur in neg_log_graph:
                for to_cur in neg_log_graph[from_cur]:
                    if distance[to_cur] > distance[from_cur] + neg_log_graph[from_cur][to_cur]:
                        distance[to_cur] = distance[from_cur] + neg_log_graph[from_cur][to_cur]
                        predecessor[to_cur] = from_cur
        for from_cur in neg_log_graph:
            for to_cur in neg_log_graph[from_cur]:
                if distance[to_cur] > distance[from_cur] + neg_log_graph[from_cur][to_cur] - TOLERANCE:
                    cycle = self.get_negative_loop(predecessor, SOURCE_CURRENCY)
                    if not cycle or cycle[len(cycle)-1] != SOURCE_CURRENCY:
                        continue
                    money = TOTAL_MONEY
                    print('Cycle = {}'.format(cycle))
                    print('ARBITRAGE:\n\tstart with {} {}'.format(cycle[len(cycle)-1], money))
                    for index in range(len(cycle)-1, 0, -1):
                        f_cur = cycle[index]
                        t_cur = cycle[index-1]
                        if (f_cur not in self.graph) or (t_cur not in self.graph[f_cur]):
                            exchange_rate = 1/self.graph[t_cur][f_cur]['price']
                        else:
                            exchange_rate = self.graph[f_cur][t_cur]['price']
                        money = money*exchange_rate
                        print('\texchange {} for {} at {} --> {} {}'.format(f_cur, t_cur, exchange_rate, t_cur, money))
                    return
