#!/usr/bin/python
from __future__ import print_function # for python 2 compatibility
import hackathon_protocol
import math, os


USERNAME="the_Heartbreakers"
PASSWORD="94ba670a"

CONNECT_IP = os.environ.get("HACKATHON_CONNECT_IP") or "127.0.0.1"
CONNECT_PORT = int(os.environ.get("HACKATHON_CONNECT_PORT") or 12345)


def calc_volatility(mid_prices, window_size):
    assert window_size > 1

    if len(mid_prices) < window_size:
        return 0

    window = mid_prices[-window_size:]
    mean = sum(window) / window_size
    return math.sqrt(sum([(x - mean)**2 for x in window]) / (window_size - 1))


class MyClient(hackathon_protocol.Client):
    def __init__(self, sock):
        super(MyClient, self).__init__(sock)
        self.target_instrument = 'TEA'

        self.send_login(USERNAME, PASSWORD)
        self.mid_prices = []
        self.header = {}

    def is_log_enabled(self):
        return False    # Enable or disable log

    def on_header(self, csv_header):
        self.header = { column_name: n for n, column_name in enumerate(csv_header) }
        print("Header:", self.header)

    def on_orderbook(self, cvs_line_values):

        # TODO: update your model here

        # read values using column names
        instrument = cvs_line_values[self.header['0_ID']]
        best_bid = int(cvs_line_values[self.header['BID_P_1']])
        best_ask = int(cvs_line_values[self.header['ASK_P_1']])

        if instrument == self.target_instrument:
            self.mid_prices.append((best_bid + best_ask) / 2.0)

    def make_prediction(self):
        # return current volatility as answer
        answer = calc_volatility(self.mid_prices, 100)

        # TODO: provide better prediction algorithm here
        self.send_volatility(answer)

    def on_score(self, items_processed, time_elapsed, score_value):
        print("Completed! items processed: %d, time elapsed: %.3f sec, score: %.6f" % (items_processed, time_elapsed, score_value))
        self.stop()


def on_connected(sock):
    client = MyClient(sock)
    client.run()


if __name__ == '__main__':
    hackathon_protocol.tcp_connect(CONNECT_IP, CONNECT_PORT, on_connected)
