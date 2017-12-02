#!/usr/bin/python

from __future__ import print_function # for python 2 compatibility
import hackathon_protocol
import os
import lightgbm as lgb

USERNAME="the_Heartbreakers"
PASSWORD="94ba670a"

CONNECT_IP = os.environ.get("HACKATHON_CONNECT_IP") or "127.0.0.1"
CONNECT_PORT = int(os.environ.get("HACKATHON_CONNECT_PORT") or 12345)


class MyClient(hackathon_protocol.Client):
    def __init__(self, sock):
        super(MyClient, self).__init__(sock)
        self.counter = 0
        self.target_instrument = 'TEA'
        self.send_login(USERNAME, PASSWORD)
        self.last_raw = None

        # Load pre-trained model previously created by create_model.ipynb
        self.model = lgb.Booster(model_file='my_model.txt')

    def on_header(self, csv_header):
        self.header = {column_name: n for n, column_name in enumerate(csv_header)}
        #print("Header:", self.header)

    def on_orderbook(self, cvs_line_values):

        self.last_raw = [
            int(cvs_line_values[self.header['ASK_P_1']]),
            int(cvs_line_values[self.header['BID_P_1']]),
            int(cvs_line_values[self.header['ASK_P_2']]),
            int(cvs_line_values[self.header['BID_P_2']])]

    def make_prediction(self):
        assert self.last_raw is not None
        prediction = self.model.predict([self.last_raw])
        answer = prediction[0]
        self.send_volatility(answer)
        self.last_raw = None

    def on_score(self, items_processed, time_elapsed, score_value):
        print("Completed! items processed: %d, time elapsed: %.3f sec, score: %.6f" % (items_processed, time_elapsed, score_value))
        self.stop()


def on_connected(sock):
    client = MyClient(sock)
    client.run()


def main():
    hackathon_protocol.tcp_connect(CONNECT_IP, CONNECT_PORT, on_connected)


if __name__ == '__main__':
    main()
