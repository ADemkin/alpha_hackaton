#!/usr/bin/python
from __future__ import print_function   # for python 2 compatibility
import os
import time
import sys, hashlib, re
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error

sys.path.append('solution_example') # folder with hackathon_protocol.py
import hackathon_protocol

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 20)

# Constants
WARMUP_MESSAGES = 1000
PREDICTION_HORIZON = 100
ORDERBOOK_DEPTH = 10
EXPECTED_CVS_ELEMENTS_COUNT = 2 + 4*ORDERBOOK_DEPTH # instrument + time + (price+volume)*(bid+ask)* depth

# Command line setup parameters
HOST = ''
PORT = 12345
DATAFILE = None
TARGET_INSTRUMENT = 'TEA'
ENABLE_PROGRESS_BAR = True
OUTPUT_LOG_DIR = None

class CheckSolutionServer:
    def __init__(self):

        self.time0 = time.time()
        self.orderbooks_count = 0

        print("Loading data from '%s'..." % DATAFILE)
        self.dataframe = pd.read_csv(DATAFILE, sep=';')
        loaded_items = len(self.dataframe.index)
        print("Loaded", loaded_items, "items, analyzing data...")
        self.answers = self.get_answers_and_cut_off_dataframe_tail()
        print("Data analyzed, preparing messages...")
        self.raw_messages = self.get_raw_messages()
        print("Prepared {} orderbooks, {} messages, ".format(self.orderbooks_count, len(self.raw_messages)))

    def run(self):
        print("Server listening on port", PORT)
        hackathon_protocol.tcp_listen(HOST, PORT, self.on_client_connected)

    def get_answers_and_cut_off_dataframe_tail(self, period=PREDICTION_HORIZON):
        # calc correct volatility
        # (shifted to the past to PREDICTION_HORIZON records of TEA)
        instrument_column_name = list(self.dataframe.columns.values)[0]
        d = self.dataframe.iloc[WARMUP_MESSAGES:].copy()
        d = d[d[instrument_column_name] == TARGET_INSTRUMENT]   # only TEA instrument

        if len(d.index) == 0:
            message = "Instrument '{}' is not found in dataset ".format(TARGET_INSTRUMENT)
            raise ValueError(message)

        midprice = (d['BID_P_1'] + d['ASK_P_1']) / 2
        r = midprice.rolling(window=period).std().shift(-(period-1)).dropna()
        last_index = r.index[-1]
        self.dataframe = self.dataframe.ix[:last_index + 1]  # trim dataframe from the end
        return r

    def get_raw_messages(self):
        result = []

        header = tuple(self.dataframe.columns.values)
        header = header[:EXPECTED_CVS_ELEMENTS_COUNT] # drop Y column
        header_msg = hackathon_protocol.prepare_header_raw_message(header)
        predict_msg = hackathon_protocol.prepare_predict_now_raw_message()

        result.append((False, header_msg))

        for tt in self.dataframe.itertuples():
            #if n > 100000: break
            n, csv_items = tt[0], tt[1:]
            csv_items = csv_items[:EXPECTED_CVS_ELEMENTS_COUNT]  # prevent sending answer if it is present
            instrument = csv_items[0]
            need_response = n > WARMUP_MESSAGES and instrument == TARGET_INSTRUMENT
            raw_msg = hackathon_protocol.prepare_orderbook_raw_message(csv_items)
            result.append((False, raw_msg))
            self.orderbooks_count += 1
            if need_response:
                result.append((True, predict_msg))
        return result

    class Session(hackathon_protocol.Server):
        def __init__(self, sock, raw_messages, correct_answers, orderbooks_count):
            super(CheckSolutionServer.Session, self).__init__(sock)
            self.counter = 0
            self.orderbooks_count = orderbooks_count
            self.username = None
            self.pass_hash = None
            self.raw_messages = raw_messages
            self.start_time = time.time()
            self.volatility_responses_count = 0
            self.users_answers = []
            self.correct_answers = correct_answers
            self.expected_item_num = None
            self.on_finish_called = False
            self.output_log_dir = OUTPUT_LOG_DIR
            self.session_log = []

        def is_log_enabled(self): return False

        def on_login(self, username, pass_hash):

            if self.username is not None:
                self.log_message("Unexpecting logon. Ignoring.")
                return

            print("LOGIN '{}' '{}'".format(username, pass_hash))

            self.start_time_we_wait_user_response_from = None
            self.counter = 0
            self.username = username
            self.pass_hash = pass_hash
            self.send_next()

        def on_volatility(self, volatility):

            if self.expected_item_num is None:
                # we do not expect volatility right now
                return

            self.start_time_we_wait_user_response_from = None

            self.volatility_responses_count += 1
            # remember answer to item_num's response
            self.users_answers.append((self.expected_item_num, volatility))
            self.expected_item_num = None
            self.send_next()

        def log(self, is_send, raw_message):
            self.session_log.append((time.time(), is_send, raw_message))

        def send_next(self):
            N = len(self.raw_messages)
            while True:
                if self.counter < N:
                    need_response, raw_message = self.raw_messages[self.counter]
                    item_num = self.counter
                    self.send_raw_message(raw_message)
                    self.counter += 1

                    if self.counter % 20000 == 0:
                        self.report_progress(self.counter, N)

                    if need_response:
                        # wait user's response for this orderbook
                        self.expected_item_num = item_num
                        self.start_time_we_wait_user_response_from = time.time()
                        break
                else:
                    self.report_progress(N, N)
                    self.on_finish()
                    self.stop()  # stop current session
                    break

        def on_finish(self):
            if self.on_finish_called: return
            elapsed_time = time.time() - self.start_time
            score = self.calc_score()

            self.log_message("\nSCORE %.3f, time: %.3f sec, %d orderbooks sent, %d responses processed"\
                             % (score, elapsed_time, self.counter, self.volatility_responses_count))

            self.send_score(self.counter, elapsed_time, score)
            self.save_session_log()
            self.on_finish_called = True

        def report_progress(self, current, total):

            print_progress_bar(current, total)

        def user_response_timeout(self, timeout):
            self.log_message("Response timeout {} ".format(timeout))
            self.stop()

        def log_message(self, message):
            print(message)
            self.session_log.append((time.time(), None, message))

        def calc_score(self):
            ua = self.users_answers
            a = self.correct_answers
            b = pd.Series([i[1] for i in ua], index=[i[0] for i in ua])
            delta = 10
            if abs(len(a) - len(b)) < delta: # dont care if difference is small
                a = a[:min(len(a), len(b))]
                b = b[:min(len(a), len(b))]

                mse = np.sqrt(mean_squared_error(a, b))
                if mse > 0:
                    return 10.0 / mse

                self.log_message('Mse is zero. Score=0')
            else:
                self.log_message('Incorrect number of user answers {} (expected: {}). Score=0'.format(len(b), len(a)))

            return 0.0

        def save_session_log(self):
            #print("save_session_log({})".format(self.output_log_dir))
            if not self.output_log_dir: return
            if self.on_finish_called: return

            def get_msecs_str(t):
                return "%03d" % (round(t*1000) % 1000)

            username = self.username or "unknown"
            timestamp = time.strftime('%Y%m%d-%H%M%S-', time.localtime(self.start_time)) + get_msecs_str(self.start_time)
            filename = os.path.join(self.output_log_dir, "%s_%s.log" % (timestamp, username))

            with open(filename, 'w') as output:
                for t, is_send, raw_message in self.session_log:
                    output.write(time.strftime('%Y.%m.%d %H:%M:%S.', time.localtime(t)) + get_msecs_str(t))

                    if is_send: output.write(" [SENT] ")
                    elif is_send is not None: output.write(" [RECV] ")
                    else: output.write("        ")

                    if isinstance(raw_message, str): output.write(raw_message)
                    else: output.write(hackathon_protocol.bytes_to_string(raw_message))

                    output.write('\n')

            print("Log file saved at", filename)

        def try_read_pid_file(self):

            if not isinstance(FILE_WITH_PID_TO_NOTIFY, str):
                self.log_message('Disabled notify on finish')
                return

            if not os.path.isfile(FILE_WITH_PID_TO_NOTIFY):
                self.log_message('PID file "{}" does not exist'.format(FILE_WITH_PID_TO_NOTIFY))
                return
            try:
                content = open(FILE_WITH_PID_TO_NOTIFY).read()
            except OSError:
                self.log_message('Cannot read file with PID "{}"'.format(FILE_WITH_PID_TO_NOTIFY))
                return

            try:
                lines = content.splitlines(False)
                print("notify_pid", lines)
                self.notify_pid = int(lines[0].strip())
                if self.notify_pid < 0: raise ValueError('PID should not be negative')

                if len(lines) > 1 and lines[1].strip():
                    self.output_log_dir = lines[1].strip()

            except ValueError:
                self.log_message('PID file "{}" contains invalid PID "{}"'.format(FILE_WITH_PID_TO_NOTIFY, content[:1000]))
                return

    def on_client_connected(self, sock, address):

        session = CheckSolutionServer.Session(sock, self.raw_messages, self.answers, self.orderbooks_count)

        try:
            session.run()
            session.on_finish()
        finally:
            session.save_session_log()


def print_progress_bar(iteration, total):
    if not ENABLE_PROGRESS_BAR: return
    prefix, suffix = '', ''
    decimals, length, fill = 1, 100, u'\u2588'
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
    # Print New Line on Complete
    if iteration == total:
        print('\n')


def main():
    global DATAFILE, HOST, PORT, FORK_ON_CONNECT, ENABLE_PROGRESS_BAR, \
        OUTPUT_LOG_DIR, TARGET_INSTRUMENT

    import argparse

    parser = argparse.ArgumentParser(description="Alfa Hackathon validation server")
    parser.add_argument("datafile", help="CSV data file", default="data/training.csv", nargs='?')
    parser.add_argument("--host", "-ip", help="server listen ip", default='0.0.0.0')
    parser.add_argument("--port", "-p", help="server listen port", type=int, default=12345)
    parser.add_argument("--instrument", "-i", help="Target instrument we calculation volatility for", default="TEA")
    parser.add_argument("--no-progress", "-n", help="Disable progress bar in console", action="store_true")
    parser.add_argument("--log-dir", "-l", help="Path to directory to put logs", default=None)

    args = parser.parse_args()

    DATAFILE = args.datafile
    HOST = args.host
    PORT = args.port
    TARGET_INSTRUMENT = args.instrument
    ENABLE_PROGRESS_BAR = not args.no_progress
    OUTPUT_LOG_DIR = args.log_dir

    server = CheckSolutionServer()
    server.run()


if __name__ == '__main__':
    main()
