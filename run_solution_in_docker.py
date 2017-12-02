#!/usr/bin/python

import sys, os, time, atexit, shutil
import logging
import configparser
import zipfile
import json
import tempfile
import docker

from contextlib import contextmanager

SOLUTION_PATH=''
DOCKER_NETWORK = "bridge"
DOCKER_BRIDGE_IP = "172.17.0.1"
DOCKER_BRIDGE_PORT = 12345
METADATA_FILE = 'metadata.ini'
NO_PULL = False
FILE_TO_SAVE_STDOUT = None
FILE_TO_SAVE_STDERR = None
STOP_FILE_PATH = None
DOCKER_APP_MEMLIMIT = '8g'

@contextmanager
def pushd(newDir):
    previousDir = os.getcwd()
    os.chdir(newDir)
    yield
    os.chdir(previousDir)

logger = None

TEMP_DIR = ''
MAX_TIME_SEC = 1800 # 30 min
RECEIVED_SIGUSR1 = False

def make_logger(name, log_file = None, level=logging.INFO):
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    if log_file is not None:
        handler = logging.FileHandler(log_file)

    else:
        handler = logging.StreamHandler(stream=sys.stdout)

    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

def read_ini_metadata(ini_metadata):

    if not os.path.isfile(ini_metadata):
        solution_root = os.path.dirname(ini_metadata)
        raise ValueError("Metadata file does not exit (" + ini_metadata + "), solution root: "\
                         + ', '.join(os.listdir(solution_root)))

    config = configparser.ConfigParser()
    config.read(ini_metadata)

    def strip(s):
        return s.strip().strip('"')

    return strip(config['MAIN']['docker_image']), strip(config['MAIN']['run_command'])


def get_solution_folder(solution_path):

    if os.path.isdir(solution_path):
        return solution_path

    if not os.path.isfile(solution_path):
        raise ValueError("No file or directory (" + solution_path + ")")

    if not solution_path.endswith('.zip'):
        raise ValueError("Unknown solution format (" + solution_path + ")")

    unzip_dir = os.path.join(TEMP_DIR, os.path.basename(solution_path))

    # extract
    with zipfile.ZipFile(solution_path, 'r') as zip_file:
        zip_file.extractall(path=unzip_dir)

    # check if all content is inside one subdir
    dircontent = os.listdir(unzip_dir)
    if len(dircontent) == 1:
        subdir = os.path.join(unzip_dir, dircontent[0])
        if os.path.isdir(subdir):
            unzip_dir = subdir # found subdir is root of solution

    return unzip_dir


def folder_to_tar_stream(folder):
    import tarfile
    from io import BytesIO

    tarstream = BytesIO()
    tar = tarfile.TarFile(fileobj=tarstream, mode='w')
    with pushd(folder):
        tar.add(".")  # add all content in current dir
    tar.close()

    tarstream.seek(0)
    return tarstream


def run_docker_container(solution_folder, docker_image, run_command):
    client = docker.from_env()

    workdir = '/solution/'

    if not NO_PULL:
        logger.info("Pulling docker image '{}'".format(docker_image))
        client.images.pull(docker_image)
    else:
        logger.info("Pulling disabled, docker image: '{}'".format(docker_image))

    logger.info("Creating container from '{}', command: '{}'".format(docker_image, run_command))
    container = client.containers.create(
        docker_image,
        command=run_command,
        environment={"HACKATHON_CONNECT_IP": DOCKER_BRIDGE_IP,
                     "HACKATHON_CONNECT_PORT": DOCKER_BRIDGE_PORT,
                     "PYTHONUNBUFFERED": '1'},
        network=DOCKER_NETWORK,
        mem_limit=DOCKER_APP_MEMLIMIT,
        detach=True,
        working_dir=workdir)

    tarstream = folder_to_tar_stream(solution_folder)
    container.put_archive(path=workdir, data=tarstream)

    # start container
    logger.info("Starting container...")
    container.start()

    start_time = time.time()

    # wait for finish
    res = True
    for current_status in container.stats():
        elapsed_time = time.time() - start_time

        current_status = json.loads(current_status.decode('utf8'))
        #print("current_status=", current_status)
        is_active = current_status.get('pids_stats', {}).get('current', 0) > 0
        if not is_active:
            break

        name = current_status['name']

        logger.info('Running container {} for {}'.format(name, elapsed_time))

        need_kill = False

        if STOP_FILE_PATH and os.path.isfile(STOP_FILE_PATH):
            logger.warning('Got STOP_FILE')
            need_kill = True

        if RECEIVED_SIGUSR1:
            logger.warning('Received SIGUSR1')
            need_kill = True

        if elapsed_time > MAX_TIME_SEC:
            logger.warning('Timeout elapsed_time > MAX_TIME_SEC ({} > {})'.format(elapsed_time, MAX_TIME_SEC))
            need_kill = True

        if need_kill:
            logger.warning('Killing container {}'.format(name))
            container.kill()
            logger.warning('Container killed')
            res = False
            break

    # if buffered output - give docker a chance to save it
    time.sleep(1)

    # save stdout & std err
    if FILE_TO_SAVE_STDOUT is not None:
        logger.info('Saving STDOUT to {}'.format(FILE_TO_SAVE_STDOUT))
        with open(FILE_TO_SAVE_STDOUT, 'wb') as file:
            content = container.logs(stdout=True, stderr=False, timestamps=True, tail=10000)
            file.write(content)
            logger.info('Saved STDOUT, size: {}'.format(len(content)))

    if FILE_TO_SAVE_STDERR is not None:
        logger.info('Saving STDERR to {}'.format(FILE_TO_SAVE_STDERR))
        with open(FILE_TO_SAVE_STDERR, 'wb') as file:
            file.write(container.logs(stdout=False, stderr=True, timestamps=True, tail=10000))
            logger.info('Saved STDERR, size: {}'.format(len(content)))

    if FILE_TO_SAVE_STDOUT is None and FILE_TO_SAVE_STDERR is None:
        logger.info("====== User app output =======")
        for line in container.logs(timestamps=True).splitlines():
            logger.info(line)
        logger.info("==== User app output done ====")

    return res


# note: signals way not work if we running docker app (docker will try to propagate signal inside docker app).
# Therefore we may use special file existence check to stop (see --stop-when-file-exists parameter)
def on_sigusr1(signum, stack):
    global RECEIVED_SIGUSR1
    if signum != signal.SIGUSR1: return

    logger.error("RECEIVED_SIGUSR1")
    #    open("RECEIVED_SIGUSR1", 'w').write("RECEIVED_SIGUSR1")
    RECEIVED_SIGUSR1 = True


def main():
    global TEMP_DIR

    TEMP_DIR = tempfile.mkdtemp()

    def rm_tmp():
        shutil.rmtree(TEMP_DIR)
    atexit.register(rm_tmp)

    solution_folder = get_solution_folder(SOLUTION_PATH)
    docker_image, run_command = read_ini_metadata(os.path.join(solution_folder, METADATA_FILE))
    res = run_docker_container(solution_folder, docker_image, run_command)
    logger.info("Done!" if res else "Fail!")
    return  res


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description="Run solution inside docker container." +
                                                 " Required file {} in root of solution.".format(METADATA_FILE))
    parser.add_argument("solution_path", help="path to folder or zip with solution files")
    parser.add_argument("--timeout", "-t", type=int, help="Execution timeout in seconds", default=300)
    parser.add_argument("--network", "-n", help="Docker network", default=DOCKER_NETWORK)
    parser.add_argument("--gateway-ip-address", "-g",
                        help="Gateway address inside docker container. Typically '172.17.0.1' for Linux and 'docker.for.mac.localhost' for Mac.",
                        default=DOCKER_BRIDGE_IP)

    parser.add_argument("--port", "-p", type=int, help="Connecting port", default=DOCKER_BRIDGE_PORT)

    parser.add_argument("--no-pull", help="Skip docker image pull", action="store_true")
    parser.add_argument("--listen-sigusr1", help="Listen SIGUSR1 notification to stop container", action="store_true")
    parser.add_argument("--log-file", help="Path to logger file", default=None)
    parser.add_argument("--stdout-file", help="Path to save app's stdout", default=None)
    parser.add_argument("--stderr-file", help="Path to save app's stderr", default=None)
    parser.add_argument("--stop-when-file-exists", help="Path to a file. When file is exist processing will be stopped (alternative to signals)", default=None)
    parser.add_argument("--mem-limit",
                   help="Docker app memory limit. String with a units identification char (100000b, 1000k, 128m, 1g)",
                   default=DOCKER_APP_MEMLIMIT)

    args = parser.parse_args()

    SOLUTION_PATH = args.solution_path
    DOCKER_NETWORK = args.network
    DOCKER_BRIDGE_IP = args.gateway_ip_address
    DOCKER_BRIDGE_PORT = args.port
    MAX_TIME_SEC = args.timeout
    NO_PULL = args.no_pull
    FILE_TO_SAVE_STDOUT = args.stdout_file
    FILE_TO_SAVE_STDERR = args.stderr_file
    STOP_FILE_PATH = args.stop_when_file_exists
    DOCKER_APP_MEMLIMIT = args.mem_limit

    time.sleep(1)

    logger = make_logger(__name__, args.log_file)
    #
    # FORMAT = '%(asctime)-15s %(message)s'
    # logging.basicConfig(format=FORMAT, level=logging.INFO)

    logger.info("PID: {}".format(os.getpid()))
    logger.info("MAX_TIME_SEC={}".format(MAX_TIME_SEC))

    logger.info("FILE_TO_SAVE_STDOUT={}".format(FILE_TO_SAVE_STDOUT))
    logger.info("FILE_TO_SAVE_STDERR={}".format(FILE_TO_SAVE_STDERR))
    logger.info("DOCKER_APP_MEMLIMIT={}".format(DOCKER_APP_MEMLIMIT))

    if args.listen_sigusr1:
        import signal
        signal.signal(signal.SIGUSR1, on_sigusr1)

    res = main()
    sys.exit(0 if res else 1)
