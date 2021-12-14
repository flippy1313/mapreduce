"""Doc."""
from json.encoder import JSONEncoder
import os
import socket
import logging
import pathlib
import shutil
import heapq
import threading
import json
import time
from queue import Queue
import click
from mapreduce.utils import partition, send_msg
from contextlib import ExitStack

# Configure logging
logging.basicConfig(level=logging.DEBUG)


class PathJSONEncoder(json.JSONEncoder):
    """
    Extended the Python JSON encoder to encode Pathlib objects.

    Docs: https://docs.python.org/3/library/json.html

    Usage:
    >>> json.dumps({
            "executable": TESTDATA_DIR/"exec/wc_map.sh",
        }, cls=PathJSONEncoder)
    """

    def default(self, o):
        """Override base class method to include Path object serialization."""
        if isinstance(o, pathlib.Path):
            return str(o)
        return super().default(o)


class Manager:
    """Yay."""

    def __init__(self, port, hb_port):
        """Doc."""
        logging.info("Starting manager:%s", port)
        logging.info("Manager:%s PWD %s", port, os.getcwd())

        p = pathlib.Path.cwd() / "tmp"
        p.mkdir(parents=True, exist_ok=True)
        for job in p.glob('job-'):
            job = pathlib.Path(job)
            shutil.rmtree(job)
        self.reg = {}
        self.active = {}
        self.jobs = Queue()
        self.msgs = Queue()
        self.cur_job = None
        self.cur_type = "none"
        self.signals = {"shutdown": False}
        self.counter = 0
        self.cur_tasks = Queue()
        self.cur_num_task = 0
        self.all_dead = False
        self.port = port
        thread_listen = threading.Thread(target=self.listen, args=(port,))
        thread_listen.start()

        thread_listen_hb = threading.Thread(
            target=self.listen_hb, args=(hb_port,))
        thread_listen_hb.start()

        thread_listen_worker = threading.Thread(
            target=self.worker_status,
            args=()
        )
        thread_listen_worker.start()
        thread_listen.join()
        thread_listen_hb.join()
        thread_listen_worker.join()

    def message(self):
        """Doc."""
        if not self.msgs.empty():
            m = self.msgs.get()
            # logging.debug(json.dumps(m, indent=2))
            if m["message_type"] == "register":
                self.reg[m["worker_pid"]] = m
                self.active[m["worker_pid"]] = [True, [], "ready", 0]
                m["message_type"] = "register_ack"
                send_msg(
                    json.dumps(m), m['worker_port'], m['worker_host'])
                if self.all_dead:
                    self.all_dead = False
                if not self.cur_tasks.empty():
                    if self.cur_type == "map":
                        self.dist_job_map()
                    elif self.cur_type == "group":
                        self.dist_job_group()
                    else:
                        self.dist_job_reduce()
            elif m["message_type"] == "new_manager_job" and not self.all_dead:
                logging.info("Manager:%s begin map stage", self.port)
                m["counter"] = self.counter
                self.jobs.put(m)
                job_dir = pathlib.Path("tmp/job-"+str(self.counter))
                mapper_dir = job_dir / pathlib.Path("mapper-output")
                grouper_dir = job_dir / pathlib.Path("grouper-output")
                reducer_dir = job_dir / pathlib.Path("reducer-output")
                job_dir.mkdir()
                mapper_dir.mkdir()
                grouper_dir.mkdir()
                reducer_dir.mkdir()
                self.counter += 1
                if not self.cur_job:
                    self.dist_job_map()
            elif m["message_type"] == "status" and not self.all_dead:
                # start map or group
                if "output_files" in m:
                    if m["status"] == "finished":
                        self.cur_num_task -= 1
                        self.active[m["worker_pid"]][2] = "ready"
                        if not self.cur_tasks.empty():
                            if self.cur_type == "map":
                                self.dist_job_map()
                            else:
                                self.dist_job_reduce()
                    if self.cur_num_task == 0:
                        if self.cur_type == "map":
                            # map done, start group
                            logging.info(
                                "Manager:%s end map stage", self.port)
                            logging.info(
                                "Manager:%s begin group stage", self.port)
                            self.dist_job_group()
                        elif self.cur_type == "reduce":
                            # deal with reduced job
                            self.display_output()
                            logging.info(
                                "Manager:%s end reduce stage", self.port)
                            if not self.jobs.empty():
                                self.cur_job = None
                                self.dist_job_map()
                # start reduce
                elif "output_file" in m:
                    if m["status"] == "finished":
                        self.cur_num_task -= 1
                        self.active[m["worker_pid"]][2] = "ready"
                    if self.cur_num_task == 0:
                        logging.info("Manager:%s end group stage", self.port)
                        logging.info(
                            "Manager:%s begin reduce stage", self.port)
                        self.rearrange_sorted()
                        self.dist_job_reduce()

    # Map (Process Input files, Output <key, val>)

    def dist_job_map(self):
        """Doc."""
        if not self.cur_job:
            self.cur_job = self.jobs.get()
            files = [
                os.path.join(self.cur_job["input_directory"], f)
                for f in os.listdir(self.cur_job["input_directory"])
            ]
            files.sort()
            part = partition(files, self.cur_job["num_mappers"])
            for p in part:
                map_task = {
                    "message_type": "new_worker_task",
                    "input_files": p,
                    "executable": self.cur_job["mapper_executable"],
                    "output_directory":
                        pathlib.Path(
                            "tmp/job-"+str(self.cur_job['counter'])
                            + "/mapper-output"),
                }
                self.cur_tasks.put(map_task)
            self.cur_type = "map"
            self.cur_num_task = self.cur_job["num_mappers"]
        for w in self.active:
            if self.active[w][2] == "ready" and not self.cur_tasks.empty():
                m = self.cur_tasks.get()
                self.active[w][1] = m
                self.active[w][2] = "busy"
                m["worker_pid"] = int(w)
                logging.info(m)
                logging.info(self.reg[w]['worker_port'])
                logging.info(self.reg[w]['worker_host'])
                send_msg(
                    json.dumps(m, cls=PathJSONEncoder),
                    self.reg[w]['worker_port'],
                    self.reg[w]['worker_host']
                )

    def dist_job_group(self):
        """Doc."""
        if self.cur_type != "group":
            self.cur_type = "group"
            num_groupers = 0
            for w in self.active:
                if self.active[w][2] == "ready":
                    num_groupers += 1
            # retrieve files
            path = pathlib.Path(
                "tmp/job-{}/mapper-output".format
                (self.cur_job['counter'])
            )
            map_out_files = path.glob('*')
            map_out_files = [str(path) for path in map_out_files]
            map_out_files.sort()
            if len(map_out_files) < num_groupers:
                num_groupers = len(map_out_files)

            # start partition
            group_job_list = [[] for i in range(num_groupers)]
            count = 0
            for path in map_out_files:
                grouper_idx = count % num_groupers
                group_job_list[grouper_idx].append(path)
                count += 1

            count = 0

            for job in group_job_list:
                # job: grouping input dirs for each worker
                group_out_path = pathlib.Path(
                    "tmp/job-" + str(self.cur_job['counter'])
                    + "/grouper-output"
                )
                out_idx = f'0{count + 1}' if (count +
                                              1) <= 9 else f'{count + 1}'
                group_out_path = group_out_path / f'sorted{out_idx}'
                self.cur_tasks.put({
                    "message_type": "new_sort_job",
                    "input_files": job,
                    "out_directory": group_out_path
                })
                count += 1
        self.cur_num_task = self.cur_tasks.qsize()
        for w in self.active:
            if self.active[w][2] == "ready" and not self.cur_tasks.empty():
                cur_task = self.cur_tasks.get()
                group_task = {
                    "message_type": "new_sort_task",
                    "input_files": cur_task['input_files'],
                    "output_file": cur_task['out_directory'],
                    "worker_pid": int(w)
                }
                send_msg(json.dumps(
                    group_task, indent=2, cls=PathJSONEncoder),
                    self.reg[w]['worker_port'], self.reg[w]['worker_host'])
                self.active[w][2] = "busy"
                self.active[w][1] = group_task

    def retrieve_out_files(self):
        """Doc."""
        path = pathlib.Path(
            "tmp/job-" + str(self.cur_job['counter']) + "/grouper-output"
        )
        out_files = path.glob('*')
        out_files = [str(path) for path in out_files]
        return out_files

    def rearrange_sorted(self):
        """Doc."""
        open_sorted_files = []
        open_reducer_files = []
        reduce_path = \
            f"tmp/job-{self.cur_job['counter']}/grouper-output/reduce"
        num_reducers = self.cur_job['num_reducers']
        for i in range(num_reducers):
            i = i + 1  # start from 1
            file_path = f'{reduce_path}{("0" + str(i)) if i < 10 else i}'
            open_reducer_files.append(open(pathlib.Path(file_path), 'w'))
        output_files = self.retrieve_out_files()
        for output_file in output_files:
            open_sorted_files.append(open(pathlib.Path(output_file), 'r'))
        line_num = -1
        current_key = None
        for line in heapq.merge(*open_sorted_files):
            pair = line.split('\t')
            if pair[0] != current_key:
                line_num += 1
                current_key = pair[0]
            index = line_num % num_reducers
            open_reducer_files[index].write(line)

    def dist_job_reduce(self):
        """Doc."""
        self.cur_type = "reduce"
        num_reducers = self.cur_job["num_reducers"]

        # Retrieve files
        reduce_dir = pathlib.Path(
            "tmp/job-{}/grouper-output/".format(self.cur_job['counter'])
        )
        path = pathlib.Path(reduce_dir)
        input_files = path.glob('reduce*')
        input_files = [str(path) for path in input_files]

        # start partition
        reduce_job_list = [[] for i in range(num_reducers)]
        count = 0
        for path in input_files:
            reducer_idx = count % num_reducers
            reduce_job_list[reducer_idx].append(path)
            count += 1

        for job in reduce_job_list:
            self.cur_tasks.put({"input_files": job})

        # send message
        self.cur_num_task = len(reduce_job_list)
        for w in self.active:
            if self.active[w][2] == "ready" and not self.cur_tasks.empty():
                cur_task = self.cur_tasks.get()
                out_dir = pathlib.Path(
                    "tmp/job-{}/reducer-output/".format(
                        self.cur_job['counter']))
                reduce_task = {
                    "message_type": "new_worker_task",
                    "input_files": cur_task['input_files'],
                    "executable": self.cur_job['reducer_executable'],
                    "output_directory": out_dir,
                    "worker_pid": int(w)
                }
                send_msg(
                    json.dumps(reduce_task, cls=PathJSONEncoder),
                    self.reg[w]['worker_port'],
                    self.reg[w]['worker_host']
                )
                self.active[w][2] = "busy"
                self.active[w][1] = reduce_task

    def display_output(self):
        """Doc."""
        output_dir = pathlib.Path(self.cur_job["output_directory"])
        output_dir.mkdir()
        logging.info(str(output_dir))
        reducer_dir = f"tmp/job-{self.cur_job['counter']}/reducer-output/"
        logging.info(str(reducer_dir))
        for i, f in enumerate(pathlib.Path(reducer_dir).iterdir()):
            out_file_name = \
                f'outputfile{("0" + str(i+1)) if i < 9 else str(i+1)}'
            shutil.move(f, output_dir / pathlib.Path(out_file_name))
            logging.info(out_file_name)

    def listen(self, port):
        """LISTEN THREAD: listen TCP messages."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", port))
            sock.listen()
            sock.settimeout(1)
            while not self.signals["shutdown"]:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                logging.info("Connection from%s", address[0])
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                            logging.info(data)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                    message_bytes = b''.join(message_chunks)
                    message_str = message_bytes.decode("utf-8")
                    logging.info(message_str)
                    try:
                        message_dict = json.loads(message_str)
                        logging.info(message_dict)
                    except json.JSONDecodeError:
                        continue
                    if message_dict["message_type"] == "shutdown":
                        logging.info("shutting down")
                        for w in self.active:
                            if self.active[w][2] != "dead":
                                send_msg(
                                    json.dumps(message_dict),
                                    self.reg[w]['worker_port'],
                                    self.reg[w]['worker_host']
                                )
                        self.signals["shutdown"] = True
                    else:
                        self.msgs.put(message_dict)
                        self.message()

    def listen_hb(self, hb_port):
        """Doooc."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", hb_port))
            sock.settimeout(1)
            while not self.signals["shutdown"]:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                if message_dict["message_type"] == "heartbeat":
                    worker_pid = message_dict['worker_pid']
                    if worker_pid in self.active:
                        self.active[worker_pid][3] = 0
                time.sleep(1)

    def worker_status(self):
        """Doooc."""
        while not self.signals["shutdown"]:
            for worker in self.active:
                self.active[worker][3] += 1
                if self.active[worker][3] >= 10 and\
                        self.active[worker][2] != "dead":
                    logging.debug("dead")
                    self.active[worker][2] = "dead"
                    self.active[worker][0] = False
                    self.cur_tasks.put(self.active[worker][1])
                    logging.debug(self.active[worker][1])
                    # logging.info(self.cur_tasks)
                    send = True
                    num_dead = 0
                    num_worker = 0
                    for worker in self.active:
                        num_worker += 1
                        if self.active[worker][2] == "busy":
                            send = False
                        if self.active[worker][2] == "dead":
                            num_dead += 1
                    if num_dead == num_worker:
                        self.all_dead = True
                        break

                    if send:
                        if self.cur_type == "map":
                            self.dist_job_map()
                        elif self.cur_type == "group":
                            self.dist_job_group()
                        else:
                            self.dist_job_reduce()
            time.sleep(1)


@click.command()
@click.argument("port", nargs=1, type=int)
@click.argument("hb_port", nargs=1, type=int)
def main(port, hb_port):
    """Ji."""
    Manager(port, hb_port)


if __name__ == '__main__':
    main()
