"""Doc."""
import os
import logging
import json
from queue import Queue
import time
import threading
import socket
import subprocess
import click
from mapreduce.utils import PathJSONEncoder, send_msg


# Configure logging
logging.basicConfig(level=logging.INFO)


class Worker:
    """Doc."""

    def __init__(self, manager_port, manager_hb_port, worker_port):
        """Doc."""
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())

        self.manager_port = manager_port
        self.manager_hb_port = manager_hb_port
        self.worker_port = worker_port
        self.worker_id = os.getpid()
        self.state = {"ready": True, "busy": False, "dead": False}
        self.signals = {"shutdown": False}
        self.msgs = Queue()
        # self.group_index = 1

        thread_handle = threading.Thread(target=self.listen, args=())
        thread_handle.start()
        # This is a fake message to demonstrate pretty printing with logging
        # thread_msg = threading.Thread(target=self.handle_msg, args=())
        # thread_msg.start()
        thread_heartbeats = threading.Thread(
            target=self.send_heartbeat,
            args=()
        )
        thread_heartbeats.start()

        thread_handle.join()
        thread_heartbeats.join()

    def send_heartbeat(self):
        """Doc."""
        # create an INET, STREAMing socket, this is UDP
        message = {
            "message_type": "heartbeat",
            "worker_pid": self.worker_id
        }
        while not self.signals["shutdown"]:
            send_msg(message, self.manager_hb_port, TCP=False)
            time.sleep(2)

    # def handle_msg(self, message_dict):
    #     while not self.signals["shutdown"]:
    #         if not self.msgs.empty():
    #             m = self.msgs.empty()
    #             if m["message_type"] == "new_worker_job":
    #                 self.do_task(self,m)
    #             elif m["message_type"] == "new_sort_job":
    #                 self.do_sort(self,m)
    #         time.sleep(0.1)

    def do_task(self, _m_):
        """Wo."""
        input_files = _m_["input_files"]
        output_directory = _m_["output_directory"]
        executable = _m_["executable"]
        output_files = []
        # breakpoint()
        mode = "mapping"
        if input_files[0].find("/reduce") != -1:
            mode = "reduce"
        for file in input_files:
            if mode == "mapping":
                output_file = output_directory + '/' + file[-6:]
            else:
                output_file = output_directory + '/' + file[-8:]
            with open(file, "r", encoding='UTF-8') as _f_:
                with open(output_file, "w+", encoding='UTF-8') as outf:
                    subprocess.run(
                        [executable], stdin=_f_,
                        stdout=outf, check=True
                    )
                    output_files.append(output_file)
        message_chunk = {
                "message_type": "status",
                "output_files": output_files,
                "status": "finished",
                "worker_pid": self.worker_id
            }
        send_msg(
            json.dumps(message_chunk, cls=PathJSONEncoder),
            self.manager_port
        )

    def do_sort(self, _m_):
        """Doc."""
        input_files = _m_["input_files"]
        output_file = _m_["output_file"]
        tmp = []

        with open(output_file, 'w+', encoding='UTF-8') as out:
            for _f_ in input_files:
                with open(_f_, 'r', encoding='UTF-8') as _fd_:
                    for _ff_ in _fd_:
                        tmp.append(_ff_)
            tmp.sort()
            for item in tmp:
                out.write(item)

        message_dict = {
                "message_type": "status",
                "output_file": output_file,
                "status": "finished",
                "worker_pid": self.worker_id
            }
        send_msg(
            json.dumps(message_dict, indent=2, cls=PathJSONEncoder),
            self.manager_port
        )

    def listen(self):
        """Doc."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.worker_port))
            sock.listen()
            if self.state["ready"]:
                message_dict = {
                    "message_type": "register",
                    "worker_host": "localhost",
                    "worker_port": self.worker_port,
                    "worker_pid": self.worker_id
                }
                logging.debug(
                    "Worker:%s received\n%s",
                    self.worker_port,
                    json.dumps(message_dict, indent=2),
                )
                send_msg(json.dumps(message_dict, indent=2), self.manager_port)

            sock.settimeout(1)
            while not self.signals["shutdown"]:

                try:
                    clientsocket = sock.accept()[0]
                except socket.timeout:
                    continue

                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                    message_bytes = b''.join(message_chunks)
                    message_str = message_bytes.decode("utf-8")
                    try:
                        message_dict = json.loads(message_str)
                    except json.JSONDecodeError:
                        continue
                    if message_dict["message_type"] == "shutdown":
                        self.signals["shutdown"] = True
                    elif message_dict["message_type"] == "register_ack":
                        continue
                    elif message_dict["message_type"] == "new_worker_task":
                        self.do_task(message_dict)
                    elif message_dict["message_type"] == "new_sort_task":
                        self.do_sort(message_dict)

            time.sleep(2)


@click.command()
@click.argument("manager_port", nargs=1, type=int)
@click.argument("manager_hb_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(manager_port, manager_hb_port, worker_port):
    """Instantiate worker object for this process."""
    Worker(manager_port, manager_hb_port, worker_port)


if __name__ == '__main__':
    main()
