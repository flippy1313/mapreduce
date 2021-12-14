"""Utils file.

This file is to house code common between the Manager and the Worker

"""
import socket
import json
from queue import Queue
import pathlib


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


def partition(list, num):
    """Doc."""
    part = []
    for i in range(num):
        part.append([])
    for i in range(len(list)):
        part[i % num].append(list[i])
    return part


def send_msg(message, port, IP="localhost", TCP=True):
    """Doc."""
    # create an INET, STREAMing socket, this is TCP
    if TCP:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect((IP, port))
            # breakpoint()
            # send a message
            sock.sendall(message.encode('utf-8'))
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # connect to the server
            sock.connect((IP, port))
            message = json.dumps(message)
            # breakpoint()
            # send a message
            sock.sendall(message.encode('utf-8'))
