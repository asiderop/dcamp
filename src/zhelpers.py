"""
Helper module for example applications. Mimics ZeroMQ Guide's zhelpers.h.
"""

import binascii
import os
from random import randint

import zmq

# fix ROUTER/DEALER aliases, missing from pyzmq < 2.1.9
if not hasattr(zmq, 'ROUTER'):
    zmq.ROUTER = zmq.XREP
if not hasattr(zmq, 'DEALER'):
    zmq.DEALER = zmq.XREQ


# Receives all message parts from socket, prints neatly
def dump(msg_or_socket):
    if isinstance(msg_or_socket, zmq.Socket):
        # it's a socket, call on current message
        return dump(msg_or_socket.recv_multipart())
    else:
        msg = msg_or_socket
    print("----------------------------------------")
    for part in msg:
        print("[%03d]" % len(part), end=' ')
        is_text = True
        for c in part:
            if c < 32 or c > 128:
                is_text = False
                break
        if is_text:
            # print only if ascii text
            print(part.decode())
        else:
            # not text, print hex
            print(binascii.hexlify(part))


# Set simple random printable identity on socket
def set_id(zsocket):
    identity = "%04x-%04x" % (randint(0, 0x10000), randint(0, 0x10000))
    zsocket.setsockopt(zmq.IDENTITY, identity)


def socket_set_hwm(socket, hwm=-1):
    """libzmq 2/3 compatible sethwm"""
    try:
        socket.sndhwm = socket.rcvhwm = hwm
    except AttributeError:
        socket.hwm = hwm


def zpipe(ctx):
    """build inproc pipe for talking to threads

    mimic pipe used in czmq zthread_fork.

    Returns a pair of PAIRs connected via inproc
    """
    a = ctx.socket(zmq.PAIR)
    a.linger = 0
    b = ctx.socket(zmq.PAIR)
    b.linger = 0
    socket_set_hwm(a, 1)
    socket_set_hwm(b, 1)
    iface = "inproc://%s" % binascii.hexlify(os.urandom(8)).decode()
    a.bind(iface)
    b.connect(iface)
    return a, b
