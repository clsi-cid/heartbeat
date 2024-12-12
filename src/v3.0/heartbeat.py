import sys
import os
import datetime
import threading
import queue
import time
import socket
import select
import json
import psutil
import struct
import traceback

from definitions import Constant
from utils.my_logger import MyLogger, LogMixin

log = MyLogger()

HEARTBEAT_PORT          = 5066
HEARTBEAT_INTERVAL_SEC  = 30
CMD_LIST_PVS            = 0x4DA3B921

class CMD(object):
    REQUEST = 'cmd:request'

class MyLogger(object):

    def __init__(self, debug_level=None):
        self._debug_level = debug_level
        self._call_level = 1

    def emit(self, label, arg):
        file, function, line = self.get_details()

        # t = "{:%H:%M:%S:%f}".format(datetime.datetime.now())
        # Add date to logs
        t = "{:%m-%d %H:%M:%S:%f}".format(datetime.datetime.now())

        if isinstance(arg, str):
            print("%s: %s %s (%s:%d): %s" %    (t, label, function, file, line, arg))
        elif callable(arg):
            print("%s: %s %s (%s:%d): %s" %    (t, label, function, file, line, arg()))
        else:
            print("%s: %s %s (%s:%d): logger got -> type(arg): %s" %  (t, label, function, file, line, type(arg)))

    def dbg(self, level, arg):
        if self._debug_level is not None and level <= self._debug_level:
            self.emit("DBG(%d) " % level, arg)

    def log(self, arg=None):
        self.emit("LOG(1) ", arg)

    def info(self, arg=None):
        self.emit("LOG(1) ", arg)

    def error(self, arg=None):
        self.emit("ERR(1) ", arg)

    def err(self, arg=None):
        self.emit("ERR(1) ", arg)

    def warn(self, arg=None):
        self.emit("WARN(1)", arg)

    def call(self, arg="called"):
        self.emit("CALL(1)", arg)

    def get_details(self):

        # for item in x:
        #     print item

        try:
            x = traceback.extract_stack()

            source_item = x[-4]
            # print "This is the source item", source_item

            file = source_item[0]
            line = source_item[1]
            function = source_item[2]

        except Exception as err:
            print("*"*80)
            print("*"*80)
            print("MyLogger.get_details() failed:", repr(err))
            print("*"*80)
            print("*"*80)

            file = 'unknown'
            function = 'unknown'
            line = 0

        return (file, function, line)


# class Heartbeat(LogMixin):
class Heartbeat(MyLogger):
    """
    This is a python version of the EPICS heartbeat.
    It is version "3.0" (this must not change)
    This code might require tweakinf for a particular application
    """
    def __init__(self, debug_level=None):

        # If using external logger, replace call to super().__init__()
        # with call to init_LogMixin
        super().__init__()
        # self.LogMixin_register(log, level=0)

        arg_str = ' '.join(sys.argv)

        self._debug_level = debug_level
        self._img = sys.executable
        self._cmd = arg_str
        self._cwd = os.getcwd()

        self.dbg(0, lambda: "CMD: %s %s" % (self._img, self._cmd))
        self.dbg(0, lambda: "CWD: %s" % self._cwd)

        self._terminate = False
        self._thread_rx = threading.Timer(0, self.worker_rx)
        self._thread_tx = threading.Timer(0, self.worker_tx)
        self._queue = queue.Queue()

        self._seq = 0
        self._launch_time = int(time.time())

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # self._sock.bind(('', HEARTBEAT_PORT))

        self._socket_tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket_tcp.bind(("" , 0))
        self._socket_tcp.listen(1)
        self._tcp_port = self._socket_tcp.getsockname()[1]

        self._tcp_port_str = '%d' % self._tcp_port
        self._pid_str = '%d' % os.getpid()

        # print("TCP_PORT: %d" % self._tcp_port)

        self._bcast_addr_list = self.get_broadcast_addrs()

    def get_broadcast_addrs(self):

        result = []
        addrs = psutil.net_if_addrs()
        for name, value in addrs.items():
            self.dbg(0, lambda: "interface: %s" % name)
            for item in value:
                # print("%s ---> %s" % (type(item), repr(item)))
                # print("item[0]: %s" % item[0])

                if not item.family == socket.AddressFamily.AF_INET:
                    continue

                broadcast_addr = item.broadcast
                if broadcast_addr is None:
                    continue

                if not broadcast_addr.startswith('10.'):
                    self.dbg(0, lambda: "skipping broadcast address: %s" % repr(broadcast_addr))
                    continue

                self.dbg(0, lambda: "adding broadcast address: %s" % repr(broadcast_addr))
                result.append(broadcast_addr)

        return list(set(result))

    def start(self):
        self.dbg(0, lambda: "called")
        self._thread_rx.start()
        self._thread_tx.start()

    def stop(self):
        self.dbg(0, lambda: "called")
        self._terminate = True
        self._thread_rx.join()
        self._thread_tx.join()

        self._sock.close()
        self._socket_tcp.close()

    def worker_rx(self):

        while not self._terminate:
            self.dbg(2, lambda: "running")

            ready = select.select([self._sock], [], [], 2)
            if not self._sock in ready[0]:
                continue

            data, addr_rx = self._sock.recvfrom(100000)
            self._queue.put_nowait((CMD.REQUEST, {'data' :data, 'addr_rx' : addr_rx}))

        self.dbg(0, "terminating")

    def worker_tx(self):

        next_heartbeat_time = time.time() + 1

        while not self._terminate:
            self.dbg(2, lambda: "running")

            if time.time() > next_heartbeat_time:
                self.send_heartbeat()
                next_heartbeat_time += HEARTBEAT_INTERVAL_SEC

            try:
                item = self._queue.get(block=True, timeout=2.0)

            except queue.Empty:
                continue

            cmd = item[0]
            data = item[1]

            if cmd == CMD.REQUEST:
                self.process_request(data)

        self.dbg(0, "terminating")

    def send_heartbeat(self):

        self.dbg(2, lambda: "called")
        cur_time = int(time.time())

        heartbeat = {
            'seq'       : '%d' % self._seq,
            'img'       : self._img,
            'stcmd'     : self._cmd,
            'cwd'       : self._cwd,
            'pid'       : self._pid_str,
            'up'        : '%d' % (cur_time - self._launch_time),
            'cur'       : '%d' % cur_time,
            'sp'        :  self._tcp_port_str,
            'ver'       : '3.0'
        }

        heartbeat_str = json.dumps(heartbeat)
        msg = bytes(heartbeat_str, encoding='utf8')
        # print("heartbeat str: %s %s" % (heartbeat_str, type(heartbeat_str)))

        for bcast_addr in self._bcast_addr_list:
            self.dbg(1, lambda: "sending heartbeat to: %s" % repr(bcast_addr))
            self._sock.sendto(msg, (bcast_addr, HEARTBEAT_PORT))

        self._seq += 1

    def process_request(self, data_in):

        data = data_in.get('data')
        addr_rx = data_in.get('addr_rx')

        self.dbg(2, lambda: "called; addr_rx: %s" % repr(addr_rx))

        # print_binary(data)

        try:
            c = struct.unpack_from("IIII", data)
            cmd     = socket.ntohl(c[0])
            seq     = socket.ntohl(c[1])
            offset  = socket.ntohl(c[2])
            handle  = socket.ntohl(c[3])

            # print("command: %0x" % cmd)
            # print("seq:     %0x" % seq)
            # print("offset:  %0x" % offset)
            # print("handle:  %0x" % handle)

            if cmd == CMD_LIST_PVS:
                result = self.handle_cmd_list_pvs(offset)

            else:
                self.err("command not handled: %s" % repr(cmd))
                return

            result['handle'] = handle

            self.dbg(1, lambda: "send to %s: %s" % (repr(addr_rx), result))
            msg = bytes(json.dumps(result), encoding='utf8')
            self._sock.sendto(msg, addr_rx)

        except Exception as err:
           self.err("Exception: %s" % repr(err))

    def handle_cmd_list_pvs(self, offset):

        return { 'offset' : offset, 'pvs' : [], 'total' : 0}

        # self.dbg(0, "called; offset: %d" % offset)
        # offset_in = offset
        # pvs = []
        # if offset < 100:
        #
        #     for _ in range(7):
        #         if offset >= 100:
        #             break
        #
        #         pvs.append("test_pv_%d" % offset)
        #         offset += 1
        #
        # return { 'offset' : offset_in, 'pvs' : pvs, 'total' : 100}


if __name__ == '__main__':

    heartbeat = Heartbeat(debug_level=0)
    heartbeat.start()

    while True:
        time.sleep(1)

    heartbeat.stop()
