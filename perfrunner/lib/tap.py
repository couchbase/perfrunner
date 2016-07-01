import random
import socket
import string
import struct

from perfrunner.lib.mc_bin_client import memcacheConstants as Constants
from perfrunner.lib.mc_bin_client import MemcachedClient


class Batch(object):

    def __init__(self, source):
        self.source = source
        self.msgs = []
        self.bytes = 0
        self.adjust_size = 0

    def append(self, msg, num_bytes):
        self.msgs.append(msg)
        self.bytes = self.bytes + num_bytes

    def size(self):
        return len(self.msgs)

    def msg(self, i):
        return self.msgs[i]


class TAP(object):

    BATCH_MAX_SIZE = 1000.0
    BATCH_MAX_BYTES = 400000.0
    RECV_MIN_BYTES = 4096

    def __init__(self, host='127.0.0.1', port=11210, bucket='default',
                 password=''):
        self.tap_done = False
        self.tap_name = ''.join(random.sample(string.letters, 16))
        self.ack_last = False
        self.cmd_last = None
        self.num_msg = 0

        self.tap_conn = self.get_tap_conn(host, port, bucket, password)

    def provide_batch(self):
        batch = Batch(self)

        try:
            while (not self.tap_done and
                   batch.size() < self.BATCH_MAX_SIZE and
                   batch.bytes < self.BATCH_MAX_BYTES):

                rv, cmd, vbucket_id, key, flg, exp, cas, meta, val, \
                    opaque, need_ack = self.read_tap_conn(self.tap_conn)

                if rv != 0:
                    self.tap_done = True
                    return rv, batch

                if cmd == Constants.CMD_TAP_MUTATION:
                    if not False:
                        msg = (cmd, vbucket_id, key, flg, exp, cas, meta, val)
                        batch.append(msg, len(val))
                        self.num_msg += 1
                elif cmd == Constants.CMD_TAP_OPAQUE:
                    pass
                else:
                    return 'Unexpected TAP message'

                if need_ack:
                    self.ack_last = True
                    self.tap_conn._sendMsg(cmd, '', '', opaque, vbucketId=0,
                                           fmt=Constants.RES_PKT_FMT,
                                           magic=Constants.RES_MAGIC_BYTE)
                    return 0, batch

                self.ack_last = False
                self.cmd_last = cmd

        except EOFError:
            if batch.size() <= 0 and self.ack_last:
                self.tap_done = True

        if batch.size() <= 0:
            return 0, None

        return 0, batch

    def get_tap_conn(self, host, port, bucket, password):
        tap_conn = MemcachedClient(host, port)
        tap_conn.sasl_auth_cram_md5(bucket, password)
        tap_conn.tap_fix_flag_byteorder = True

        tap_opts = {Constants.TAP_FLAG_DUMP: '',
                    Constants.TAP_FLAG_SUPPORT_ACK: '',
                    Constants.TAP_FLAG_TAP_FIX_FLAG_BYTEORDER: ''}

        ext, val = self.encode_tap_connect_opts(tap_opts)
        tap_conn._sendCmd(Constants.CMD_TAP_CONNECT, self.tap_name, val, 0, ext)
        return tap_conn

    def read_tap_conn(self, tap_conn):
        buf, cmd, vbucket_id, opaque, cas, keylen, extlen, data, datalen = \
            self.recv_msg(tap_conn.s, getattr(tap_conn, 'buf', ''))
        tap_conn.buf = buf

        rv = 0
        metalen = flags = flg = exp = 0
        meta = key = val = ''
        need_ack = False

        if data:
            ext = data[0:extlen]
            if extlen == 8:
                metalen, flags, ttl = \
                    struct.unpack(Constants.TAP_GENERAL_PKT_FMT, ext)
            elif extlen == 16:
                metalen, flags, ttl, flg, exp = \
                    struct.unpack(Constants.TAP_MUTATION_PKT_FMT, ext)
                if not tap_conn.tap_fix_flag_byteorder:
                    flg = socket.ntohl(flg)
            need_ack = flags & Constants.TAP_FLAG_ACK
            meta_start = extlen
            key_start = meta_start + metalen
            val_start = key_start + keylen

            meta = data[meta_start:key_start]
            key = data[key_start:val_start]
            val = data[val_start:]
        elif datalen:
            rv = 'Error: could not read full TAP message body'

        return \
            rv, cmd, vbucket_id, key, flg, exp, cas, meta, val, opaque, need_ack

    def recv_msg(self, sock, buf):
        pkt, buf = self.recv(sock, Constants.MIN_RECV_PACKET, buf)
        if not pkt:
            raise EOFError()
        magic, cmd, keylen, extlen, dtype, errcode, datalen, opaque, cas = \
            struct.unpack(Constants.REQ_PKT_FMT, pkt)
        data, buf = self.recv(sock, datalen, buf)
        return buf, cmd, errcode, opaque, cas, keylen, extlen, data, datalen

    def recv(self, skt, nbytes, buf):
        recv_arr = [buf]
        recv_tot = len(buf)

        while recv_tot < nbytes:
            data = skt.recv(max(nbytes - len(buf), self.RECV_MIN_BYTES))
            if not data:
                return None, ''
            recv_arr.append(data)
            recv_tot += len(data)

        joined = ''.join(recv_arr)

        return joined[:nbytes], joined[nbytes:]

    def encode_tap_connect_opts(self, opts):
        header = 0
        val = []
        for op in sorted(opts.keys()):
            header |= op
            val.append(opts[op])
        return struct.pack('>I', header), ''.join(val)


def main():
    tap = TAP()

    while True:
        rv, batch = tap.provide_batch()
        if not batch or rv:
            break


if __name__ == '__main__':
    main()
