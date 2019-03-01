#!/usr/bin/env python
from __future__ import print_function
import base64
import json
import os
import random
import sys
import tempfile
import time
import unittest
import copy
import io

#import bson
import yaml
import msgpack
import redis

import format_pb2 as fpb
from google.protobuf import json_format
from google.protobuf.internal.decoder import _DecodeVarint
from google.protobuf.internal.encoder import _VarintEncoder

random.seed(0)

def bits(sz):
    return base64.b64encode(bytearray(random.getrandbits(8) for x in range(sz)))

def randkey():
    sz = random.randrange(32, 256)
    return base64.b64encode(bits(sz)).decode("ascii")

def randval():
    sz = random.randrange(1024, 4096)
    return base64.b64encode(bits(sz)).decode("ascii")

def bulkdata():
    sz = random.randrange(2**20, 2**23)
    return base64.b64encode(bits(sz)).decode("ascii")

def randset():
    sz = random.randrange(1024, 8192)
    return [randkey() for x in range(sz)]

def randmap():
    sz = random.randrange(256, 1024)
    return {randkey(): randval() for x in range(sz)}

def thing():
    _thing = {}
    _thing['pairs'] = randmap()
    _thing['idx1'] = randset()
    _thing['idx2'] = randset()
    _thing['idx3'] = randset()
    _thing['data'] = bulkdata()
    return _thing

def proto_dump(o, f):
    for x in o:
        thing = fpb.Thing(**x)
        encoder = _VarintEncoder()
        encoder(f.write, thing.ByteSize())
        f.write(thing.SerializeToString())

def proto_dumps(o):
    f = io.BytesIO()
    proto_dump(o, f)
    return f.getvalue()

def proto_load(f):
    out = []
    while True:
        buf = f.read(4)
        if not buf:
            break

        size, pos = _DecodeVarint(buf, 0)
        buf = buf[pos:] + f.read(size - 4 + pos)

        out.append(fpb.Thing.FromString(buf))

    return out

def proto_loads(o):
    f = io.BytesIO()
    f.write(o)
    f.seek(0)
    return proto_load(f)

def proto_cmp(o1, o2):
    out = list(map(json_format.MessageToDict, o2))
    tc = unittest.TestCase('__init__')
    tc.assertEqual(out, o1)


dumpers = {
    #'yaml': lambda o: yaml.dump_all(o, Dumper=yaml.CDumper).encode('ascii'),
    'json': lambda o: json.dumps(o).encode('ascii'),
    #'bson': lambda o, s: s.write(bson.encode(o)),
    'mp': msgpack.dumps,
    'pb': proto_dumps,
}
loaders = {
    #'yaml': lambda x: list(yaml.load_all(x, Loader=yaml.CLoader)),
    'json': json.loads,
    #'bson': lambda o, s: s.write(bson.dumps(o)),
    'mp': lambda o: msgpack.loads(o, raw=False),
    'pb': proto_loads,
}
testers = {
    'yaml': lambda o1, o2: unittest.TestCase('__init__').assertEqual(o1, o2),
    'json': lambda o1, o2: unittest.TestCase('__init__').assertEqual(o1, o2),
    #'bson': lambda o, s: s.write(bson.(o)),
    'mp': lambda o1, o2: unittest.TestCase('__init__').assertEqual(o1, o2),
    'pb': proto_cmp,
}

def test_thing():
    _thing = {}
    _thing['pairs'] = {"foo": "bar", "foo2": "bar2"}
    _thing['idx1'] = [u"dummy", "OK"]
    _thing['idx2'] = ["test"]
    _thing['idx3'] = ["thing"]
    _thing['data'] = u"some raw data"
    return _thing

def test():
    dat = tempfile.mkdtemp()
    print("[TEST] data_dir: ", dat)

    for name in dumpers:
        f = os.path.join(dat, name)
        os.makedirs(f)

    i = "test"
    collection = [test_thing(), test_thing()]
    for nam, dumper in dumpers.items():
        with open(os.path.join(dat, nam, str(i)), "wb") as fo:
            copied = copy.deepcopy(collection)
            fo.write(dumper(copied))

    for nam, loader in loaders.items():
        with open(os.path.join(dat, nam, str(i)), "rb") as fo:
            loaded = loader(fo.read())

        testers[nam](collection, loaded)

def run(n=1, r=None):
    collection = [thing() for i in range(n)]
    dat_dir = tempfile.mkdtemp()
    print("[RUN] datadir: ", dat_dir)

    start = time.time()
    _copy = copy.deepcopy(collection)
    diff = time.time() - start
    print("Baseline {} (copy) in {}".format(n, diff))

    for name in dumpers:
        os.makedirs(os.path.join(dat_dir, name))

    for nam, dumper in dumpers.items():
        pth = os.path.join(dat_dir, nam, str(n))
        with open(pth, "wb") as fo:
            encoded = dumper(collection)
            fo.write(encoded)
            if r:
                r.set("{}/collection{}".format(nam,n), encoded)


    for nam, loader in loaders.items():
        pth = os.path.join(dat_dir, nam, str(n))
        start = time.time()
        with open(pth, "rb") as fo:
            loaded = loader(fo.read())
            print(loaded[0].__class__)
        diff = time.time() - start
        print("Loaded {} from {} in {}".format(nam, pth, diff))
        if r:
            k = "{}/collection{}".format(nam,n)
            start = time.time()
            encoded = r.get(k)
            decoded = loader(encoded)
            diff = time.time() - start
            print("Loaded {} from redis:{} in {}".format(nam, k, diff))


if __name__ == "__main__":
    r = redis.Redis(host='localhost', port=6379, db=0)
    test()
    run(10, r=r)
