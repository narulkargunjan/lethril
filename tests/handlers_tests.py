from nose.tools import *
import lethril.handlers as handlers
import unittest
import shutil
import os
import json

class TestRotatingFileHandler(unittest.TestCase):

    def setUp(self):
        self.output_dir = '/tmp/lethril/test'
        self.nper = 100
        self.h = handlers.RotatingFileHandler(self.output_dir,
                                       entries_per_file=self.nper,
                                       delimter="\n")

    def tearDown(self):
        shutil.rmtree(self.output_dir)

    def test_buffer_append(self):
        for i in xrange(self.nper):
            self.h("Item-%03d" % i)

        self.assertEqual(self.nper, len(self.h._buffer),
                         msg="Buffer append failed")
        
        self.assertEqual(self.nper, self.h._count,
                         msg="Internal count does not match buffer size")


    def test_buffer_flush(self):
        remaining = 10

        for i in xrange(self.nper + remaining):
            self.h("Item-%03d" % i)

        self.assertEqual(remaining, len(self.h._buffer))

        self.assertEqual(remaining, self.h._count,
                         msg="Internal count does not match buffer size")

        # Manual Flush
        self.h._flush(remaining)

        self.assertEqual(0, len(self.h._buffer))
        self.assertEqual(0, self.h._count,
                         msg="Internal count does not match buffer size")        


    def test_flush(self):

        nfiles = 10
        remaining = 10

        for i in xrange((nfiles * self.nper) + remaining):
            self.h("Item-%03d" % i)

        files = sorted(os.listdir(self.output_dir))
        self.assertEqual(nfiles, len(files),
                         msg="Incorrect number of files written")

        count = 0
        for _file in files:
            path = os.path.join(self.output_dir, _file)
            with open(path, 'r') as f:
                items = f.readlines()
                self.assertEqual(self.nper, len(items))

                for i, item in enumerate(items):
                    item = item.strip()
                    n = i + (count * self.nper)
                    self.assertEqual("Item-%03d" % n, item)

            count += 1

        # Manual flush
        self.h._flush(remaining)
        nfiles = sorted(os.listdir(self.output_dir))
        latest = nfiles[-1]

        self.assertEqual(len(files) + 1, len(nfiles))
        self.assertTrue(latest not in files)
        with open(os.path.join(self.output_dir, latest), 'r') as f:
            items = f.readlines()
            for i, item in enumerate(items):
                item = item.strip()
                n = i + (count * self.nper)
                self.assertEqual("Item-%03d" % n, item)

class TestTrackHandler(unittest.TestCase):
    def setUp(self):
        self.output_dir = '/tmp/lethril/test'
        self.nper = 100
        self.h = handlers.TrackHandler(self.output_dir, ['A', 'B'],
                                       entries_per_file=self.nper,
                                       delimter="\n")


    def _append(self, track, n):
        for i in xrange(n):
            data = {
                'text' : "%s-%03d" % (track, i),
                'in_reply_to_status_id' : ''
            }
            self.h(json.dumps(data))

    def _check(self, track, n):
        h = self.h._rfh[track.lower()]
        print len(h._buffer)
        self.assertEquals(n, len(h._buffer), msg="Buffer append failed")
        self.assertEqual(n, h._count,
                         msg="Internal count does not match buffer size")

    def tearDown(self):
        shutil.rmtree(self.output_dir)

    def test_buffer_append(self):
        na = self.nper
        nb = self.nper
        nab = self.nper / 10

        self._append('A', na)
        self._check('A', na)

        self._append('B', nb)
        self._check('B', nb)
        self._check('A', na)

        self._append('A-B', nab)
        self._check('A', nab)
        self._check('B', nab)

        _na = self.nper / 2
        _nb = self.nper / 2
        _nab = self.nper / 3

        self._append('A', _na)
        self._check('A', nab + _na)

        self._append('B', _nb)
        self._check('B', nab + _nb)
        self._check('A', nab + _na)

        self._append('A-B', _nab)
        self._check('A', nab + _na + _nab)
        self._check('B', nab + _nb + _nab)