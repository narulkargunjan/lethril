import esm
import gzip
import json
import logging
import os
import sys

from collections import deque, defaultdict
from datetime import datetime
from threading import Thread


# Module level log
log = logging.getLogger('lethril.handlers')


class BaseHandler():
    pass


class StdOutHandler(BaseHandler):
    """
    Output handler that writes raw data to standard output
    """
    def __init__(self, delimter="\n"):
        self.delimter = delimter

    def __call__(self, data):
        sys.stdout.writelines([data, self.delimter])


class RotatingFileHandler(BaseHandler):
    def __init__(self, output_dir, entries_per_file=500, delimter='\n',
                 compress=False):

        # Create output directory if it does not exist already
        try:
            os.makedirs(output_dir)
        except Exception, e:
            if e.errno !=  17:
                log.exception("Exception while creating output directory")        

        self.output_dir = output_dir
        self.delimter = delimter
        self.entries_per_file = entries_per_file
        self.compress = compress

        self._buffer = deque()
        self._count = 0


    def _flush(self, count):
        log.debug("Flushing %s of %s" % (count, len(self._buffer)))
        fname = '%s_%s' % (os.uname()[1], datetime.now().isoformat())
        fname = self.compress and ''.join([fname, '.gz']) or fname
        fpath = os.path.join(self.output_dir, fname)

        with (self.compress and gzip.open(fpath, 'wb')
              or open(fpath, 'w')) as f:
            for i in xrange(count):
                data = self._buffer.popleft()
                self._count -= 1
                f.writelines([data, self.delimter])

        log.info("Writing file : %s" % os.path.join(self.output_dir, fname))

    def __call__(self, data):
        self._buffer.append(data)
        self._count += 1
        if self._count > self.entries_per_file:
            self._flush(self.entries_per_file)


class TrackHandler(BaseHandler):
    def __init__(self, output_dir, track, entries_per_file=500,
                 delimter='\n', compress=False):


        # Create output directory if it does not exist already
        try:
            os.makedirs(output_dir)
        except Exception, e:
            if e.errno !=  17:
                log.exception("Exception while creating output directory")        

        self.output_dir = output_dir
        self.track = track
        self.entries_per_file = entries_per_file
        self.delimter = delimter
        self.compress = compress

        self._index = esm.Index()
        self._rfh = {}
        for item in track:
            item = item.lower()
            _item_dir = os.path.join(self.output_dir, item)
            _rfh = RotatingFileHandler(_item_dir,
                                       entries_per_file=entries_per_file,
                                       delimter=delimter, compress=compress)
            self._rfh[item] = _rfh
            self._index.enter(item)

        self._index.fix()

    def _flush(self):
        for name, handler in self._rfh.iteritems():
            count = len(handler._buffer)
            if count:
                log.info("Flushing %s items in %s" % (count, name))
                handler._flush(count)

    def __call__(self, data):
        parsed = json.loads(data)

        if 'in_reply_to_status_id' in parsed:
            # This is a status. Currently not handling alternate message types
            text = parsed['text'].lower().encode('utf-8')
            for span, item in self._index.query(text):
                self._rfh[item](data)


class GenericHandlerWorker(Thread):
    def __init__(self, queue, handler):
        Thread.__init__(self)
        self.queue = queue
        self.handler = handler
        self.daemon = True

    def run(self):
        log.info("Worker Intialized")
        while True:            
            item = self.queue.get()
            log.debug("Got Task")

            try:
                self.handler(item)
            except:
                log.exception("Handler Error")

            log.debug("Task Done")
            self.queue.task_done()