#coding: utf-8

import signal
import collections
import multiprocessing

def _init_pool():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    return

def run_worker(runnable, args, channel, multi=True):
    return Worker(runnable, args, channel=channel, multi=multi)._run()

class Worker(object):
    def __init__(self, runnable=None, args=(), channel=None, multi=True):
        self.runnable = runnable
        self.args = args
        self.multi = multi
        self.channel = channel

        return

    def _run(self):
        if self.multi:
            self.runnable(*self.args)
        else:
            self.runnable(self.args)

        self.channel.put('completed')

        return

class MultiprocessPool(object):
    def __init__(self, nProcess=None):
        self.nProcess = multiprocessing.cpu_count()*2 if nProcess is None else nProcess
        self.running = False
        m = multiprocessing.Manager()
        q = m.Queue()
        self.channel = q

        return

    def __enter__(self):
        self.pool = multiprocessing.Pool(self.nProcess, _init_pool)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.close()
        self.pool.join()
        return

    def is_running(self):
        return self.running

    def _run(self):
        self.running = True
        completed = 0

        while True:
            msg = self.channel.get()
            if msg == 'completed':
                completed += 1
            if completed >= self.nProcess:
                break

        return

    def map(self, runnable, args):
        if self.is_running():return
        assert len(args) > 0
        assert isinstance(args, collections.Iterable)

        self.nProcess = len(args)

        for arg in args:
            self.pool.apply_async(run_worker, args=(runnable, arg, self.channel, False, ))

        self._run()

        return

    def run(self, runnable, args=(), q=None):
        if self.is_running():return

        for i in range(self.nProcess):
            self.pool.apply_async(run_worker, args=(runnable, args, self.channel, ))

        self._run()

        return


if __name__ == '__main__':
    import time
    import os

    def test_run(x):
        print 'testing run function .....', x, os.getpid()
        time.sleep(1)

        return

    with MultiprocessPool(2) as mp:
        mp.run(test_run, (2, ))

    def test_map(x):
        print 'testing map function .....', x, os.getpid()
        time.sleep(1)

        return

    with MultiprocessPool(3) as mp:
        mp.map(test_map, [1, 2, 3])
