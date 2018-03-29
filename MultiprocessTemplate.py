#!/usr/bin/env python
#pylint: disable=C0103

#
#
#
# Some extra notes about what's happening here:
#   * Macintosh apparently doesn't implement the signals necessary
#       to support qsize. It also has a hard-coded limit on queue
#       sizes of 32767. So, you have to monitor the size of any shared
#       queues on a Mac, and you have to do it yourself since you
#       can't use queue.qsize()
#   * the obvious way to do that is with a multiprocssing
#       Value() variable. In theory that should be multiprocess
#       safe, but the exact details mean that writing to a Value()
#       isn't multiprocess safe. For more details:
#        http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing/


import multiprocessing
import multiprocessing.queues
from queue import Empty
import os
import time

haltMessage = "__stop__"

def parentIsAlive(parentPID):
    # clever way to handle making children die if parent is killed. from:
    # http://stackoverflow.com/questions/2542610/python-daemon-doesnt-kill-its-kids
    try:
        os.kill(parentPID, 0)
    except OSError:
        # this means the parent's dead.
        return False
    else:
        return True

class ParentIsDead(Exception):
    """Signal to say that the parent process is dead, and the child
    should stop."""
    pass

class StopNow(Exception):
    """Signal to say that the parent has asked you to  stop."""
    pass

class CountingQueue(multiprocessing.queues.Queue):
    """Class to do our own counting of queue size, since Macintosh
    doesn't implement the semaphores necessary for python to make
    qsize work on its own."""
    def __init__(self):
        context = multiprocessing.get_context()
        super(CountingQueue, self).__init__(ctx=context)
        self.countlock = multiprocessing.Lock()
        self.queuesize = multiprocessing.Value("i", 0)

    def get(self, *args, **kwargs):
        """Simple get, overridden to use the internal count."""
        result = super(CountingQueue, self).get(*args, **kwargs)
        self.countlock.acquire()
        self.queuesize.value -= 1
        self.countlock.release()
        return result

    def put(self, *args, **kwargs):
        """Simple put, again, overridden to use the internal count."""
        result = super(CountingQueue, self).put(*args, **kwargs)
        self.countlock.acquire()
        self.queuesize.value += 1
        self.countlock.release()
        return result

    def qsize(self):
        """return the self-counted queue size. Note: this will not be
        exactly accurate, since the size is updated after calling the
        parent get/put. If using this is being updated a lot, it will
        likely be off by a few. Only use this for order-of-magnitude
        decisions, not precise ones."""
        return self.queuesize.value


class KillableWorker(multiprocessing.context.Process):
    """multiprocessing pattern: start a process, get items off an
    input queue, waiting for a given timeout. If the queue is
    empty at the timeout, check that the parent is still alive,
    and signal that the process should quit if the parent is dead."""
    def __init__(self, parentPID, inputqueue, timeout=5, haltMessage=haltMessage):
        super(KillableWorker, self).__init__()
        self.inputqueue = inputqueue
        self.parentPID = parentPID
        self.timeout = timeout
        self.haltMessage = haltMessage

    def getItem(self):
        """get an item off the input queue, and check to see if the
        parent is still alive, or if we've been asked to stop."""
        try:
            response = self.inputqueue.get(timeout=self.timeout)
        except Empty:
            if not parentIsAlive(self.parentPID):
                raise ParentIsDead
            else:
                response = None
        if response == self.haltMessage:
            raise StopNow
        return response

    def handleItem(self, response):
        """override this for actual workers, for what they're going to
        do with the message that comes in on the queue."""
        raise NotImplementedError()

    def amDone(self):
        """If there's anything special you want to do when the process
        stops (close logfiles, etc), override this method to do that."""
        pass

    def run(self):
        """this will be called to actually run the process. You should
        generally not override this."""
        done = False
        while not done:
            try:
                response = self.getItem()
            except (ParentIsDead, StopNow):
                done = True
                continue
            self.handleItem(response)
        self.amDone()


class WritingWorker(KillableWorker):
    """Worker that opens a file, and writes data to it.

    Override the handleItem method to actually get data & write
    to the file. the format of the data on the inputqueue isn't
    known so you have to handle that yourself."""
    def __init__(self, parentPID, inputqueue, fileName, timeout=5, haltMessage=haltMessage):
        super(WritingWorker, self).__init__(parentPID, inputqueue, timeout, haltMessage)
        self.fileName = fileName
        self.fileHandle = open(self.fileName, "w")

    def amDone(self):
        """Make sure the filehandle is closed before anything else is done."""
        self.fileHandle.close()
        super(WritingWorker, self).amDone()

    def handleItem(self, response):
        """override this for actual workers, for what they're going to
        do with the message that comes in on the queue."""
        raise NotImplementedError()


class WorkerThatTalksToWriter(KillableWorker):
    """Simple override of the default KillableWorker that takes an
    output queue also. You will still need to override handleItem to actually
    put things on the output queue."""
    def __init__(self, parentPID, inputqueue, outputqueue, timeout=5, haltMessage=haltMessage):
        super(WorkerThatTalksToWriter, self).__init__(parentPID, inputqueue, timeout, haltMessage)
        self.outputqueue = outputqueue

    def handleItem(self, response):
        """override this for actual workers, for what they're going to
        do with the message that comes in on the queue."""
        raise NotImplementedError()


def startWorkersAndWriter(workerClass, workerKWargs, writerClass, writerKWargs, numWorkers):
    parentPID = os.getpid()
    workers = []
    workerInputQueue = CountingQueue()
    workerToWriterQueue = CountingQueue()
    workerKWargs['parentPID'] = parentPID
    workerKWargs['inputqueue'] = workerInputQueue
    workerKWargs['outputqueue'] = workerToWriterQueue
    for _ in range(numWorkers):
        worker = workerClass(**workerKWargs)
        worker.daemon = True
        worker.start()
        workers.append(worker)
    writerKWargs['parentPID'] = parentPID
    writerKWargs['inputqueue'] = workerToWriterQueue
    writer = writerClass(**writerKWargs)
    writer.daemon = True
    writer.start()
    return workers, writer, workerInputQueue, workerToWriterQueue

def stopWorkersAndWriter(workers, workerInputQueue, writer, workerToWriterQueue, numWorkers, haltMessage=haltMessage):
    for _ in range(numWorkers):
        workerInputQueue.put(haltMessage)
    while workerInputQueue.qsize() > 0:
        time.sleep(5)
    workerToWriterQueue.put(haltMessage)
    while workerToWriterQueue.qsize() > 0:
        time.sleep(5)
    for worker in workers:
        if worker.is_alive():
            worker.join(timeout=5)
            if worker.is_alive():
                worker.terminate()
    if writer.is_alive():
        writer.join(timeout=5)
        if writer.is_alive():
            writer.terminate()
