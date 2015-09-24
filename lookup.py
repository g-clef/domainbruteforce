#!/usr/bin/env python
#pylint: disable=C0103

# The point: start a bunch of child processes that will look up
#       subdomains of a given parent domain, based on the
#       subdomain name they get from a queue. if found, the
#       results are put onto another queue for a writer process
#       to grab and write to a file.
#
# Is looking for the parent domain to be given as an argument to
#       the app on the command line at startup.


import time
import sys
import dns.resolver
import random
import string
import MultiprocessTemplate as MT

# some constants to tweak for performance, etc.
numWorkers = 20
# note: do not set maxQueueSize > 32767 if you are running on a Mac.
maxQueueSize = 10000
inputFile = "dictionary.txt"
outfileName = "results.csv"
ns = ["8.8.8.8"]
debug = True

class LookupWorker(MT.WorkerThatTalksToWriter):
    """worker to look up names, and send any successes to another queue."""
    def __init__(self, baseDomain, nameserver, **kwargs):
        super(LookupWorker, self).__init__(**kwargs)
        if not baseDomain.startswith("."):
            self.baseDomain = "." + baseDomain
        else:
            self.baseDomain = baseDomain
        self.resolver = dns.resolver.Resolver()
        self.resolver.nameservers = nameserver
        self.resolver.search = []
        self.resolver.retry_servfail = False

    def lookupName(self, hostname):
        try:
            if debug:
                print "looking up %s" % hostname
            result = self.resolver.query(hostname, "A")
            data = result.response.to_text()
            data = data.replace("\n", " ").replace("\r", " ")
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer,
                dns.resolver.NoNameservers, dns.resolver.Timeout):
            data = None
        return data

    def handleItem(self, response):
        if response is None:
            return
        hostname = response + self.baseDomain
        resolution = self.lookupName(hostname)
        if resolution is not None:
            # don't care if this is exactly accurate, just close
            # enough to know that it has to make this worker
            # sleep for a bit.
            while self.outputqueue.qsize() > maxQueueSize:
                time.sleep(5)
            self.outputqueue.put((hostname, resolution),)

class LookupWriter(MT.WritingWorker):

    def handleItem(self, response):
        if response is not None:
            (hostname, result) = response
            self.fileHandle.write("%s,%s\n" % (hostname, result))
            self.fileHandle.flush()

def testForWildCard(baseDomain):
    """run a bunch of randomly-chosen names against this, if they all resolve
    to the same thing, stop."""
    # generate a bunch of random names, put them on the queue to be looked up
    # grab the results, if any (watch the client queue to see if results have
    # come through....how to test if they all just timed out?).
    #
    # There are two criteria for success: 1) all of the queries have to return
    # an answer, and 2) the vast majority of them have to return the same IP
    #
    if debug:
        print "testing for wildcard zone"
    resolver = dns.resolver.Resolver()
    resolver.nameservers = ns
    resolutions = []
    numTests = 20
    numAnswers = 0
    for _ in range(numTests):
        random_hostname = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(16))
        if baseDomain.startswith("."):
            hostname = random_hostname + baseDomain
        else:
            hostname = random_hostname + "." + baseDomain
        try:
            response = resolver.query(hostname, "A")
            numAnswers += 1
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer,
            dns.resolver.NoNameservers, dns.resolver.Timeout):
            response = []
        if response:
            for answer in response.response.answer:
                for item in answer.items:
                    resolutions.append(item.address)
    if debug:
        print "got %s responses out of %s tries" % (numAnswers, numTests)
    if numAnswers < 0.95 * numTests:
        return False
    uniqueNames = set(resolutions)
    if debug:
        print "have %s unique responses" % len(uniqueNames)
        print uniqueNames
    if len(uniqueNames) > 0.05 * numTests:
        return False
    return True

def readDictionary(workerQueue):
    with open(inputFile) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            while workerQueue.qsize() > maxQueueSize:
                time.sleep(5)
            if debug:
                print "putting %s on queue for lookup" % line
            workerQueue.put(line)

if __name__ == "__main__":
    baseDomain = sys.argv[1]
    if debug:
        print "starting to look up %s" % baseDomain
    workerArgs = {"baseDomain":baseDomain, "nameserver":ns}
    writerArgs = {"fileName":outfileName}
    (workers,
        writer,
        workerInputQueue,
        workerToWriterQueue) = MT.startWorkersAndWriter(LookupWorker,
                                                        workerArgs,
                                                        LookupWriter,
                                                        writerArgs,
                                                        numWorkers)
    if testForWildCard(baseDomain):
        workerToWriterQueue.put((baseDomain, "is a wildcard zone"),)
    else:
        if debug:
            print "opening input file"
        readDictionary(workerInputQueue)
    if debug:
        print "done with input file, telling workers to stop"
    MT.stopWorkersAndWriter(workers, workerInputQueue, writer,
                        workerToWriterQueue, numWorkers)
    if debug:
        print "all done."

