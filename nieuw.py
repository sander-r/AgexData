import sys
import csv
import time
import socket
import logging
import threading


logging.basicConfig(
    level=logging.DEBUG,
    format='[%(levelname)s] (%(threadName)-10s) %(message)s',
)


def read_csv_ip(filename='test.csv'):
    """Read a CSV file containing the IP addresses.

    There might be one or more IP addresses per line (';' separated)
    Return the set of them.

    """

    ip_addresses = set()
    with open(filename, 'rb') as csv_file:
        data = csv.reader(csv_file, delimiter=';', quotechar='"')
        for line in data:
            ip_addresses.update([ip for ip in line if ip])
    return ip_addresses


def write_csv_host(data, filename='result.txt'):
    """Write the results of gethostbyaddr on the IP addresses into a file.

    The results are written like: 'IP;Host;Aliases'.

    """

    with open(filename, 'wb') as csv_file:
        output = csv.writer(csv_file, delimiter=';', quotechar='"')
        for ip, lookup in data.items():
            output.writerow([ip, lookup['host'], lookup['aliases']])


class LookupThread(threading.Thread):
    def __init__(self, ip, result, pool):
        self.ip = ip
        self.result = result
        self.pool = pool
        threading.Thread.__init__(self)

    def run(self):
        self.pool.acquire()
        try:
            logging.debug('Starting')
            self.lookup(self.ip)
        finally:
            self.pool.release()
            logging.debug('Exiting')

    def lookup(self, ip):
        """Try to find the host of IP.

        Returns a dict:
            {ip: {'host': host, 'aliases': aliases}}
        If host is not found, then the dict will hold:
            {ip: {'host': 'No host found', 'aliases': ''}}

        """

        try:
            host, aliases, _ = socket.gethostbyaddr(ip)
            self.result[ip] = {
                'host': host,
                'aliases': aliases if aliases else ''
            }
        except socket.herror:
            self.result[ip] = {'host': 'No host found', 'aliases': ''}


if __name__ == '__main__':
    ip_addresses = read_csv_ip()

    start = time.time()
    result = {}

    # Limit the number of concurrent threads to 8
    pool = threading.BoundedSemaphore(8)

    lookup_threads = [LookupThread(ip, result, pool) for ip in ip_addresses]
    # Start the threads
    for t in lookup_threads:
        t.start()

    # Tell main to wait for all of them
    main_thread = threading.currentThread()
    for thread in threading.enumerate():
        if thread is main_thread:
            continue
        logging.debug('Joining %s', thread.getName())
        thread.join()

    elapsed = time.time() - start

    print '(elapsed time: %.2f seconds)' % elapsed

    write_csv_host(result)
