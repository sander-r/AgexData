# -*- coding: utf-8 -*-
from __future__ import print_function
import requests
import csv
import sys
import time
from threading import Thread, Event, Lock
from lxml import html as html_parser
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from optparse import OptionParser


progname = __file__.split('.')[0]
usage = "%prog [OPTIONS]"
description = "Find LinkedIn.com company links"
optParser = OptionParser(usage=usage, version='1.0',
                         description=description, prog=progname)

optParser.add_option("-i", "--input",
                     action="store", dest="input",
                     type="string", default="urls.txt",
                     help="Input txt file, default is urls.txt")

optParser.add_option("-o", "--output",
                     action="store", dest="output",
                     type="string", default="urls.csv",
                     help="Output csv file, default is urls.csv")

optParser.add_option("-t", "--threads",
                     action="store", dest="threads",
                     type="int", default=100,
                     help="Number of working threads, default is 100")

optParser.add_option("-d", "--max_depth",
                     action="store", dest="max_depth",
                     type="int", default=5,
                     help="Size of max_depth, default is 5")

optParser.add_option("-p", "--max_pages",
                     action="store", dest="max_pages",
                     type="int", default=50,
                     help="Count of pages load, default is 50")

optParser.add_option("-f", "--offset",
                     action="store", dest="offset",
                     type="int", default=0,
                     help="Offset in input file, default is 0")


(opts, args) = optParser.parse_args()

headers = {'User-Agent': 'Mozilla/5.0', 'X-Requested-With': 'XMLHttpRequest'}
lock = Lock()

count = 0


def writeCSV(data):
    ofile = open(opts.output, "a", encoding='utf-8')
    writer = csv.writer(ofile, delimiter=';', quotechar='"',
                        quoting=csv.QUOTE_ALL)
    writer.writerow(data)
    ofile.close()


def writeNFF(data):
    ofile = open('notfound.txt', "a")
    ofile.write("%s\n" % data)
    ofile.close()


def meta_redirect(html):
    result = html.xpath("//meta[translate(@http-equiv, 'REFSH',"
                        " 'refsh') = 'refresh']/@content")
    if result:
        wait, text = result[0].split(";")
        if text.strip().lower().startswith("url="):
            url = text[5:]
            return url
    return None


def parse_site(domain, url, depth=0, links=[]):
    curl = urlparse(url)
    if curl.path not in links:
        links.append(curl.path)
    try:
        response = requests.get(url, headers=headers, timeout=7)
    except Exception:
        return None, None
    if not response.ok:
        return None, None
    data = response.text
    html = None
    try:
        html = html_parser.fromstring(data)
    except Exception:
        return None, None
    results = html.xpath("//a[contains(@href,'linkedin.com/company')]")
    if results:
        for a in results:
            return a.get('href'), url
    else:
        if depth >= opts.max_depth:
            return None, None
        soup = BeautifulSoup(data, 'lxml')
        refs = soup.find_all('a')
        if not len(refs):
            redirect = meta_redirect(html)
            if redirect:
                rdomain = str(urlparse(redirect).netloc)
                return parse_site(rdomain, redirect, depth=0, links=[])
        for link in refs:
            newurl = link.get('href')
            purl = urlparse(newurl)
            if not purl.path or purl.scheme == 'mailto' or purl.path == '/':
                continue
            elif purl.netloc and domain != purl.netloc:
                continue
            else:
                newlink = "%s://%s%s%s" % (curl.scheme,
                                           curl.netloc,
                                           curl.path,
                                           purl.path)
                if purl.path not in links:
                    links.append(purl.path)
                    href, furl = parse_site(domain, newlink, depth=depth+1,
                                            links=links)
                    if href:
                        return href, furl
                    if len(links) >= opts.max_pages:
                        return None, None
            time.sleep(0.001)
    return None, None


class searchLinkedInThread(Thread):

    def __init__(self, event, url):
        self.url = url
        self.event = event
        Thread.__init__(self)

    def run(self):
        global count
        global connects_pool
        global lock
        domain = str(urlparse(self.url).netloc)
        href, curl = parse_site(domain, self.url, depth=0, links=[])
        lock.acquire()
        try:
            if href:
                writeCSV([self.url, curl, href])
            else:
                writeNFF(self.url)
            count += 1
            c = connects_pool.workers_count()
            sys.stdout.write('\r')
            sys.stdout.write("workers [%d], urls done[%d]" % (c, count))
            sys.stdout.flush()
        finally:
            lock.release()
        self.event.set()


def slot_available(thread):
    if thread is None:
        return True
    else:
        return not thread.is_alive()


class ThreadPool:

    MAX_THREADS = opts.threads

    def __init__(self):
        self.pool = [None] * self.MAX_THREADS
        self.event = Event()

    def is_available_slots(self):
        return any(slot_available(thread) for thread in self.pool)

    def workers_count(self):
        count = 0
        for thread in self.pool:
            if thread is not None and thread.is_alive():
                count += 1
        return count

    def dispatch(self, thread):
        if not self.is_available_slots():
            self.event.wait()
            self.event.clear()
        for slot_id in range(self.MAX_THREADS):
            if slot_available(self.pool[slot_id]):
                self.pool[slot_id] = thread
                thread.start()
                break

connects_pool = ThreadPool()
i = 0

try:
    urllist = None
    with open(opts.input) as urllist:
        for line in urllist:
            if opts.offset > i:
                i += 1
                continue
            url = line.strip('\n')
            while not connects_pool.is_available_slots():
                time.sleep(0.001)
            if connects_pool.is_available_slots():
                pt = searchLinkedInThread(connects_pool.event, url)
                connects_pool.dispatch(pt)
            time.sleep(0.001)
            i += 1
    while connects_pool.workers_count():
        lock.acquire()
        try:
            sys.stdout.write('\r')
            c = connects_pool.workers_count()
            sys.stdout.write("workers [%d], urls done[%d]" % (c, count))
            sys.stdout.flush()
        finally:
            lock.release()
        time.sleep(0.001)
    print()
except KeyboardInterrupt:
    print("\nWaiting threads finished...\n")
    while connects_pool.workers_count():
        lock.acquire()
        try:
            sys.stdout.write('\r')
            c = connects_pool.workers_count()
            sys.stdout.write("workers [%d], urls done[%d]" % (c, count))
            sys.stdout.flush()
        finally:
            lock.release()
        time.sleep(0.3)
    print()
    print("Offset number is %d" % i)
finally:
    try:
        urllist.close()
    except:
        pass
