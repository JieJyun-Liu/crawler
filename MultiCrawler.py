# -*- coding: utf-8 -*-

import sys
from urllib import urlopen
from bs4 import BeautifulSoup
from Queue import Queue, Empty
from threading import Thread


# https://www.dmoz.org/
visited = set()
queue = Queue()

def get_parser(host, root, charset):
    # queue.put_nowait('https://www.dmoz.org/');

    def parse():
        try:
            while True:
                url = queue.get_nowait()
                try:
                    content = urlopen(url).read().decode(charset)
                except UnicodeDecodeError:
                    print 'Unicode Decode Error!'
                    continue
                for link in BeautifulSoup(content).findAll('a'):
                    try:
                        href = link['href']
                    except KeyError:
                        continue
                    if not href.startswith('http://'):
                        href = 'http://%s%s' % (host, href)
                    if not href.startswith('http://%s%s' % (host, root)):
                        continue
                    if href not in visited:
                        visited.add(href)
                        queue.put(href)
                        print href
        except Empty:
            pass

    return parse

if __name__ == '__main__':
    # host, root, charset = sys.argv[1:], sys.argv[1:], sys.argv[1:]
    host, root, charset = [], [], []

    queue.put('https://www.dmoz.org')
    parser = get_parser(host, root, charset)
    queue.put('http://%s%s' % (host, root))
    print queue

    workers = []
    for i in range(5):
        worker = Thread(target=parser)
        worker.start()
        workers.append(worker)
    for worker in workers:
        worker.join()

