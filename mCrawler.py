import sys
from datetime import datetime

# multi-thread
import gevent
from gevent import monkey
from gevent.queue import Queue

from bs4 import BeautifulSoup
from Queue import Queue, Empty

from pymongo import MongoClient

# monkey.patch_all()

# import threading
import urllib2

visited = set()
tasks = Queue()
threadNumber = 5

# Create a mongodb connection
client = MongoClient()
client = MongoClient('localhost', 27017)

# Getting a database
db = client.url_db

# Getting a collection
collection = db.url_collection

cursor = db.url_collection.find()

for document in cursor:
    print(document)


def init():
    seedUrl = 'https://www.dmoz.org'
    host = 'www.dmoz.org'
    parseURL(seedUrl, host, '')


def parseURL(href, host, root):

    html_doc = urllib2.urlopen(href).read()
    # print(html_doc)

    soup = BeautifulSoup(html_doc, 'html.parser')
    root = ''

    for link in soup.find_all('a'):
        # print(link.get('href'))
        try:
            href = link['href']
        except KeyError:
            continue

        if not href.startswith(('http://', 'https://', 'ftp://')):
            href = 'http://%s%s' % (host, href)
        # print href
        # print 'host = ', host
        print 'href = ', href

        # if not href.startswith('http://%s%s' % (host, root)):
        #     continue
        
        db.collection.insert_one(
            {
                "urls": {
                    "href": href
                    "host": host
                    "root": root
                },

                "date": datetime.strptime("2014-10-01", "%Y-%m-%d"),
                "count": 1
            }
        )

        # if href not in visited:
        #     visited.add(href)
        #     tasks.put(href)
        #     print href

def worker(threadNumber):
    while not tasks.empty():
        print 'worker ', threadNumber
        try:
            url = tasks.get_nowait()
            # print 'url: ', url
        except Empty:
            print 'Task queue is empty!'
        
        url_list = url.split('/', 3)
        
        host = url_list[2]
        root = '/'+url_list[3]

        # print 'host = ', host
        # print 'root = ', root
        # parseURL(url, host, root)

        gevent.sleep(0)


# def boss():
#     print 'I am boss'


if __name__ == '__main__':

    init()

    for i in range(0,5):
        jobs = []
        jobs.append(gevent.spawn(worker, i))

    gevent.joinall(jobs)

