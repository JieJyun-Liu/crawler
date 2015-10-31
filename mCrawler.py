import sys
from time import gmtime, strftime

# multi-thread
import gevent
from gevent import monkey
from gevent.queue import Queue

from bs4 import BeautifulSoup
from Queue import Queue, Empty

from pymongo import MongoClient

# monkey.patch_all()

import threading
import urllib2

visited = set()
tasks = Queue()
threadNumber = 8
# URL Count
url_count = 0

# Create a mongodb connection
client = MongoClient()
#client = MongoClient('localhost', 27017)

# Getting a database
db = client.url_db

# Getting a collection
collection = db.url_collection



def init():
    url_count = 0
    seedUrl = 'https://www.dmoz.org'
    host = 'www.dmoz.org'
    parseURL(seedUrl, host, '')
    timer = threading.Timer(600, writeURLCount)

def writeURLCount():
    time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    writeString = time + " " + str(url_count)
    f = open('url_count.txt', 'r+')
    f.write(writeString)
    f.close()

def parseURL(href, host, root):
    global url_count
    
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
        data = { 'url':href }

        # If value not exist
        if db.collection.find(data).count() == 0:
            tasks.put(href)
            url_count = url_count + 1
            db.collection.update(data, data, upsert=True);
            print href
        
        # db.collection.insert_one(
        #     $urls {
        #         "urls": href,
        #         "date": time.strftime("%d/%m/%Y"),
        #     }
        # )

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
        
        try:
            url_list = url.split('/', 3)
        
            host = url_list[2]
            root = '/'+url_list[3]
            parseURL(url, host, root)
        
        except:
            pass

        gevent.sleep(0)


# def boss():
#     print 'I am boss'


if __name__ == '__main__':

    init()

    for i in range(0,8):
        jobs = []
        jobs.append(gevent.spawn(worker, i))

    gevent.joinall(jobs)

