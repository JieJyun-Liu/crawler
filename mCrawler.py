import sys
import datetime
from time import time

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
import re
import json                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     

tasks = Queue()

# Create a mongodb connection
client = MongoClient()
#client = MongoClient('localhost', 27017)

# Getting a database
db = client.url_db

# Getting a collection
collection = db.url_collection

start_time = time()

def init():
    seedUrl = 'http://www.seedquest.com/'
    host = 'www.seedquest.com'
    parseURL(seedUrl, host, '')

def writeURLCount():
   urlCount = db.collection.count()
   
   ts = time.time()
   curtime = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
   
   writeString = "\n"+ curtime + " " + str(urlCount)
   f = open('url_counts.txt', 'a')
   f.write(writeString)
   f.close()

def parseURL(href, host, root):
   # global url_count
   # timer = threading.Timer(2, writeURLCount, (href, host, root)).start()
    html_doc = urllib2.urlopen(href).read()
    # print(html_doc)

    soup = BeautifulSoup(html_doc, 'html.parser')
    root = ''
    patterns = ['.js', '.css', '.png', '.jpg']
    danger = False

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

        for pattern in patterns:
            if re.search(pattern, href):
                danger = True

        # if not href.startswith('http://%s%s' % (host, root)):
        #     continue
        data = {'url': href }
        # print data
        # If value not exist
        if danger == False and db.collection.find(data).count() == 0:
            tasks.put(href)
            db.collection.update(data, data, upsert=True);
            print href
        
def worker(threadNumber):
    
    while not tasks.empty():
        print 'worker ', threadNumber
        
        try:
            pass_time = time()
            interval = pass_time - start_time

            if interval > 600:
                start_time = time()
                writeURLCount()
        except:
            pass

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

if __name__ == '__main__':

    init()

    for i in range(0,8):
        jobs = []
        jobs.append(gevent.spawn(worker, i))

    gevent.joinall(jobs)

