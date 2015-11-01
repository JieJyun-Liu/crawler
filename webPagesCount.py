import sys
import urllib2
import datetime
import time
import threading

from pymongo import MongoClient
url_count = 0

# Create a mongodb connection
client = MongoClient()

# Getting a database
db = client.url_db

# Getting a collection
collection = db.url_collection


def init():
	try:
		while True:
			try:
				timer = threading.Timer(600, writeURLCount).start()
			except:
				pass
	except:
		pass

def writeURLCount():
	urlCount = db.collection.count()

	ts = time.time()
	curtime = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
	writeString = "\n"+ curtime + " " + str(url_count)
	f = open('url_count.txt', 'a')
	f.write(writeString)
	f.close()

if __name__ == '__main__':
    
    init()
