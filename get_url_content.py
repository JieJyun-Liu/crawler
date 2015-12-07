''' 

* get_url_content.py
    -> Putting data out of HTML via URL.

'''


from pymongo import MongoClient
from bs4 import BeautifulSoup
from bson.objectid import ObjectId

import urllib2


# MongoDB Setting
client = MongoClient()
db = client.url_db
collection = db.url_collection

urls = []
error_urls = open('err_urls.txt', 'a')

# Get URL data from database.
def get_url_data(data_limit):

	global urls
	global error_urls

	
	url_count = 10

	hex_start = "5636228b95db3d99a0a9b6af"

	hex_ind = hex_start
	print hex_ind
	
	for index in range(url_count):

		objID = ObjectId(hex_ind)

		# Query url by objectID
		urls = db.collection.find({ "_id": objID })
		parseURLs(urls)

		hex_ind = long(hex_ind, 16)
		print hex_ind
		hex_ind = hex_ind + 1
		hex_ind = hex(hex_ind).rstrip("L").lstrip("0x") or "0"
		print hex_ind

	# urls =  db.collection.find().limit(10)

def parseURLs(urls):	

	for url in urls:
		print url['url']

		try:
			# Open html source file via url.
			html_doc = urllib2.urlopen(url['url']).read()
			soup = BeautifulSoup(html_doc, 'html.parser')
			
			# Kill all script and style elements.
			for script in soup(["script", "style"]):
			    script.extract()
			
			text = soup.get_text()

			# Break into lines and remove leading and trailing space on each
			lines = (line.strip() for line in text.splitlines())
			# Break multi-headlines into a line each
			chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
			# Drop blank lines.
			text = '\n'.join(chunk for chunk in chunks if chunk)

			# print(text.encode('utf-8'))
			# error_urls = open('err_urls.txt', 'w')
			

		except:
			write_string = url['url'] + '\n';
			error_urls.write(write_string)
			print "Error URL: " + url['url']




if __name__ == '__main__':

	data_limit = 100
	get_url_data(data_limit)
	error_urls.close()
