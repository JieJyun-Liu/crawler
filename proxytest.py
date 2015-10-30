import urllib


r = urllib.urlopen('http://www.google.com')
print r.read()