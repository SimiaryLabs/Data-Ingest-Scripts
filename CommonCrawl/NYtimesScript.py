import requests
import argparse
import time
import json
import StringIO
import gzip
import csv
import codecs
import re

import numpy as np

from collections import defaultdict
from collections import Counter

from bs4 import BeautifulSoup

from tempfile import TemporaryFile

import boto.sqs
from boto.sqs.message import Message

#
# Searches the Common Crawl Index for a domain.
#
def search_domain(domain):

    record_list = []
    
    print "[*] Trying target domain: %s" % domain
    
    for index in index_list:
        
        print "[*] Trying index %s" % index
        
        cc_url  = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index
        cc_url += "url=%s&matchType=domain&output=json" % domain
        
        response = requests.get(cc_url)
        
        if response.status_code == 200:
            
            records = response.content.splitlines()
            
            for record in records:
                record_list.append(json.loads(record))
            
            print "[*] Added %d results." % len(records)
            
    
    print "[*] Found a total of %d hits." % len(record_list)
    
    return record_list  


#
# Downloads a page from Common Crawl - adapted graciously from @Smerity - thanks man!
# https://gist.github.com/Smerity/56bc6f21a8adec920ebf
#
def download_page(record):

    offset, length = int(record['offset']), int(record['length'])
    offset_end = offset + length - 1

    # We'll get the file via HTTPS so we don't need to worry about S3 credentials
    # Getting the file on S3 is equivalent however - you can request a Range
    prefix = 'https://aws-publicdatasets.s3.amazonaws.com/'
    
    # We can then use the Range header to ask for just this set of bytes
    resp = requests.get(prefix + record['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})
    
    # The page is stored compressed (gzip) to save space
    # We can extract it using the GZIP library
    raw_data = StringIO.StringIO(resp.content)
    f = gzip.GzipFile(fileobj=raw_data)
    
    # What we have now is just the WARC response, formatted:
    data = f.read()
    
    response = ""
    
    if len(data):
        try:
            warc, header, response = data.strip().split('\r\n\r\n', 2)
        except:
            pass
            
    return response



def extract_external_links(html_content):

    parser = BeautifulSoup(html_content, "lxml")
        
    #links = parser.find_all("a")
    #print parser.title.string
    #print 
    paragraphs = parser.find_all("p", itemprop="articleBody")
    
    article = {}
    title = parser.find("h1", class_="articleHeadline").string
    if title is None:
    	article["title"] = "Empty"
    else:
    	article["title"] = title

    article["paragraphs"] = []
    
    for paragraph in paragraphs:
        article["paragraphs"].append(' '.join(paragraph.get_text().split()))
        #re.sub(' +',' ', paragraph.get_text())

    return article


domain = 'nytimes.com'
#index_list = ["2014-52","2015-06","2015-11","2015-14","2015-18","2015-22","2015-27"]
index_list = ["2015-27"]

record_list = search_domain(domain)
#link_list   = []

#i = 0
#j = 0

#cnt = Counter()
#html_pages = []
nytimes = []

for record in record_list:
    url = record['url'].split('/')[3]

    if (url == '1987'):
    	sub = record['url'].split('/')[4]
    	if (sub == '12'):
    		#if(record['url'].split('/')[5] == '01'):
    		#print record['url']
    		#html_pages.append(download_page(record))
            article = extract_external_links(download_page(record))
            article["url"] = record['url']
            nytimes.append(article)
    	#cnt[sub] += 1
    	#j = j + 1
    #i = i + 1
np.save("nytimes-12", nytimes)