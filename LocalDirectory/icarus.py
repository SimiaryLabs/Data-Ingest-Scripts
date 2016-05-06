#!/usr/bin/python

import sys, getopt

from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from cStringIO import StringIO

import docx2txt

import glob

from MeteorClient import MeteorClient


def convert_pdf_to_txt(path):
    rsrcmgr = PDFResourceManager()
    retstr = StringIO()
    codec = 'utf-8'
    laparams = LAParams()
    device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
    fp = open(path, 'rb',)
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    password = ""
    maxpages = 0
    caching = True
    pagenos=set()
    for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password,caching=caching, check_extractable=True):
        interpreter.process_page(page)
    fp.close()
    device.close()
    str = retstr.getvalue()
    retstr.close()
    return str

def convert_docx_to_txt(path):
    return docx2txt.process(path)

def convert_txt_to_txt(path):
    fh = open(path,"r")
    txt = fh.read()
    fh.close()
    return txt


def insert_callback(error, data):
    if error:
        print(error)
        return

def processDirectory(path, newCorporaName, newCorporaID, nextDocumentID):
   
   print("Processing files...")

   # Connect to Pterraformer
   client = MeteorClient('ws://127.0.0.1:4567/websocket')
   client.connect()

   # ID of the next available Document record
   id = int(nextDocumentID)
   # ID of the next available Corpus record
   corpusID = newCorporaID

   # Create a new Collection
   client.insert('corpora', { '_id': str(corpusID), 'owner': "public", 'properties': { '@context': 'http://schema.org', 'name': newCorporaName, 'modifyDate': '14/6/14'}})

   
   types = ('./*.pdf', './*.docx', './*.txt') # the tuple of file types

   # Must work with the path object to allow specification of the correct directory
   files_grabbed = []
   for files in types:
       files_grabbed.extend(glob.glob(files))
       
   files_grabbed   # the list of pdf and cpp files


   for filename in files_grabbed:
       print(filename)
       fileExtensionType = filename.split(".")[-1]
       
       if (fileExtensionType == "pdf"):
           rawText = convert_pdf_to_txt(filename)
       elif (fileExtensionType == "docx"):
           rawText = convert_docx_to_txt(filename)
       else:
           rawText = convert_txt_to_txt(filename)
           
       id2 = id
       id = id + 1
       
       #print(rawText[:50])
       
       client.insert('documents', { '_id': str(id2),
           'corpus': str(corpusID),
           'rawText': rawText,
           'properties': {
             '@context': 'http://schema.org/',
             '@type': 'CreativeWork',
             'name': filename,
             'modifyDate': '13/4/2015'
           },
           'parsingResults': {
           },
           'placeReferences': [
           ],
           'matchedPlaces': {
             "type": "FeatureCollection",
             "features": []
           },
           'markedUpText': "hello"
         }, callback=insert_callback)

def main(argv):
   inputDiretory = ''

   try:
      opts, args = getopt.getopt(argv,"hi:n:c:d:",["directory=", "name=", "corporaID=", "documentID="])
   except getopt.GetoptError:
      print 'icarus.py -d <inputDirectory>'
      sys.exit(2)

   for opt, arg in opts:
      if opt == '-h':
         print 'icarus.py -i <inputDirectory> -n <CorporaName> -c <NextAvailableCorporaID> -d <NextAvailableDocumentID>'
         sys.exit()
      elif opt in ("-i", "--directory"):
         inputDiretory = arg
      elif opt in ("-n", "--name"):
         corpusName = arg
      elif opt in ("-c", "--corporaID"):
         corpusID = arg
      elif opt in ("-d", "--documentID"):
         documentID = arg


   print 'Input Directory is', inputDiretory
   print 'New Corpora name is', corpusName
   processDirectory(inputDiretory, corpusName, corpusID, documentID)


if __name__ == "__main__":
   main(sys.argv[1:])