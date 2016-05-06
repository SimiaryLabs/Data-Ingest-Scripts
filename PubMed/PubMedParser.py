# For parsing the XML structure
from bs4 import BeautifulSoup
# For reading file lists from the filesystem
import glob
# For saving the processed output to disk
import numpy as np
import pickle

import sys
sys.setrecursionlimit(100)

# For timing
import time

# Extracts the information from a single document, creates a payload object containing the corpusID and API key ready to be sent
def pubMed_document_insert_request(fileName, corpus_id, api_key):

    # Create payload structure
    payload = {}
    payload['properties'] = {}
    payload['properties']['type'] = "article"
    payload['sections'] = []
    
    # Pteraforma API stuff
    payload['api_key'] = api_key
    payload['corpus'] = corpus_id
    payload['options'] = {
        'doTemporal': True,
        'doGeo': True,
        'doGeoboost': True
    }
    
    # Load Article into beautiful soup
    handle = open(fileName, 'rb')
    soup = BeautifulSoup(handle, "lxml-xml")
    
    # Process bilographic header information
    payload['title'] = soup.front.find('article-title').get_text()
    if (soup.front.find('article-id', {"pub-id-type": "doi"}) != None):
        payload['properties']['DOI'] = soup.front.find('article-id', {"pub-id-type": "doi"}).get_text()

    payload['properties']['refs'] = []
    
    payload['properties']['publishedDate'] = {}
    if (soup.front.find('pub-date').find('day') != None):
        payload['properties']['publishedDate']['day'] = soup.front.find('pub-date').find('day').get_text()
    if (soup.front.find('pub-date').find('month') != None):
        payload['properties']['publishedDate']['month'] = soup.front.find('pub-date').find('month').get_text()
    if (soup.front.find('pub-date').find('year') != None):
        payload['properties']['publishedDate']['year'] = soup.front.find('pub-date').find('year').get_text()
    if (soup.front.find('journal-title') != None):
        payload['properties']['journal'] = soup.front.find('journal-title').get_text()

    payload['properties']['pubmedid'] = []
    
    # Process back section if it exists
    if(soup.back != None):
        for ref in soup.back.find_all("ref"):
            payload['properties']['refs'].append(ref.get('id'))

    # Process Abstract
    if (soup.abstract != None):
        abstractParagraphs = []
        for paragraph in soup.abstract.find_all("p"):
            abstractParagraphs.append(paragraph.get_text())

        # Gets references within the abstract
        abstractReferences = []
        for ref in soup.abstract.find_all("xref"):
            refs = ref.get('rid').split()
            for ind in refs:
                abstractReferences.append(ind)

        # Inserts abstrat section
        payload['sections'].append({ "title": "Abstract", "type": "abstract","number": "0", "paragraphs": abstractParagraphs, "references": abstractReferences})

    # Other sections
    if (soup.body != None):
        for section in soup.body.find_all("sec", recursive=False):

            # Gets all the paragraphs
            sectionParagraphs = []
            # Gets references within the section
            sectionReferences = []
            
            for paragraph in section.find_all("p", recursive=False):
                sectionParagraphs.append(paragraph.get_text())
                for ref in paragraph.find_all("xref"):
                    refs = ref.get('rid').split()
                    for ind in refs:
                        sectionReferences.append(ind)

            if (section.title != None):
                sectionTitle = section.title.get_text()
            else:
                sectionTitle = ""
             
            payload['sections'].append({ "title": sectionTitle, "type": section.get('sec-type') , "number": section.get('id'), "paragraphs": sectionParagraphs, "references": sectionReferences})

            
            
            # Process nested Sections
            for subsection in section.find_all("sec"):            
                subSectionParagraphs = []
                subSectionReferences = []
                for paragraph in subsection.find_all("p", recursive=False):
                    subSectionParagraphs.append(paragraph.get_text())
                    for ref in paragraph.find_all("xref"):
                        refs = ref.get('rid').split()
                        for ind in refs:
                            subSectionReferences.append(ind)
                
                if(subsection.title == None):
                    title = "blank"
                else:
                    title = subsection.title.get_text()
                    
                payload['sections'].append({ "title": title, "type": section.get('sec-type') ,"number": subsection.get('id'), "paragraphs": subSectionParagraphs, "references": subSectionReferences})
    else:
        print("no body")
    
    # Return the Payload to-do: needs to be converted into a request
    return payload


# Method for process the entire archive (change the glob regex to change that scope)
def process_archive(archive, maxNum, startNumber, api_key, corpus_id, outputName):

    # Measuring execution time
    start = time.time()

    # Get the list of files in this archive
    fileList = glob.glob(archive)

    print("After Glob: ", time.time() - start)

    dataArray = []
    
    # Loop over each article in the archive
    count = 0
    for article in fileList:
        # Allows skipping frst n files
        if (count < startNumber):
            count = count + 1
            continue

        # Creates the request object, this is the point of extension for integration with Pteraforma
        request = pubMed_document_insert_request(article, corpus_id, api_key)

        # Logging code for feedback, remove if run in production
        if (count % 100 == 0):
            print(count, time.time() - start)

        # Saving the result
        dataArray.append(request)

        # Implements a cut off number to processm, helpful for testing
        count = count + 1
        if (count > maxNum):
            break

    np.save(outputName, dataArray)
    #pickle.dump(dataArray, outputName)

    #with open("a-b.pickle",'wb') as f:
    #    pickle.dump(dataArray,f)

def process_archives(maxNum, startNumber, api_key, corpus_id):

    # 261,868 articles total, 18.53GB
    ab = "BioMed/articles.A-B/*/*.nxml"
    abOutput = "ab"
    process_archive(ab, maxNum, startNumber, api_key, corpus_id, abOutput)

    # 248,550 articles total, 17.93GB
    ch = "BioMed/articles.C-H/*/*.nxml"
    chOutput = "ch"
    #process_archive(ch, maxNum, startNumber, api_key, corpus_id,  chOutput)

    # 399,542 articles total, 23.97GB
    ni = "BioMed/articles.I-N/*/*.nxml"
    inOutput = "in"
    #process_archive(ni, maxNum, startNumber, api_key, corpus_id,  inOutput)

    # 274,080 articles total, 22.03GB
    oz = "BioMed/articles.A-B/*/*.nxml"
    ozOutput = "oz"
    #process_archive(oz, maxNum, startNumber, api_key, corpus_id,  ozOutput)

    # Total Artciles = 1,183,859
    # Total Size = 82.46GB


# parameters
totalNumber = 10000
startFrom = 0
api_key = "a1df798b5a10274bf32106303e91f05f"
corpus_id = 52

process_archives(totalNumber, startFrom, api_key, corpus_id)

