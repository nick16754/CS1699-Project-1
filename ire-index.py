import os
import json
import nltk
import collections
import re
from nltk.tokenize import RegexpTokenizer
from collections import defaultdict

exclude = set(['!','@','\'','-', '_','<',',','>','.','?','/','@','#','$','%','^','&','*','(',')','+','=','\\','[','{',']','}','|','`','~'])
DocDictionary = defaultdict(list)
DocList = []
input_files = os.listdir("input")
#for root, dirs, files in os.walk('input'):
os.chdir("input")
numDocs = 0
for item in input_files:
    numDocs += 1
    doc_in = item
    doc_Name = item
    if os.path.isfile(os.path.join(os.getcwd(),doc_in)):
        doc_input = [line.strip() for line in open(doc_in, 'r')]
        for item in doc_input:
            splitItem = item.split()
            for item in splitItem:
                item = item.lower()
                item = re.sub(r'\d+', '', item)
                item = ''.join(ch for ch in item if ch not in exclude)
                myTokens = nltk.word_tokenize(item)
                porter = nltk.PorterStemmer()
                for token in myTokens:
                    item = porter.stem(token)
                ##DocList.append(item)  ##This would add the stemmed word to a list
                if item in DocDictionary:
                    docList = DocDictionary[item]
                    insideDict = docList[0]
                    if(doc_Name in insideDict):
                        insideDict[doc_Name] += 1
                    else:
                        insideDict[doc_Name] = 1
                        docList[1] += 1
                else:
                    docList = DocDictionary[item]
                    insideDict = {}
                    insideDict[doc_Name] = 1
                    docList.append(insideDict)
                    docList.append(1)

    else:
        print "Error reading from input."
        exit(1)
#for keys,values in DocDictionary.items():
#    print(keys)
#    print(values)
#print "is:"
#print DocDictionary['is']
DocDictionary["NumberOfDocsReadFromInput.avi"] = numDocs    ##this is hoped to never be used for a document name, I could have made it more outrageous. I'm sure there is a better way to do this.
os.chdir("..")
f = open('inverted-index.json','wb')
json.dump(DocDictionary,f)
f.close()