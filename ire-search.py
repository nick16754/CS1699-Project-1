import os
import json
import nltk
import collections
import re
import math
from nltk.tokenize import RegexpTokenizer
from collections import defaultdict

exclude = set(['!','@','\'','-', '_','<',',','>','.','?','/','@','#','$','%','^','&','*','(',')','+','=','\\','[','{',']','}','|','`','~'])

f = open('inverted-index.json','rb')
DocDictionary = json.load(f)
f.close()
numDocs = DocDictionary["NumberOfDocsReadFromInput.avi"]
#relevDict = {}

file_in = 'keywords.txt'####raw_input("keywords.txt")####
if os.path.isfile(os.path.join(os.getcwd(),file_in)):
    f = open(file_in)
    keywordLine = []
    for line in f:
        relevDict = {}
        inputKeyWords = (line).split()
        print 'Keyword(s) = ', (inputKeyWords)
        inputKeyWordsLower = []
        for item in inputKeyWords:
            inputKeyWordsLower.append(item.lower())
        #print(inputKeyWordsLower)
        KeyWordsNums = []
        for item in inputKeyWordsLower:
            if re.sub(r'\d+', '', item) != (''):
                KeyWordsNums.append(re.sub(r'\d+', '', item))
        #print KeyWordsNums
        KeyWordsPunc = []
        for item in KeyWordsNums:
            #tokenizer = RegexpTokenizer(r'\w+' r'[\']')
            #TokenList = tokenizer.tokenize(item)
            #for item in TokenList:                             ####I'm sure I'm supposed to use regular expression and this tokenizer to do this, but I couldn't really figure it out.
            #    KeyWordsPunc.append(item)
            KeyWordsPunc.append(''.join(ch for ch in item if ch not in exclude))
        #print("punct removed: ")
        #print(KeyWordsPunc)

        FinalKeyWords = []
        for item in KeyWordsPunc:
            myTokens = nltk.word_tokenize(item)
            # Stemming
            porter = nltk.PorterStemmer()
            for token in myTokens:
                FinalKeyWords.append(porter.stem(token))
        for item in FinalKeyWords:
            print "weight of ", item
            if(item in DocDictionary):
                itemsList = DocDictionary[item]
                n = itemsList[1]
                nameToFreq = itemsList[0]
                for key in nameToFreq:
                    assert isinstance(n, object)
                    x = numDocs / float(n)
                    print 'in', key, ": ", (1 + math.log(nameToFreq[key],2)) * math.log(x, 2)#, nameToFreq[key], numDocs, n
                    if key in relevDict:
                        relevDict[key] += (1 + math.log(nameToFreq[key], 2)) * math.log(x, 2)
                    else:
                        relevDict[key] = (1 + math.log(nameToFreq[key],2)) * math.log(x, 2)

            else:
                inv = 0
        #for item in FinalKeyWords:  ##I read the entire keyword file as a list of keywords
        #for key in relevDict:
            #print 'Filename:',key,' Relevancy: ',relevDict[key]

        for w in sorted(relevDict, key=relevDict.get, reverse=True):
            print w, relevDict[w]

else:
    print('keywords.txt file not found.')