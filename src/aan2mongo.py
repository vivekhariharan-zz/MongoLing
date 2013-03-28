
import pymongo
import sys

from bson.author import Author
from bson.paper import Paper


class Aan2Mongo:
    
    authorInfo = {}
    paperInfo = {}
    def __init__(self, pathToAan):
        self.releasePath = pathToAan +"/release";
        self.papersPath = pathToAan+"/papers_text"
        self.authorsPath = pathToAan+"/author_affiliations"
        
    def portData(self):
        self.load_paper_metadata()
        self.load_citation_data()
        self.load_authors_data()
        self.load_paper_text_data()
        self.write_to_mongo()
        
    def load_authors_data(self):
        authorsFileReader = open(self.authorsPath+"/author_affiliations_raw.txt", 'r')
        
        for line in authorsFileReader:
            cleanedLine = line.replace('\n', '').strip()
            if cleanedLine == '':
#                if line is blank skip to next line
                continue
            lineParts = cleanedLine.split('\t')
            paperId = lineParts[0].strip()
            paperUrl = lineParts[1].strip()
            authors = lineParts[2:len(lineParts)]
            
            print "Id: " + paperId
            print "URL: " + paperUrl
            print "authors: " + str(authors)

    def load_paper_metadata(self):
        years = {2008, 2009, 2010, 2011, 2012}
        currentPaper = Paper()
        isLineContinuation = False
        for year in years:
            metaDataReader = open(self.releasePath+"/"+str(year)+"/acl-metadata.txt", 'r')
            prevLine = ""
            prevValue = ""
            prevField = ""
            
            for line in metaDataReader:
                cleanedLine = line.replace("\n", '').strip()
                
                if not isLineContinuation:
                    #checking for continuation lines
                    if cleanedLine == '':
                        #if line is blank create new paper object and skip line
                        if prevLine == '' and cleanedLine == '':
                            prevLine = cleanedLine
                            continue
                        paperId = currentPaper.id
                        self.paperInfo[paperId] = currentPaper
                    
                        currentPaper = Paper()
                        prevLine = cleanedLine
                        continue
                    lineParts = cleanedLine.split("=")
                    if len(lineParts) == 2:
                        field = lineParts[0].strip()
                        value = lineParts[1].strip()
                        if '}' in value:
                            self.load_field_value(currentPaper, field, value)
                            isLineContinuation = False
                        else:
                            prevField = field
                            prevValue = value
                            isLineContinuation = True
                    else:
                        #continued line      
                        field = prevField
                        value = prevValue +" "+cleanedLine
                        if '}' in value:
                            self.load_field_value(currentPaper, field, value)
                            isLineContinuation = False
                        else:
                            prevField = field
                            prevValue = value
                            isLineContinuation = True
                prevLine = cleanedLine
                
            metaDataReader.close()
    
    def load_paper_text_data(self):
        pass
    
    def load_citation_data(self):
        years = {2008, 2009, 2010, 2011, 2012}
        for year in years:
            citationFileReader = open(self.releasePath+"/"+str(year)+"/acl.txt")
            
            for line in citationFileReader:
                cleanedLine = line.replace('\n', '').strip()
                if cleanedLine == '':
                # if it is a blank line skip it
                    continue
                lineParts = cleanedLine.split("==>")
                sourcePaperId = lineParts[0].strip()
                citedPaperId = lineParts[1].strip()
                if self.paperInfo.has_key(citedPaperId):
                    paper = paperInfo[citedPaperId]
                    paper.citation_list.append(sourcePaperId)
            
            citationFileReader.close()    

    def load_field_value(self, paper, field, value):
        '''loads particular field into the paper object with the value'''
        if field == "id":
            paper.id = value.lstrip("{").rstrip("}")
        if field == "author":
            paper.authors_names = value.lstrip("{").rstrip("}").split(";")
        if field == "title":
            paper.title = value.lstrip("{").rstrip("}")
        if field == "venue":
            paper.venue = value.lstrip("{").rstrip("}")
        if field == "year":
            paper.year = value.lstrip("{").rstrip("}")
        
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print " Provide path to the aan folder "
        print "Python Aan2Mongo.py <path-to-aan-folder>"
        sys.exit(1)
    
    path = sys.argv[1]
    porter = Aan2Mongo(path)
    porter.portData()
            
        