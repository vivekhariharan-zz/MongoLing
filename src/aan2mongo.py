
from pymongo import Connection
import sys

from dataobjects.author_fields import AuthorFields
from dataobjects.paper_fields import PaperFields


class Aan2Mongo:
    
    authorDetails = {}
    paperInfo = {}
    
    def __init__(self, pathToAan):
        self.paperFields = PaperFields()
        self.authorFields = AuthorFields()
        self.aanMongoConnection = Connection()
        self.aanDb = self.aanMongoConnection['mongoling']
        self.aanDb.research_papers.remove({})
        self.aanDb.authors.remove({})
        self.releasePath = pathToAan +"/release";
        self.papersPath = pathToAan+"/papers_text"
        self.authorsPath = pathToAan+"/author_affiliations"
        
    def portData(self):
        self.load_paper_metadata()
        self.load_citation_data()
        self.load_authors_data_to_author_details()
        self.load_authors_data_to_paper_info()
        self.load_paper_text_data()
        self.write_to_mongo()
        self.aanMongoConnection.close()
        
    def write_to_mongo(self):
        paperKeys = self.paperInfo.keys()
        for paperKey in paperKeys:
            currentPaper = self.paperInfo[paperKey]
            
            
            self.aanDb.research_papers.insert(currentPaper)
        
        authorKeys = self.authorDetails.keys()
        for authorKey in authorKeys:
            currentAuthor = self.authorDetails[authorKey]
            self.aanDb.authors.insert(currentAuthor)
            
    def load_authors_data_to_paper_info(self):
        '''loads author objects to corresponding paper info '''
        authorsFileReader = open(self.authorsPath+"/author_affiliations_raw.txt", 'r')
        
        for line in authorsFileReader:
            decodedLine = line.decode("iso-8859-1")
            encodedLine = decodedLine.encode('utf-8', 'ignore')
            cleanedLine = encodedLine.replace('\n', '').rstrip()
            if cleanedLine == '':
                #if line is blank skip to next line
                continue
            lineParts = cleanedLine.split('\t')
            paperId = lineParts[0].strip()
            paperUrl = lineParts[1].strip()
            tempAuthors = []
            if paperId == '':
                continue
            for i in range(2, len(lineParts)):
                if lineParts[i].strip() =='':
                    continue
                tempAuthors.append(lineParts[i])
            
            #combining authorname and association into new list
            tempIterator = iter(tempAuthors)
            authors = [authorName+"###"+next(tempIterator, '') for authorName in tempIterator]
            
            if self.paperInfo.has_key(paperId):
                paperInfo = self.paperInfo[paperId]
                paperInfo[self.paperFields.URL] = paperUrl
                
                for authorInfo in authors:
                    authorName = authorInfo.split("###")[0].strip()
                    if self.authorDetails.has_key(authorName):
                        currentAuthor = self.authorDetails[authorName]
                    else:
                        continue
                    if paperInfo.has_key(self.paperFields.AUTHORS_LIST):
                        paperInfo[self.paperFields.AUTHORS_LIST].append(currentAuthor)
                    else:
                        paperInfo[self.paperFields.AUTHORS_LIST] = []
                        paperInfo[self.paperFields.AUTHORS_LIST].append(currentAuthor)
                        
                self.paperInfo[paperId] = paperInfo
                    
            else:
                newPaperInfo = {}
                newPaperInfo[self.paperFields.AAN_ID] = paperId
                newPaperInfo[self.paperFields.URL] = paperUrl
                
                for authorInfo in authors:
                    authorName = authorInfo.split("###")[0].strip()
                    currentAuthor = self.authorDetails[authorName]
                    if newPaperInfo.has_key(self.paperFields.AUTHORS_LIST):
                        newPaperInfo[self.paperFields.AUTHORS_LIST].append(currentAuthor)
                    else:
                        newPaperInfo[self.paperFields.AUTHORS_LIST] = []
                        newPaperInfo[self.paperFields.AUTHORS_LIST].append(currentAuthor)
                self.paperInfo[paperId] = newPaperInfo
                
        authorsFileReader.close()
                

    def load_authors_data_to_author_details(self):
        '''populates author objects with affiliation and papers they have authored '''
        authorsFileReader = open(self.authorsPath+"/author_affiliations_raw.txt", 'r')
        
        for line in authorsFileReader:
            decodedLine = line.decode("iso-8859-1")
            encodedLine = decodedLine.encode("utf-8", 'ignore')
            cleanedLine = encodedLine.replace('\n', '').rstrip()
            if cleanedLine == '':
                #if line is blank skip to next line
                continue
            lineParts = cleanedLine.split('\t')
            paperId = lineParts[0].strip()
            paperUrl = lineParts[1].strip()
            tempAuthors = []
            for i in range(2, len(lineParts)):
                if lineParts[i].strip() =='':
                    continue
                tempAuthors.append(lineParts[i])
            
            #combining authorname and association into new list
            tempIterator = iter(tempAuthors)
            authors = [authorName+"###"+next(tempIterator, '') for authorName in tempIterator]
            
            for authorInfo in authors:
                authorName = authorInfo.split("###")[0].strip()
                authorAffiliation = authorInfo.split("###")[1].strip()
                authorName = authorName
                authorAffiliation = authorAffiliation
                if self.authorDetails.has_key(authorName):
                    currentAuthor = self.authorDetails[authorName]
                    currentAuthor[self.authorFields.PAPERS_LIST].append(paperId)
                    self.authorDetails[authorName] = currentAuthor
                else:
                    currentAuthor = {}
                    currentAuthor[self.authorFields.NAME] = authorName
                    currentAuthor[self.authorFields.AFFILIATED_TO] = authorAffiliation
                    currentAuthor[self.authorFields.PAPERS_LIST] = []
                    currentAuthor[self.authorFields.PAPERS_LIST].append(paperId)
                    self.authorDetails[authorName] = currentAuthor

        authorsFileReader.close()
        
    def load_paper_metadata(self):
        '''creates initial paper objects with metadata from acl-metadata.txt '''
        years = {2008, 2009, 2010, 2011, 2012}
        currentPaper = {}
        isLineContinuation = False
        for year in years:
            metaDataReader = open(self.releasePath+"/"+str(year)+"/acl-metadata.txt", 'r')
            prevLine = ""
            prevValue = ""
            prevField = ""
            
            for line in metaDataReader:
                decodedLine = line.decode("iso-8859-1")
                encodedLine = decodedLine.encode("utf-8", 'ignore')
                cleanedLine = encodedLine.replace("\n", '').rstrip()
                
                
                if not isLineContinuation:
                    #checking for continuation lines
                    if cleanedLine == '':
                        #if line is blank create new paper object and skip line
                        if prevLine == '' and cleanedLine == '':
                            prevLine = cleanedLine
                            continue
                        paperId = currentPaper[self.paperFields.AAN_ID]
                        self.paperInfo[paperId] = currentPaper
                    
                        currentPaper = {}
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
        '''Adds citations to the paper objects '''
        years = {2008, 2009, 2010, 2011, 2012}
        for year in years:
            citationFileReader = open(self.releasePath+"/"+str(year)+"/acl.txt")
            
            for line in citationFileReader:
                decodedLine = line.decode("iso-8859-1")
                encodedLine = decodedLine.encode("utf-8", 'ignore')
                cleanedLine = line.replace('\n', '').rstrip()
                if cleanedLine == '':
                # if it is a blank line skip it
                    continue
                lineParts = cleanedLine.split("==>")
                sourcePaperId = lineParts[0].strip()
                citedPaperId = lineParts[1].strip()
                if self.paperInfo.has_key(citedPaperId):
                    paper = self.paperInfo[citedPaperId]
                    if paper.has_key(self.paperFields.CITATION_LIST):
                        paper[self.paperFields.CITATION_LIST].append(sourcePaperId)
                    else:
                        paper[self.paperFields.CITATION_LIST] = []
                        paper[self.paperFields.CITATION_LIST].append(sourcePaperId)
            citationFileReader.close()    

    def load_field_value(self, paper, field, value):
        '''loads particular field into the paper object with the value'''
        if field == "id":
            paper[self.paperFields.AAN_ID] = value.lstrip("{").rstrip("}")
        if field == "author":
            paper[self.paperFields.AUTHORS_NAMES] = value.lstrip("{").rstrip("}").split(";")
        if field == "title":
            titleValue = value.lstrip('{').rstrip('}')
            
            paper[self.paperFields.TITLE] = titleValue
        if field == "venue":
            paper[self.paperFields.VENUE] = value.lstrip("{").rstrip("}")
        if field == "year":
            paper[self.paperFields.YEAR] = value.lstrip("{").rstrip("}")
        
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print " Provide path to the aan folder "
        print "Python Aan2Mongo.py <path-to-aan-folder>"
        sys.exit(1)
    
    path = sys.argv[1]
    porter = Aan2Mongo(path)
    porter.portData()
            
        