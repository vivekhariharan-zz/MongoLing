
from pymongo import Connection
import sys

from dataobjects.author_fields import AuthorFields
from dataobjects.paper_fields import PaperFields
from dataobjects.venue_fields import VenueFields

from os import listdir
from os.path import isfile, join, isdir

def getDirNames(dirPath):
    dirNames = []
    for f in listdir(dirPath):
        if isdir(join(dirPath,f)):
            dirNames.append(join(dirPath,f))
    
    return dirNames

def listFiles(dirPath):
    """Given a directory it returns a list containing all the files in the directory """
    files = []
    for f in listdir(dirPath):
        if isfile(join(dirPath,f)):
            files.append(join(dirPath,f))
        if isdir(join(dirPath,f)):
            files.extend(listFiles(join(dirPath,f)))
    
    return files
            
class Aan2Mongo:
    
    authorDetails = {}
    paperInfo = {}
    venueInfo = {}
    
    def __init__(self, pathToAan):
        pathToAan = pathToAan.rstrip('/')
        self.paperFields = PaperFields()
        self.authorFields = AuthorFields()
        self.venueFields = VenueFields()
        self.aanMongoConnection = Connection()
        self.aanDb = self.aanMongoConnection['mongoling']
        self.aanDb.research_papers.remove({})
        self.aanDb.venue_info.remove({})
        self.aanDb.authors.remove({})
        self.releasePath = pathToAan +"/release";
        self.papersPath = pathToAan+"/papers_text"
        self.authorsPath = pathToAan+"/author_affiliations"
        self.citationSummaryPath = pathToAan+"/citation_summaries"
        
    def portData(self):
        """function that drives the data loading """
        self.load_paper_metadata()
        self.load_citation_data()
        self.load_authors_data_to_author_details()
        self.load_authors_data_to_paper_info()
        self.load_paper_text_data()
        self.load_citation_summary()
        self.load_venue_data()
        self.compute_stats()
        self.write_to_mongo()
        self.aanMongoConnection.close()
        
    def write_to_mongo(self):
        """Writes all paper, author and venue objects to mongodb """
        paperKeys = self.paperInfo.keys()
        for paperKey in paperKeys:
            currentPaper = self.paperInfo[paperKey]
            self.aanDb.research_papers.insert(currentPaper)
        
        authorKeys = self.authorDetails.keys()
        for authorKey in authorKeys:
            currentAuthor = self.authorDetails[authorKey]
            self.aanDb.authors.insert(currentAuthor)
        
        venueKeys = self.venueInfo.keys()
        for venueKey in venueKeys:
            currentVenue = self.venueInfo[venueKey]
            self.aanDb.venue_data.insert(currentVenue)
    
    def load_citation_summary(self):
        """populates the paper objects with corresponding citation summaries"""
        citationFiles = listFiles(self.citationSummaryPath)
        for citationFile in citationFiles:
            citationFileReader = open(citationFile, 'r')
            citationFileName = citationFile.split('/')[len(citationFile.split('/')) - 1]
            paperId = citationFileName.split('.')[0]
            
            if self.paperInfo.has_key(paperId):
                currentPaperInfo = self.paperInfo[paperId]
                currentPaperInfo[self.paperFields.CITATION_SUMMARY] = citationFileReader.read()
            
            citationFileReader.close()
        
        
    def compute_stats(self):
        """computes the stats for each paper, author and object """
        paperKeys = self.paperInfo.keys()
        for paperKey in paperKeys:
            currentPaper = self.paperInfo[paperKey]
            if currentPaper.has_key(self.paperFields.CITATION_LIST):
                currentPaper[self.paperFields.CITATION_COUNT] =  len(currentPaper[self.paperFields.CITATION_LIST])
            else:
                currentPaper[self.paperFields.CITATION_LIST] = []
                currentPaper[self.paperFields.CITATION_COUNT] =  len(currentPaper[self.paperFields.CITATION_LIST])
        
        authorKeys = self.authorDetails.keys()
        for authorKey in authorKeys:
            currentAuthor = self.authorDetails[authorKey]
            if currentAuthor.has_key(self.authorFields.PAPERS_LIST):
                currentAuthor[self.authorFields.PAPER_COUNT] = len(currentAuthor[self.authorFields.PAPERS_LIST])
            else:
                currentAuthor[self.authorFields.PAPERS_LIST] = []
                currentAuthor[self.authorFields.PAPER_COUNT] = len(currentAuthor[self.authorFields.PAPERS_LIST])
        
        venueKeys = self.venueInfo.keys()
        for venueKey in venueKeys:
            currentVenue = self.venueInfo[venueKey]
            if currentVenue.has_key(self.venueFields.PAPERS_LIST):
                currentVenue[self.venueFields.PAPER_COUNT] = len(currentVenue[self.venueFields.PAPERS_LIST])
            else:
                currentVenue[self.venueFields.PAPERS_LIST] = []
                currentVenue[self.venueFields.PAPER_COUNT] = len(currentVenue[self.venueFields.PAPERS_LIST])
            
    def load_authors_data_to_paper_info(self):
        """loads author objects to corresponding paper info """
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
        """populates author objects with affiliation and papers they have authored """
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
                if self.authorDetails.has_key(authorName):
                    currentAuthor = self.authorDetails[authorName]
                    currentAuthor[self.authorFields.PAPERS_LIST].append(paperId)
                    affliatedList = currentAuthor[self.authorFields.AFFILIATED_TO]
                    if authorAffiliation not in affliatedList:
                        currentAuthor[self.authorFields.AFFILIATED_TO].append(authorAffiliation)
                    self.authorDetails[authorName] = currentAuthor
                else:
                    currentAuthor = {}
                    currentAuthor[self.authorFields.NAME] = authorName
                    currentAuthor[self.authorFields.AFFILIATED_TO] = [] 
                    currentAuthor[self.authorFields.AFFILIATED_TO].append(authorAffiliation)
                    currentAuthor[self.authorFields.PAPERS_LIST] = []
                    currentAuthor[self.authorFields.PAPERS_LIST].append(paperId)
                    self.authorDetails[authorName] = currentAuthor

        authorsFileReader.close()
        
    def load_paper_metadata(self):
        """creates initial paper objects with metadata from acl-metadata.txt """
        dirs = getDirNames(self.releasePath)
        #remove directories that are not years
        for dir in dirs:
            dirName = dir.split("/")[len(dir.split("/"))-1]
            if dirName.isdigit():
                continue
            else:
                dirs.remove(dir)
                
        currentPaper = {}
        isLineContinuation = False
        for dir in dirs:
            metaDataReader = open(dir+"/acl-metadata.txt", 'r')
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
        """populates the paper objects with corresponding paper text"""
        #TODO: handle .cite files and .body files separately
        paperTextFiles = listFiles(self.papersPath)
        for paperTextFile in paperTextFiles:
            paperTextFileReader = open(paperTextFile, 'r')
            paperFileName = paperTextFile.split('/')[len(paperTextFile.split('/')) - 1]
            paperId = paperFileName.split('.')[0]
            
            if self.paperInfo.has_key(paperId):
                currentPaperInfo = self.paperInfo[paperId]
                currentPaperInfo[self.paperFields.PAPER_TEXT] = paperTextFileReader.read()
            
            paperTextFileReader.close()
    
    def load_citation_data(self):
        """Adds citation links to the paper objects """
        dirs = getDirNames(self.releasePath)
        #remove directories that are not years
        for dir in dirs:
            dirName = dir.split("/")[len(dir.split("/"))-1]
            if dirName.isdigit():
                continue
            else:
                dirs.remove(dir)
        for dir in dirs:
            citationFileReader = open(dir+"/acl.txt")
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
        """loads particular field into the paper object with the value"""
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
        
    def load_venue_data(self):
        """finds venues from papers and collects papers by venue """
        paperKeys = self.paperInfo.keys()
        for paperKey in paperKeys:
            currentPaper = self.paperInfo[paperKey]
            if currentPaper.has_key(self.paperFields.VENUE):
                currentVenue = currentPaper[self.paperFields.VENUE]
                if self.venueInfo.has_key(currentVenue):
                    venueRecord = self.venueInfo[currentVenue]
                    venueRecord[self.venueFields.PAPERS_LIST].append(currentPaper[self.paperFields.AAN_ID])
                    self.venueInfo[currentVenue] = venueRecord
                else:
                    venueRecord = {}
                    venueRecord[self.venueFields.NAME] = currentVenue
                    venueRecord[self.venueFields.PAPERS_LIST] = []
                    venueRecord[self.venueFields.PAPERS_LIST].append(currentPaper[self.paperFields.AAN_ID])
                    self.venueInfo[currentVenue] = venueRecord
        
        
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print " Provide path to the aan folder "
        print "Python Aan2Mongo.py <path-to-aan-folder>"
        sys.exit(1)
    
    path = sys.argv[1]
    porter = Aan2Mongo(path)
    porter.portData()
            
        