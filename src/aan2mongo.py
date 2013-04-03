
from pymongo import Connection
import sys

from dataobjects.authorfields import AuthorFields
from dataobjects.paperfields import PaperFields
from dataobjects.venuefields import VenueFields
from dataobjects.papercategoryfields import PaperCategoryFields
from utils.generalutils import get_dir_names, list_files

from datetime import datetime
            
class Aan2Mongo:
    
    authorDetails = {}
    paperInfo = {}
    venueInfo = {}
    paperCategoryInfo = {}
    paperCategoryName = {}
    
    def __init__(self, pathToAan):
        pathToAan = pathToAan.rstrip('/')
        self.aanMongoConnection = Connection()
        self.aanDb = self.aanMongoConnection['mongoling']
        self.cleanUp()
        self.setUpDataObjects()        
        self.setUpPaths(pathToAan)
        
    def setUpDataObjects(self):
        """Initialize the fields class that contains the data fields for each collection """
        print "Initializing data objects"
        self.paperFields = PaperFields()
        self.authorFields = AuthorFields()
        self.venueFields = VenueFields()
        self.paperCategoryFields = PaperCategoryFields()
        
    def setUpPaths(self, pathToAan):
        """Used to set the paths for the data folders """
        print "Setting up paths to data"
        self.releasePath = pathToAan +"/release";
        self.papersPath = pathToAan+"/papers_text"
        self.authorsPath = pathToAan+"/author_affiliations"
        self.citationSummaryPath = pathToAan+"/citation_summaries"
        self.paperCategoriesPath = "../data/paper_categories.txt"
        
    def cleanUp(self):
        """remove existing data if it exists"""
        print "Cleaning up old data"
        self.aanDb.research_papers.remove({})
        self.aanDb.venue_info.remove({})
        self.aanDb.authors.remove({})
        
    def portData(self):
        """function that drives the data loading """
        print "Beginning Porting"
        self.initialize_paper_categories()
        self.load_paper_metadata()
        self.load_citation_data()
        self.load_authors_data_to_author_details()
        self.load_authors_data_to_paper_info()
#        self.load_paper_text_data()
        self.load_citation_summary()
        self.find_paper_category()
        self.load_venue_info()
        self.load_paper_category_info()
        self.compute_stats()
        self.write_to_mongo()
        self.aanMongoConnection.close()
        
    def write_to_mongo(self):
        """Writes all paper, author and venue objects to mongodb """
        print "Writing to mongoling in mongodb"
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
            self.aanDb.venue_info.insert(currentVenue)
            
        paperCategoryKeys = self.paperCategoryInfo.keys()
        for paperCategoryKey in paperCategoryKeys:
            currentPaperCategory = self.paperCategoryInfo[paperCategoryKey]
            self.aanDb.paper_category_info.insert(currentPaperCategory)
    
    def initialize_paper_categories(self):
        """loading the paper categories by code into a map"""
        paperCategoryFileReader = open(self.paperCategoriesPath, 'r')
        for line in paperCategoryFileReader:
            cleanedLine = line.replace('\n', '')
            paperCategory = cleanedLine.split('###')[0].strip()
            paperCategoryCode = cleanedLine.split("###")[1].strip()
            self.paperCategoryName[paperCategoryCode] = paperCategory
            
            
    def find_paper_category(self):
        """takes the paper category code and populates it in the paper object"""
        print "Finding Categories for each paper"
        paperInfoKeys = self.paperInfo.keys()
        for paperInfoKey in paperInfoKeys:
            paperId = paperInfoKey
            paperCategoryCode = paperId.split('-')[0][0]
            currentPaperInfo = self.paperInfo[paperId]
            currentPaperInfo[self.paperFields.PAPER_CATEGORY_CODE] = paperCategoryCode
            paperCategory = self.paperCategoryName[paperCategoryCode]
            currentPaperInfo[self.paperFields.PAPER_CATEGORY] = paperCategory
            
            
    def load_citation_summary(self):
        """populates the paper objects with corresponding citation summaries"""
        print "Loading citation summaires"
        citationFiles = list_files(self.citationSummaryPath)
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
        print "Computing count statistics"
        paperKeys = self.paperInfo.keys()
        for paperKey in paperKeys:
            currentPaper = self.paperInfo[paperKey]
            if currentPaper.has_key(self.paperFields.CITATION_LIST):
                currentPaper[self.paperFields.CITATION_COUNT] =  len(currentPaper[self.paperFields.CITATION_LIST])
            else:
                currentPaper[self.paperFields.CITATION_LIST] = []
                currentPaper[self.paperFields.CITATION_COUNT] =  len(currentPaper[self.paperFields.CITATION_LIST])
            
            if currentPaper.has_key(self.paperFields.AUTHOR_LIST):
                currentPaper[self.paperFields.AUTHOR_COUNT] = len(currentPaper[self.paperFields.AUTHOR_LIST])
            else:
                currentPaper[self.paperFields.AUTHOR_LIST] = []
                currentPaper[self.paperFields.AUTHOR_COUNT] = len(currentPaper[self.paperFields.AUTHOR_LIST])
                
        
        authorKeys = self.authorDetails.keys()
        for authorKey in authorKeys:
            currentAuthor = self.authorDetails[authorKey]
            if currentAuthor.has_key(self.authorFields.PAPER_LIST):
                currentAuthor[self.authorFields.PAPER_COUNT] = len(currentAuthor[self.authorFields.PAPER_LIST])
            else:
                currentAuthor[self.authorFields.PAPER_LIST] = []
                currentAuthor[self.authorFields.PAPER_COUNT] = len(currentAuthor[self.authorFields.PAPER_LIST])
        
        venueKeys = self.venueInfo.keys()
        for venueKey in venueKeys:
            currentVenue = self.venueInfo[venueKey]
            if currentVenue.has_key(self.venueFields.PAPER_LIST):
                currentVenue[self.venueFields.PAPER_COUNT] = len(currentVenue[self.venueFields.PAPER_LIST])
            else:
                currentVenue[self.venueFields.PAPER_LIST] = []
                currentVenue[self.venueFields.PAPER_COUNT] = len(currentVenue[self.venueFields.PAPER_LIST])
        
        paperCategoryKeys = self.paperCategoryInfo.keys()
        for paperCategoryKey in paperCategoryKeys:
            currentPaperCategory = self.paperCategoryInfo[paperCategoryKey]
            if currentPaperCategory.has_key(self.paperCategoryFields.PAPER_LIST):
                currentPaperCategory[self.paperCategoryFields.PAPER_COUNT] = len(currentPaperCategory[self.paperCategoryFields.PAPER_LIST])
            else:
                currentPaperCategory[self.paperCategoryFields.PAPER_LIST] = []
                currentPaperCategory[self.paperCategoryFields.PAPER_COUNT] = len(currentPaperCategory[self.paperCategoryFields.PAPER_LIST])
                
            if currentPaperCategory.has_key(self.paperCategoryFields.VENUE_LIST):
                currentPaperCategory[self.paperCategoryFields.VENUE_COUNT] = len(currentPaperCategory[self.paperCategoryFields.VENUE_LIST])
            else:
                currentPaperCategory[self.paperCategoryFields.VENUE_LIST] = []
                currentPaperCategory[self.paperCategoryFields.VENUE_COUNT] = len(currentPaperCategory[self.paperCategoryFields.VENUE_LIST])
            
    def load_authors_data_to_paper_info(self):
        """loads author objects to corresponding paper info """
        print "Loading author information"
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
                    if paperInfo.has_key(self.paperFields.AUTHOR_LIST):
                        paperInfo[self.paperFields.AUTHOR_LIST].append(currentAuthor)
                    else:
                        paperInfo[self.paperFields.AUTHOR_LIST] = []
                        paperInfo[self.paperFields.AUTHOR_LIST].append(currentAuthor)
                        
                self.paperInfo[paperId] = paperInfo
            else:
                newPaperInfo = {}
                newPaperInfo[self.paperFields.AAN_ID] = paperId
                newPaperInfo[self.paperFields.URL] = paperUrl
                
                for authorInfo in authors:
                    authorName = authorInfo.split("###")[0].strip()
                    currentAuthor = self.authorDetails[authorName]
                    if newPaperInfo.has_key(self.paperFields.AUTHOR_LIST):
                        newPaperInfo[self.paperFields.AUTHOR_LIST].append(currentAuthor)
                    else:
                        newPaperInfo[self.paperFields.AUTHOR_LIST] = []
                        newPaperInfo[self.paperFields.AUTHOR_LIST].append(currentAuthor)
                self.paperInfo[paperId] = newPaperInfo
                
        authorsFileReader.close()
                
    def load_authors_data_to_author_details(self):
        """populates author objects with affiliation and papers they have authored """
        print "Compiling author information"
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
                    currentAuthor[self.authorFields.PAPER_LIST].append(paperId)
                    affliatedList = currentAuthor[self.authorFields.AFFILIATED_TO]
                    if authorAffiliation not in affliatedList:
                        currentAuthor[self.authorFields.AFFILIATED_TO].append(authorAffiliation)
                    self.authorDetails[authorName] = currentAuthor
                else:
                    currentAuthor = {}
                    currentAuthor[self.authorFields.NAME] = authorName
                    currentAuthor[self.authorFields.AFFILIATED_TO] = [] 
                    currentAuthor[self.authorFields.AFFILIATED_TO].append(authorAffiliation)
                    currentAuthor[self.authorFields.PAPER_LIST] = []
                    currentAuthor[self.authorFields.PAPER_LIST].append(paperId)
                    self.authorDetails[authorName] = currentAuthor

        authorsFileReader.close()
        
    def load_paper_metadata(self):
        """creates initial paper objects with metadata from acl-metadata.txt """
        print "Loading paper metadata"
        dirs = get_dir_names(self.releasePath)
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
        print "Loading paper text"
        paperTextFiles = list_files(self.papersPath)
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
        print "Loading paper citations"
        dirs = get_dir_names(self.releasePath)
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
            paper[self.paperFields.AUTHOR_NAMES] = value.lstrip("{").rstrip("}").split(";")
        if field == "title":
            titleValue = value.lstrip('{').rstrip('}')
            
            paper[self.paperFields.TITLE] = titleValue
        if field == "venue":
            paper[self.paperFields.VENUE] = value.lstrip("{").rstrip("}")
        if field == "year":
            paper[self.paperFields.YEAR] = value.lstrip("{").rstrip("}")
        
    def load_venue_info(self):
        """finds venues from papers and collects papers by venue """
        print "Loading venue information"
        paperKeys = self.paperInfo.keys()
        for paperKey in paperKeys:
            currentPaper = self.paperInfo[paperKey]
            if currentPaper.has_key(self.paperFields.VENUE):
                currentVenue = currentPaper[self.paperFields.VENUE]
                if self.venueInfo.has_key(currentVenue):
                    venueRecord = self.venueInfo[currentVenue]
                    venueRecord[self.venueFields.PAPER_LIST].append(currentPaper[self.paperFields.AAN_ID])
                    if venueRecord.has_key(self.venueFields.PAPER_CATEGORY_CODES):
                        if currentPaper[self.paperFields.PAPER_CATEGORY_CODE] not in venueRecord[self.venueFields.PAPER_CATEGORY_CODES]:
                            venueRecord[self.venueFields.PAPER_CATEGORY_CODES].append(currentPaper[self.paperFields.PAPER_CATEGORY_CODE])
                        else:
                            venueRecord[self.venueFields.PAPER_CATEGORY_CODES] = []
                            venueRecord[self.venueFields.PAPER_CATEGORY_CODES].append(currentPaper[self.paperFields.PAPER_CATEGORY_CODE])
                            
                    self.venueInfo[currentVenue] = venueRecord
                else:
                    venueRecord = {}
                    venueRecord[self.venueFields.NAME] = currentVenue
                    venueRecord[self.venueFields.PAPER_LIST] = []
                    venueRecord[self.venueFields.PAPER_LIST].append(currentPaper[self.paperFields.AAN_ID])
                            
                    venueRecord[self.venueFields.PAPER_CATEGORY_CODES] = []
                    venueRecord[self.venueFields.PAPER_CATEGORY_CODES].append(currentPaper[self.paperFields.PAPER_CATEGORY_CODE])
                    
                    self.venueInfo[currentVenue] = venueRecord
        
    def load_paper_category_info(self):
        """collects papers by category """
        print "Loading paper category information"
        paperKeys = self.paperInfo.keys()
        for paperKey in paperKeys:
            currentPaper = self.paperInfo[paperKey]
            currentPaperCode = currentPaper[self.paperFields.PAPER_CATEGORY_CODE]
            if self.paperCategoryInfo.has_key(currentPaperCode):
                currentPaperCategoryInfo = self.paperCategoryInfo[currentPaperCode]
                currentPaperCategoryInfo[self.paperCategoryFields.PAPER_LIST].append(currentPaper[self.paperFields.AAN_ID])
                if currentPaper.has_key(self.paperFields.VENUE):
                    currentVenue = currentPaper[self.paperFields.VENUE]
                    if currentVenue not in currentPaperCategoryInfo[self.paperCategoryFields.VENUE_LIST]:
                        currentPaperCategoryInfo[self.paperCategoryFields.VENUE_LIST].append(currentVenue)
                self.paperCategoryInfo[currentPaperCode] = currentPaperCategoryInfo
            else:
                currentPaperCategoryInfo = {}
                currentPaperCategoryInfo[self.paperCategoryFields.PAPER_CODE] = currentPaperCode
                currentPaperCategoryInfo[self.paperCategoryFields.PAPER_CATEGORY] = self.paperCategoryName[currentPaperCode]
                currentPaperCategoryInfo[self.paperCategoryFields.PAPER_LIST] = []
                currentPaperCategoryInfo[self.paperCategoryFields.PAPER_LIST].append(currentPaper[self.paperFields.AAN_ID])
                
                currentPaperCategoryInfo[self.paperCategoryFields.VENUE_LIST] = []
                currentVenue = currentPaper[self.paperFields.VENUE]
                if currentVenue not in currentPaperCategoryInfo[self.paperCategoryFields.VENUE_LIST]:
                    currentPaperCategoryInfo[self.paperCategoryFields.VENUE_LIST].append(currentVenue)
                self.paperCategoryInfo[currentPaperCode] = currentPaperCategoryInfo
                
                
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Provide path to the aan folder "
        print "Python Aan2Mongo.py <path-to-aan-folder>"
        sys.exit(1)
    startTime = datetime.now()
    print "Logging: "+str(startTime.date())
    path = sys.argv[1]
    porter = Aan2Mongo(path)
    print "Starting load process: "+str(startTime)
    porter.portData()
    endTime = datetime.now()
    print "\nLoad Complete!!"
    print "Time taken: "+str((endTime - startTime))
    
        