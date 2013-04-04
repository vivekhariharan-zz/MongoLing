
import generalutils
import io
import ConfigParser
import sys
import os
from shutil import copyfile
from datetime import datetime


class FileGrouper:
    
    def __init__(self, configFile):
        self.parseConfigFile(configFile)
        
        
    def parseConfigFile(self, configFile):
        """Parses the input elements from the config file """
        print "Loading Configuration"
        config = ConfigParser.RawConfigParser(allow_no_value = False)
        configFileReader = open(configFile, 'r')
        config.readfp(configFileReader)
        self.folderToGroup = config.get('filegrouper', 'input-folder').rstrip('/')
        self.checkAfter2k(config.get('filegrouper', 'only-after-2k').strip())
        self.outputFolder = config.get('filegrouper', 'output-folder').rstrip('/')
        
    def checkAfter2k(self, onlyAfterY2k):
        """Check only after 2000 is set """
        if onlyAfterY2k == "yes":
            print "Only grouping papers after 2000"
            self.isAfter2k = True
        else:
            print "grouping all papers"
            self.isAfter2k = False
            
            
    def runGrouping(self):
        """driving method the runs the process"""
        self.groupFolder()
        
    def groupFolder(self):
        """groups folder into output folder """
        print "Grouping Folders"
        folderName = self.folderToGroup.split('/')[len(self.folderToGroup.split('/'))-1]
        outputFolder = self.outputFolder
        if not os.path.exists(outputFolder):
            os.makedirs(outputFolder)
        
        listOfFiles = generalutils.list_files(self.folderToGroup)
        for currentFile in listOfFiles:
            fileName = currentFile.split('/')[len(currentFile.split('/'))-1]
            paperCode = fileName.split('-')[0]
            paperCategory = paperCode[0]
            paperYear = paperCode[1::]
            parsedDate = datetime.strptime(paperYear, "%y")
            
            currentPaperCategoryOutput = outputFolder+"/"+paperCategory
            if self.isAfter2k == True:
                #checking if file is after parsedDate 2000
                if parsedDate.year >= 2000:
                    if not os.path.exists(currentPaperCategoryOutput):
                        os.makedirs(currentPaperCategoryOutput)
                    copyfile(currentFile, currentPaperCategoryOutput+"/"+fileName)
            else:
                if not os.path.exists(currentPaperCategoryOutput):
                    os.makedirs(currentPaperCategoryOutput)
                copyfile(currentFile, currentPaperCategoryOutput+"/"+fileName)
        
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Please provide grouper.cfg file"
        sys.exit(1)
    print "FileGrouper Log:"
    startTime = datetime.now()
    print startTime.date()
    configFile = str(sys.argv[1])
    fileGrouper = FileGrouper(configFile)
    fileGrouper.runGrouping()
    endTime = datetime.now()
    print "Running Time: "+ str((endTime - startTime))