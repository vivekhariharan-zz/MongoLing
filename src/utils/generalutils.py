
from os import listdir
from os.path import isfile, join, isdir


def get_dir_names(dirPath):
    """ Given a directory this function lists the directories in it """
    dirNames = []
    for f in listdir(dirPath):
        if isdir(join(dirPath,f)):
            dirNames.append(join(dirPath,f))
    
    return dirNames

def list_files(dirPath):
    """Given a directory it returns a list containing all the files in the directory """
    files = []
    for f in listdir(dirPath):
        if isfile(join(dirPath,f)):
            files.append(join(dirPath,f))
        if isdir(join(dirPath,f)):
            files.extend(list_files(join(dirPath,f)))
    
    return files