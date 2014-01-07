'''
Created on Jun 19, 2013

@author: masinoa
'''


import os

def createLoglogDirectory(logDirectory='../logs'):
    if not os.path.exists(logDirectory): os.makedirs(logDirectory)
    
def log(level, msg, logFile=None):
    if not logFile:
        createLoglogDirectory()
        logFile = open('{0}/log.txt'.format('../logs'), 'a')
    logFile.write('{0}: {1}\n'.format(level, msg))
        
def error(msg, logFile=None):
    log("ERROR", msg, logFile)
    
def info(msg, logFile=None):
    log("INFO", msg, logFile)
    
def warn(msg, logFile=None):
    log("WARNING", msg, logFile)