
import os, logging, json
from datetime import datetime


def getJSON(file):
    f = open(file)
    jsonObject = json.load(f)
    f.close()
    return jsonObject


def numberOfDocumentsProcessedByThisThread(file):
    try:
        with open(file, 'r') as f:
            return int(f.readlines()[-1].split(' ')[4].split('/')[0])
    except:
        return 0


def nextDocumentForThisThread(file, threadNumber, NUM_THREADS):
    try:
        with open(file, 'r') as f:
            return int(f.readlines()[-1].split(' ')[6]) + NUM_THREADS
    except:
        return threadNumber


def totalNumberOfDocumentsThisThreadMustProcess(threadNumber, totalDocuments, NUM_THREADS):
    genericTotal = totalDocuments // NUM_THREADS
    leftover = totalDocuments % NUM_THREADS
    if threadNumber <= leftover:
        return genericTotal + 1
    else:
        return genericTotal


def getTimestamp():
    return '[' + datetime.now().strftime("%m/%d/%Y %H:%M:%S") + ']'


def logProgress(documentsProcessedByThisThread, totalDocumentsForThisThread, nextDocumentForThisThread, threadNumber, outputFile):
    percent_done = round((documentsProcessedByThisThread+1 / (totalDocumentsForThisThread)) * 100, 6)
    message = f'{getTimestamp()} [Thread-{threadNumber}] {percent_done}% {documentsProcessedByThisThread+1}/{totalDocumentsForThisThread} Document {nextDocumentForThisThread}'
    print(message)
    with open(outputFile, 'a') as output:
        output.write(message + '\n')


def logError(logger, e, threadNumber):
    errorMessage = f'{getTimestamp()} [Thread-{threadNumber}] {e}'
    logger.log(logging.ERROR, errorMessage)
    print(errorMessage)


def formatEndOfFile(file): # FIX THIS
    with open(file, 'wb+') as f:   
        f.seek(-2, os.SEEK_END)
        f.truncate()
        f.write('\n]')
