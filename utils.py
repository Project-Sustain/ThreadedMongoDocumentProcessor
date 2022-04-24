
import os, logging, json
from datetime import datetime


def getJSON(file):
    f = open(file)
    jsonObject = json.load(f)
    f.close()
    return jsonObject


def documentShouldBeProcessedByThisThread(threadNumber, documentNumber, numberOfThreads):
    if documentNumber > numberOfThreads:
        return threadNumber == (documentNumber % numberOfThreads) + 1
    else:
        return threadNumber == documentNumber


def totalNumberOfDocumentsThisThreadMustProcess(threadNumber, totalDocuments, NUM_THREADS):
    genericTotal = totalDocuments // NUM_THREADS
    leftover = totalDocuments % NUM_THREADS
    if threadNumber <= leftover:
        return genericTotal + 1
    else:
        return genericTotal


def getTimestamp():
    return '[' + datetime.now().strftime("%m/%d/%Y %H:%M:%S") + ']'


def logProgress(documentsProcessedByThisThread, totalDocumentsForThisThread, threadNumber, documentNumber):
    percent_done = round((documentsProcessedByThisThread / (totalDocumentsForThisThread)) * 100, 5)
    progressMessage = f'{getTimestamp()} [Thread-{threadNumber}] {percent_done}% {documentsProcessedByThisThread}/{totalDocumentsForThisThread} Document {documentNumber}'
    print(progressMessage)


def logError(logger, e, threadNumber):
    errorMessage = f'{getTimestamp()} [Thread-{threadNumber}] {e}'
    logger.log(logging.ERROR, errorMessage)
    print(errorMessage)


# def formatEndOfFile(file): # FIX THIS
#     with open(file, 'ab+') as f:   
#         f.seek(-2, os.SEEK_END)
#         f.truncate()
#         f.write('\n]')
