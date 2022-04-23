
'''
This class can be used to iterate mongodb collections with multiple threads
See the exampleUsage.py class for an simple example

Dependencies: python3 -m pip install --user pymongo
'''

from abc import ABC
import json
import pymongo, os, logging
from time import sleep
from os.path import exists
from threading import Thread, Lock
from pymongo.errors import CursorNotFound
import utils


class ThreadedDocumentProcessor(ABC):

    def __init__(self, collectionName, numberOfThreads, query, processDocument=lambda self, document: {'id': str(document['_id'])}):

        self.processDocument = processDocument
        self.lock = Lock()
        self.collectionName = collectionName
        self.numberOfThreads = numberOfThreads
        self.errorFile = 'error.log'
        self.outputFile = 'output.json'

        logging.basicConfig(filename=self.errorFile, level=logging.DEBUG, format='%(levelname)s %(name)s %(message)s')
        self.errorLogger = logging.getLogger(__name__)
        logging.basicConfig(filename=self.outputFile, level=logging.INFO, format='')
        self.outputLogger = logging.getLogger(__name__)
        
        mongo = pymongo.MongoClient('mongodb://lattice-100:27018/')
        self.db = mongo['sustaindb']
        self.query = query
        self.numberOfDocuments = self.db[collectionName].count_documents(query)

    

    def run(self):
        for i in range(1, self.numberOfThreads+1):
            thread = Thread(target=ThreadedDocumentProcessor.iterateDocuments, args=(self, i))
            thread.start()


    def iterateDocuments(self, threadNumber):

        progressFile = os.path.expanduser('progressFiles/progress_'+str(threadNumber)+'.txt')

        if not exists(self.outputFile):
            with open(self.outputFile, 'a') as f:
                f.write('[\n')

        with open(progressFile, 'a') as f:
            pass

        lastAbsoluteDocumentNumberProcessedByThisThread = utils.lastAbsoluteDocumentNumberProcessedByThisThread(progressFile)
        documentNumber = lastAbsoluteDocumentNumberProcessedByThisThread
        documentsProcessedByThisThread = utils.numberOfDocumentsProcessedByThisThread(progressFile)        
        totalDocumentsForThisThread = utils.totalNumberOfDocumentsThisThreadMustProcess(threadNumber, self.numberOfDocuments, self.numberOfThreads)
        
        cursor = self.db[self.collectionName].find(self.query, no_cursor_timeout=True).skip(lastAbsoluteDocumentNumberProcessedByThisThread)

        try:
            for document in cursor:

                documentNumber += 1
                if utils.documentShouldBeProcessedByThisThread(threadNumber, documentNumber, self.numberOfThreads):
                    
                    try:
                        objectToWrite = self.processDocument(self, document)
                        if objectToWrite:
                            with self.lock:
                                with open(self.outputFile, 'a') as f:
                                    f.write(json.dumps(objectToWrite))
                                    f.write(',\n')

                    except Exception as e:
                        utils.logError(self.errorLogger, e, threadNumber)

                    documentsProcessedByThisThread += 1
                    utils.logProgress(documentsProcessedByThisThread, totalDocumentsForThisThread, threadNumber, progressFile, documentNumber)
                        
        except CursorNotFound as e:
            utils.logError(self.errorLogger, e, threadNumber)
            ThreadedDocumentProcessor.iterateDocuments(self, threadNumber)

        except Exception as e:
            utils.logError(self.errorLogger, e, threadNumber)
            cursor.close()
            sleep(5)
            ThreadedDocumentProcessor.iterateDocuments(self, threadNumber)
            
        cursor.close()
        completionMessage = f'{utils.getTimestamp()} [Thread-{threadNumber}] Completed'
        with open(progressFile, 'a') as f:
            f.write(completionMessage)
            print(completionMessage)

