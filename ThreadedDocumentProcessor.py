
from abc import ABC
import pymongo, os, logging, json
from time import sleep
from os.path import exists
from threading import Thread, Lock
from pymongo.errors import CursorNotFound
import utils


class ThreadedDocumentProcessor(ABC):

    def __init__(self, collectionName, numberOfThreads, query, processDocumentFunction):

        self.processDocument = processDocumentFunction
        self.lock = Lock()
        self.collectionName = collectionName
        self.numberOfThreads = numberOfThreads
        self.errorFile = 'error.log'
        self.outputFile = 'output.json'

        logging.basicConfig(filename=self.errorFile, level=logging.DEBUG, format='%(levelname)s %(name)s %(message)s')
        self.errorLogger = logging.getLogger(__name__)
        
        mongo = pymongo.MongoClient('mongodb://lattice-100:27018/')
        self.db = mongo['sustaindb']
        self.query = query
        self.numberOfDocuments = self.db[collectionName].count_documents(query)

    
    def run(self):
        for i in range(1, self.numberOfThreads+1):
            thread = Thread(target=ThreadedDocumentProcessor.iterateDocuments, args=(self, i))
            thread.start()


    def iterateDocuments(self, threadNumber, documentNumber=0, documentsProcessedByThisThread=0):

        if not exists(self.outputFile):
            with open(self.outputFile, 'a') as f:
                f.write('[\n')
      
        totalDocumentsForThisThread = utils.totalNumberOfDocumentsThisThreadMustProcess(threadNumber, self.numberOfDocuments, self.numberOfThreads)
        cursor = self.db[self.collectionName].find(self.query, no_cursor_timeout=True).skip(documentNumber)

        try:
            for document in cursor:
                documentNumber += 1

                if utils.documentShouldBeProcessedByThisThread(threadNumber, documentNumber, self.numberOfThreads):
                    try:
                        objectToWrite = self.processDocument(self, document) # This is where we call the `processDocument()` fuction written in `processDocuments.py`
                        if objectToWrite: # If your `processDocument()` function returns a dictionary, write it to the output file
                            with self.lock: # Thread-safe access to the output file
                                with open(self.outputFile, 'a') as f:
                                    f.write(json.dumps(objectToWrite))
                                    f.write(',\n')

                    except Exception as e:
                        utils.logError(self.errorLogger, e, threadNumber)

                    documentsProcessedByThisThread += 1
                    utils.logProgress(documentsProcessedByThisThread, totalDocumentsForThisThread, threadNumber, documentNumber)
                        
        except CursorNotFound as e:
            utils.logError(self.errorLogger, e, threadNumber)
            ThreadedDocumentProcessor.iterateDocuments(self, threadNumber, documentNumber=documentNumber, documentsProcessedByThisThread=documentsProcessedByThisThread)

        except Exception as e:
            utils.logError(self.errorLogger, e, threadNumber)
            cursor.close()
            sleep(5)
            ThreadedDocumentProcessor.iterateDocuments(self, threadNumber, documentNumber=documentNumber, documentsProcessedByThisThread=documentsProcessedByThisThread)
            
        cursor.close()

        print(f'{utils.getTimestamp()} [Thread-{threadNumber}] Completed')
