
import sys, pymongo, os, logging
from time import sleep
from os.path import exists
from threading import Thread
from pymongo.errors import CursorNotFound
import utils


class ThreadedDocumentProcessor:
    def __init__(self, collection, numberOfThreads, query={}):

        self.numberOfThreads = numberOfThreads
        self.errorFile = 'error.log'
        self.outputFile = 'output.json'

        logging.basicConfig(filename=self.errorFile, level=logging.DEBUG, format='%(levelname)s %(name)s %(message)s')
        self.errorLogger = logging.getLogger(__name__)
        logging.basicConfig(filename=self.outputFile, level=logging.INFO, format='')
        self.outputLogger = logging.getLogger(__name__)

        mongo = pymongo.MongoClient('mongodb://lattice-100:27018/')
        db = mongo['sustaindb']
        self.collection = db[collection]
        self.numberOfDocuments = self.collection.count()
        self.query = query
    

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

        documentsProcessedByThisThread = utils.numberOfDocumentsProcessedByThisThread(progressFile)        
        nextDocumentForThisThread = utils.nextDocumentForThisThread(progressFile, threadNumber, self.numberOfThreads)
        totalDocumentsForThisThread = utils.totalNumberOfDocumentsThisThreadMustProcess(threadNumber, self.numberOfDocuments, self.numberOfThreads)

        cursor = self.collection.find(self.query, no_cursor_timeout=True).skip(nextDocumentForThisThread-1)

        try:
            while cursor.has_next():
                document = cursor.next()
                ThreadedDocumentProcessor.processDocument(self, document, self.outputLogger)
                utils.logProgress(documentsProcessedByThisThread, totalDocumentsForThisThread, nextDocumentForThisThread, threadNumber, progressFile)
                documentsProcessedByThisThread += 1
                nextDocumentForThisThread += self.numberOfThreads
                cursor.skip(self.numberOfThreads-1)
                        
        except CursorNotFound as e:
            utils.logError(self.errorLogger, e, threadNumber)
            ThreadedDocumentProcessor.iterateDocuments(self.collection, threadNumber, self.numberOfThreads)

        except Exception as e:
            utils.logError(self.errorLogger, e, threadNumber)
            cursor.close()
            sleep(5)
            ThreadedDocumentProcessor.iterateDocuments(self.collection, threadNumber, self.numberOfThreads)
            
        cursor.close()
        completionMessage = f'{utils.getTimestamp()} [Thread-{threadNumber}] Completed'
        with open(progressFile, 'a') as f:
            f.write(completionMessage)
            print(completionMessage)

    def processDocument(self, document, outputLogger):
        raise NotImplementedError('Implement processDocument()')
