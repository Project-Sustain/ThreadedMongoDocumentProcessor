
import sys, pymongo, os, logging
from time import sleep
from os.path import exists
from threading import Thread
from pymongo.errors import CursorNotFound
import utils


class ThreadedDocumentProcessor:
    def __init__(self, collection, query, threadNumber, numberOfThreads):

        self.threadNumber = threadNumber
        self.numberOfThreads = numberOfThreads

        self.errorFile = 'error.log'
        self.outputFile = 'output.json'

        logging.basicConfig(filename=self.errorFile, level=logging.DEBUG, format='%(levelname)s %(name)s %(message)s')
        self.errorLogger = logging.getLogger(__name__)
        # Create output logger

        mongo = pymongo.MongoClient('mongodb://lattice-100:27018/')
        db = mongo['sustaindb']
        self.collection = db[collection]
        self.numberOfDocuments = self.collection.count()
        self.query = query
    

    def __main__():
        for i in range(1, self.numberOfThreads+1):
            thread = Thread(target=iterateDocuments, args=(i,))
            thread.start()


    def iterateDocuments():

        progressFile = os.path.expanduser('progressFiles/progress_'+str(self.threadNumber)+'.txt')

        if not exists(self.outputFile):
            with open(self.outputFile, 'a') as f:
                f.write('[\n')

        with open(self.progressFile, 'a') as f:
            pass

        documentStats = {
            'documentsProcessedByThisThread': utils.numberOfDocumentsProcessedByThisThread(progressFile),
            'nextDocumentForThisThread': utils.nextDocumentForThisThread(progressFile, self.threadNumber, self.numberOfThreads),
            'totalDocumentsForThisThread': utils.totalNumberOfDocumentsThisThreadMustProcess(self.threadNumber, self.numberOfDocuments, self.numberOfThreads),
            'threadNumber': self.threadNumber,
            'progressFile': progressFile,
            'outputFile': self.outputFile
        }

        cursor = self.collection.find(self.query, no_cursor_timeout=True).skip(documentStats.nextDocumentForThisThread-1)

        try:
            while cursor.has_next():
                document = cursor.next()
                processDocument(document, documentStats, self.logger)
                utils.logProgress(documentStats)
                documentStats.documentsProcessedByThisThread += 1
                documentStats.nextDocumentForThisThread += self.numberOfThreads
                cursor.skip(NUM_THREADS-1)
                        
        except CursorNotFound as e:
            utils.logError(self.logger, e, self.threadNumber)
            iterateDocuments(collection, self.threadNumber, self.numberOfThreads)

        except Exception as e:
            utils.logError(self.logger, e, self.threadNumber)
            cursor.close()
            sleep(5)
            iterateDocuments(collection, self.threadNumber, self.numberOfThreads)
            
        cursor.close()
        utils.formatEndOfFile(self.outputFile)

    def processDocument(document, documentStats, logger):
        raise NotImplementedError('Implement processDocument()')
