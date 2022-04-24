
import sys
from ThreadedDocumentProcessor import ThreadedDocumentProcessor

class DocumentProcessor(ThreadedDocumentProcessor):
    def __init__(self, collection, numberOfThreads, query):
        super().__init__(collection, numberOfThreads, query, DocumentProcessor.processDocument)

    def processDocument(self, document):
        '''
        This is the function that will be called by each thread on each document.
        If this function returns something, it must be a dictionary. 
        Said dictionary will be written in JSON format to the output.json file.

        Update this function to perform whatever actions you need to on each document.
        '''
        return {'name': document['properties']['NAME']}


def main(collection, numberOfThreads):
    query = {} # Update the `query` field to specify a mongo query
    documentProcessor = DocumentProcessor(collection, numberOfThreads, query)
    documentProcessor.run()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(f'Usage: python3 {sys.argv[0]} <collection_to_iterate> <number_of_threads>')
    collection = sys.argv[1]
    numberOfThreads = sys.argv[2]
    main(collection, numberOfThreads)
