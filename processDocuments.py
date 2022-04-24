
import sys
from ThreadedDocumentProcessor import ThreadedDocumentProcessor

class DocumentProcessor(ThreadedDocumentProcessor):
    def __init__(self, collection, number_of_threads, query):
        super().__init__(collection, number_of_threads, query, DocumentProcessor.processDocument)

    def processDocument(self, document):
        '''
        This is the function that will be called by each thread on each document.
        If this function returns something, it must be a dictionary. 
        Said dictionary will be written in JSON format to the output.json file.

        Update this function to perform whatever actions you need to on each document.
        '''
        state_name =  document['properties']['NAME']
        state_code = document['properties']['STUSPS']
        return {state_name: state_code}


def main(collection, number_of_threads):
    query = {} # Update the `query` field to specify a mongo query
    documentProcessor = DocumentProcessor(collection, number_of_threads, query)
    documentProcessor.run()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(f'Usage: python3 {sys.argv[0]} <collection_to_iterate> <number_of_threads>')
    collection = sys.argv[1]
    try:
        number_of_threads = int(sys.argv[2])
        main(collection, number_of_threads)
    except TypeError as e:
        print('Second arg must be thread number and must be an integer')
