
import sys, os
from ThreadedDocumentProcessor import ThreadedDocumentProcessor

class DocumentProcessor(ThreadedDocumentProcessor):
    def __init__(self, collection, number_of_threads, query, restart):
        super().__init__(collection, number_of_threads, query, restart, DocumentProcessor.processDocument)

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


def main(collection, number_of_threads, restart=False):
    if not restart:
        parent_dir = os.getcwd()
        dir = 'progressFiles'
        path = os.path.join(parent_dir, dir)
        os.mkdir(path)
    query = {} # Update the `query` field to specify a mongo query
    documentProcessor = DocumentProcessor(collection, number_of_threads, query, restart)
    documentProcessor.run()


if __name__ == '__main__':
    # FIXME add input validation
    if len(sys.argv) == 3:
        collection = sys.argv[1]
        number_of_threads = int(sys.argv[2])
        main(collection, number_of_threads)
    if len(sys.argv) == 4:
        collection = sys.argv[1]
        restart = True
        number_of_threads = int(sys.argv[2])
        main(collection, number_of_threads, restart=True)
