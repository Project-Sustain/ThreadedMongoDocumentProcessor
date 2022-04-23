
'''
This example usage iterates the `state_geo` collection with 3 threads and 
writes names to the output file
'''

from ThreadedDocumentProcessor import ThreadedDocumentProcessor

class WaterQualityDataDistributor(ThreadedDocumentProcessor):
    def __init__(self, collection, numberOfThreads, query):
        super().__init__(collection, numberOfThreads, query, WaterQualityDataDistributor.addDataToCollections)

    def addDataToCollections(self, document):
        return {'name': document['properties']['NAME']}


def main():
    dataDistributor = WaterQualityDataDistributor('state_geo', 3, {})
    dataDistributor.run()


if __name__ == '__main__':
    main()
