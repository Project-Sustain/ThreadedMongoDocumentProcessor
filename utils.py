
import os, logging, json
from datetime import datetime


def getJSON(file):
    f = open(file)
    jsonObject = json.load(f)
    f.close()
    return jsonObject


def documentShouldBeProcessedByThisThread(thread_number, document_number, number_of_threads):
    if document_number > number_of_threads:
        return thread_number == (document_number % number_of_threads) + 1
    else:
        return thread_number == document_number


def totalNumberOfDocumentsThisThreadMustProcess(thread_number, total_documents, number_of_threads):
    generic_total = total_documents // number_of_threads
    leftover = total_documents % number_of_threads
    if thread_number <= leftover:
        return generic_total + 1
    else:
        return generic_total


def getTimestamp():
    return '[' + datetime.now().strftime("%m/%d/%Y %H:%M:%S") + ']'


def logProgress(documents_processed_by_this_thread, total_documents_for_this_thread, thread_number, document_number):
    percent_done = round((documents_processed_by_this_thread / (total_documents_for_this_thread)) * 100, 5)
    progress_message = f'{getTimestamp()} [Thread-{thread_number}] {percent_done}% {documents_processed_by_this_thread}/{total_documents_for_this_thread} Document {document_number}'
    print(progress_message)


def logError(logger, e, thread_number):
    error_message = f'{getTimestamp()} [Thread-{thread_number}] {e}'
    logger.log(logging.ERROR, error_message)
    print(error_message)


# def formatEndOfFile(file): # FIX THIS
#     with open(file, 'ab+') as f:   
#         f.seek(-2, os.SEEK_END)
#         f.truncate()
#         f.write('\n]')
