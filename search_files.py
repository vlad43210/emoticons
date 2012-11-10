#!/usr/bin/env python
from lucene import \
    QueryParser, IndexSearcher, WhitespaceAnalyzer, FSDirectory, Hit, \
    VERSION, initVM, CLASSPATH


"""
This script is loosely based on the Lucene (java implementation) demo class 
org.apache.lucene.demo.SearchFiles.  It will prompt for a search query, then it
will search the Lucene index in the current directory called 'index' for the
search query entered against the 'contents' field.  It will then display the
'path' and 'name' fields for each of the hits it finds in the index.  Note that
search.close() is currently commented out because it causes a stack overflow in
some cases.
"""
def run(searcher, analyzer):
    while True:
        print
        print "Hit enter with no input to quit."
        command = raw_input("Query:")
        if command == '':
            return

        print
        print "Searching for:", command
        parsed_command = QueryParser.escape(command)
        query = QueryParser("text", analyzer).parse(parsed_command)
        hits = searcher.search(query)
        print "%s total matching documents." % hits.length()

        try:
            for hit in hits:
                print 'uid:', hit.get("user_id"), 'timestamp: ', hit.get("timestamp"), "country: ", hit.get('country'), "emoticons: ", hit.get('emoticons')
        except Exception, e: 
            print "failed to list hit: ", e

        print
        command = raw_input("Query:")
        parsed_command = QueryParser.escape(command)
        print "Searching for emoticon:", parsed_command
        query = QueryParser("emoticons", analyzer).parse(parsed_command)
        hits = searcher.search(query)
        print "%s total matching documents." % hits.length()

        try:
            for hit in hits:
                print 'uid:', hit.get("user_id"), 'timestamp: ', hit.get("timestamp"), "country: ", hit.get('country'), "emoticons: ", hit.get('emoticons')
        except Exception, e: 
            print "failed to list hit: ", e

        print
        command = raw_input("Query:")
        parsed_command = QueryParser.escape(command)
        print "Searching for uid:", parsed_command
        query = QueryParser("user_id", analyzer).parse(parsed_command)
        hits = searcher.search(query)
        print "%s total matching documents." % hits.length()

        try:
            for hit in hits:
                print 'uid:', hit.get("user_id"), 'timestamp: ', hit.get("timestamp"), "country: ", hit.get('country'), "emoticons: ", hit.get('emoticons')
        except Exception, e: 
            print "failed to list hit: ", e


if __name__ == '__main__':
    #STORE_DIR =  "/Volumes/TerraFirma/SharedData/vdb5/lucene_index"
    STORE_DIR =  "/Volumes/TerraFirma/SharedData/vdb5/lucene_index_test"
    initVM(CLASSPATH)
    print 'lucene', VERSION
    directory = FSDirectory.getDirectory(STORE_DIR, False)
    searcher = IndexSearcher(directory)
    analyzer = WhitespaceAnalyzer()
    run(searcher, analyzer)
    searcher.close()
