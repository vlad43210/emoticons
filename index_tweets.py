import lucene, sys, os, threading, time

from datetime import datetime
from lucene import WhitespaceAnalyzer, VERSION, initVM, Version
from index_files import IndexTweets

def getUserLocations():
    loc_hash = {}
    location_file = open('/Volumes/TerraFirma/SharedData/vdb5/emoticons/twitter_uid_country.txt','r')
    lctr = 0
    for line in location_file:
        lctr+=1
        if lctr%1000000==0: print "grabbing location data for line: ", lctr, "at time: ", time.time()
        uid, country = line.split('|')
        loc_hash[uid] = country.strip()
    return loc_hash


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print IndexTweets.__doc__
        sys.exit(1)
    env=initVM()
    print 'lucene', VERSION

    location_hash = getUserLocations()
    def fn():
        env.attachCurrentThread()
        start = datetime.now()
        ix = IndexTweets(sys.argv[1], "/Volumes/TerraFirma/SharedData/vdb5/lucene_index",
                   WhitespaceAnalyzer(Version.LUCENE_CURRENT), location_hash)
        ix.runIndexer()
        end = datetime.now()
        print end - start

    threading.Thread(target=fn).start()
