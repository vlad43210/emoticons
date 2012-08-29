#!/usr/bin/env python
from lucene import \
    QueryParser, IndexSearcher, WhitespaceAnalyzer, FSDirectory, Hit, \
    VERSION, initVM, CLASSPATH, RangeFilter, MatchAllDocsQuery

from operator import itemgetter
import string, time

def getEmoticonPropagationCurves(searcher, analyzer):
    raw_stats_dir = "/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/"
    emoticon = ">.<"
    emoticon_file_name = raw_stats_dir
    for echar in emoticon:
        if echar == ':': emoticon_file_name += 'colon_'
        elif echar == ')': emoticon_file_name += 'rparen_'
        elif echar == '(': emoticon_file_name += 'lparen_'
        elif echar == '^': emoticon_file_name += 'carrot_'
        elif echar == '_': emoticon_file_name += 'underscore_'
        elif echar == '>': emoticon_file_name += 'greaterthan_'
        elif echar == '<': emoticon_file_name += 'lessthan_'
        elif echar == '.': emoticon_file_name += 'dot_'
    emoticon_file_name = emoticon_file_name.rstrip('_')+".timehash"
    print "Searching for: ", emoticon, " at: ", time.time()
    parsed_command = QueryParser.escape(emoticon)
    query = QueryParser("text", analyzer).parse(parsed_command)
    hits = searcher.search(query)
    print "%s total matching documents." % hits.length()

    print " compiling propagation curve at: ", time.time()
    emoticon_propagation_hash = {}
    countryset = set()
    daytshash = {}
    try:
        hctr = 0
        for hit in hits:
            hctr += 1
            if hctr == hits.length(): break
            uid, timestamp, country, emoticons = hit.get("user_id"), hit.get("timestamp"), hit.get('country'), hit.get('emoticons')
            countryset.add(country)
            timestruct = time.gmtime(int(timestamp))
            daysincestart = (timestruct[0]-2005)*365+timestruct[7]
            daystartts = int(timestamp)-60*60*timestruct[3]
            daytshash[daystartts] = daysincestrart
            total_emoticon_count = string.count(emoticons, emoticon)
            if daysincestart in emoticon_propagation_hash:
                emoticon_propagation_hash[daysincestart]['total'] += total_emoticon_count
                emoticon_propagation_hash[daysincestart][country] = emoticon_propagation_hash[daysincestart].get(country,0) + total_emoticon_count
            else:
                emoticon_propagation_hash[daysincestart] = {'total':total_emoticon_count, country:total_emoticon_count}
    except Exception, e: 
        print "failed to list hit: ", e

    sorted_daytslist = sorted(daytshash.keys())
    for i, sorted_dayts in enumerate(sorted_daytslist):
        if i == len(sorted_daytslist)-1: continue
        parsed_daytts = QueryParser.escape(sorted_dayts)
        range_filter = RangeFilter("timestamp", sorted_dayts, sorted_daytslist[i+1], True, True)
        all_docs_query = MatchAllDocsQuery()
        tweets_in_range_search = searcher.search(all_docs_query, range_filter)
        num_tweets_in_range = tweets_in_range_search.length()
        emoticon_propagation_hash[daytshash[sorted_dayts]]['total tweets'] = num_tweets_in_range
        
        

    print "outputting propagation curve to flat file at: ", time.time()
    countrylist = list(countryset)
    emo_propagation_by_time = sorted(emoticon_propagation_hash.items(), key=itemgetter(0))
    emoticon_file = open(emoticon_file_name,'w')
    emoticon_file.write("day,"+",".join(countrylist)+",total\n")        
    for emo_day_entry in emo_propagation_by_time:
        emoticon_file.write(str(emo_day_entry[0])+","+",".join([str(emo_day_entry[1].get(ctry,0)) for ctry in countrylist]) + "," + str(emo_day_entry[1]["total"]) + "\n")
    emoticon_file.close()
    print "done at: ", time.time()




if __name__ == '__main__':
    STORE_DIR =  "/Volumes/TerraFirma/SharedData/vdb5/lucene_index"
    initVM(CLASSPATH)
    print 'lucene', VERSION
    directory = FSDirectory.getDirectory(STORE_DIR, False)
    searcher = IndexSearcher(directory)
    analyzer = WhitespaceAnalyzer()
    getEmoticonPropagationCurves(searcher, analyzer)
    searcher.close()
