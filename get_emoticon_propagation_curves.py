#!/usr/bin/env python
from lucene import \
    Integer, QueryParser, IndexSearcher, WhitespaceAnalyzer, FSDirectory, Hit, \
    VERSION, initVM, CLASSPATH, NumericRangeFilter, MatchAllDocsQuery

from operator import itemgetter
import string, time

def getEmoticonPropagationCurves(searcher, analyzer):
    raw_stats_dir = "/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/"
    emoticon = ";)"
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
        elif echar == ';': emoticon_file_name += 'semicolon_'
    emoticon_file_name = emoticon_file_name.rstrip('_')+".timehash"
    print "Searching for: ", emoticon, " at: ", time.time()
    parsed_command = QueryParser.escape(emoticon)
    query = QueryParser("text", analyzer).parse(parsed_command)
    hits = searcher.search(query)
    print "%s total matching documents." % hits.length()
    if hits.length() == 0: return

    print " compiling propagation curve at: ", time.time()
    emoticon_propagation_hash = {}
    countryset = set()
    daytshash = {}
    try:
        hctr = 0
        for hit in hits:
            hctr += 1
            if hctr%10000==0: print "on hit: ", hctr
            if hctr == hits.length(): break
            uid, timestamp, country, emoticons = hit.get("user_id"), hit.get("timestamp"), hit.get('country'), hit.get('emoticons')
            countryset.add(country)
            timestruct = time.gmtime(int(timestamp))
            daysincestart = (timestruct[0]-2005)*365+timestruct[7]
            daystartts = int(timestamp)-60*60*timestruct[3]
            daytshash[daystartts] = daysincestart
            total_emoticon_count = string.count(emoticons, emoticon)
            if daysincestart in emoticon_propagation_hash:
                emoticon_propagation_hash[daysincestart]['total'] += total_emoticon_count
                emoticon_propagation_hash[daysincestart][country] = emoticon_propagation_hash[daysincestart].get(country,0) + total_emoticon_count
            else:
                emoticon_propagation_hash[daysincestart] = {'total':total_emoticon_count, country:total_emoticon_count}
    except Exception, e: 
        print "failed to list hit: ", e

    test_all_docs_query = MatchAllDocsQuery()
    all_tweets = searcher.search(test_all_docs_query)
    print "total tweet docs: ", all_tweets.length()
    #adding total tweets / day for normalization
    sorted_daytslist = sorted(daytshash.keys())
    for i, sorted_dayts in enumerate(sorted_daytslist):
        if i == len(sorted_daytslist)-1: continue
        #print "parsed_daytts: ", parsed_daytts, " parsed_nextdaytts: ", parsed_nextdaytts
        #range_filter = RangeFilter("timestamp", str(sorted_dayts), str(sorted_daytslist[i+1]), True, True)
        range_filter = NumericRangeFilter.newIntRange("timestamp", Integer(sorted_dayts), Integer(sorted_daytslist[i+1]), True, True)
        all_docs_query = MatchAllDocsQuery()
        tweets_in_range_search = searcher.search(all_docs_query, range_filter)
        num_tweets_in_range = tweets_in_range_search.length()

        all_emoticon_docs_query_text = "[* TO *]"
        all_emoticon_docs_query = QueryParser("emoticons", analyzer).parse(all_emoticon_docs_query_text)
        emoticon_tweets_in_range_search = searcher.search(all_emoticon_docs_query, range_filter)
        num_emoticon_tweets_in_range_search = emoticon_tweets_in_range_search.length()
        print "num tweets in range: ", num_tweets_in_range
        print "num emoticon tweets in range: ", num_emoticon_tweets_in_range
        emoticon_propagation_hash[daytshash[sorted_dayts]]['total tweets'] = num_tweets_in_range
        
        

    print "outputting propagation curve to flat file at: ", time.time()
    countrylist = list(countryset)
    emo_propagation_by_time = sorted(emoticon_propagation_hash.items(), key=itemgetter(0))
    emoticon_file = open(emoticon_file_name,'w')
    emoticon_file.write("day,"+",".join(countrylist)+",total,alltweets\n")        
    for emo_day_entry in emo_propagation_by_time:
        emoticon_file.write(str(emo_day_entry[0])+","+",".join([str(emo_day_entry[1].get(ctry,0)) for ctry in countrylist]) + "," + str(emo_day_entry[1]["total"]) + "," + str(emo_day_entry[1]['total tweets']) + "\n")
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
