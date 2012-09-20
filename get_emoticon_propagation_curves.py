#!/usr/bin/env python
from emoticon_utilities.string_utils import normalizeEmoticonName
from lucene import \
    Integer, QueryParser, IndexSearcher, WhitespaceAnalyzer, FSDirectory, Hit, \
    VERSION, initVM, CLASSPATH, NumericRangeFilter, MatchAllDocsQuery, PrefixQuery, \
    QueryFilter, Term, BooleanFilter, FilterClause, BooleanClause, BooleanQuery

from operator import itemgetter
import string, time

def getEmoticonPropagationCurves(emoticon, searcher, analyzer):
    raw_stats_dir = "/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/"
    emoticon_file_name = raw_stats_dir + normalizeEmoticonName(emoticon).rstrip('_')+".timehash"
    print "Searching for: ", emoticon, " at: ", time.time()
    escaped_emoticon = QueryParser.escape(emoticon)
    query = QueryParser("text", analyzer).parse(escaped_emoticon)
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
            if hctr > 10000: break
            if hctr%10000==0: print "on hit: ", hctr
            if hctr == hits.length(): break
            uid, timestamp, country, emoticons = hit.get("user_id"), hit.get("timestamp"), hit.get('country'), hit.get('emoticons')
            countryset.add(country)
            timestruct = time.gmtime(int(timestamp))
            daysincestart = (timestruct[0]-2005)*365+timestruct[7]
            daystartts = int(timestamp)-60*60*timestruct[3]-60*timestruct[4]-timestruct[5]
            nextdaystartts = daystartts+86400
            daytshash[daystartts] = {'days since start':daysincestart, 'next day ts':nextdaystartts}
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
    print "number of days to process: ", len(sorted_daytslist)
    for i, sorted_dayts in enumerate(sorted_daytslist):
        if i%100 == 0: print "on day number: ", i, " at: ", time.time()
        range_filter = NumericRangeFilter.newIntRange("timestamp", Integer(sorted_dayts), Integer(daytshash[sorted_dayts]['next day ts']), True, True)
        
        #all tweets in day range
        all_docs_query = MatchAllDocsQuery()
        tweets_in_range_search = searcher.search(all_docs_query, range_filter)
        num_tweets_in_range = tweets_in_range_search.length()

        #all tweets containing emoticons
        empty_term = Term("emoticons")
        empty_term_prefix = PrefixQuery(empty_term)
        all_emoticons_docs_query_filter = QueryFilter(empty_term_prefix)
        compound_filter = BooleanFilter()
        compound_filter.add(FilterClause(range_filter, BooleanClause.Occur.MUST))
        compound_filter.add(FilterClause(all_emoticons_docs_query_filter, BooleanClause.Occur.MUST))
        emoticon_tweets_in_range_search = searcher.search(all_docs_query, compound_filter)
        num_emoticon_tweets_in_range = emoticon_tweets_in_range_search.length()

        #all tweets containing "http" or "https"
        bq = BooleanQuery()
        http_str = QueryParser.escape("http://")
        http_query = QueryParser("emoticons", analyzer).parse(http_str)
        https_str = QueryParser.escape("https://")
        https_query = QueryParser("emoticons", analyzer).parse(https_str)
        bq.add(http_query, BooleanClause.Occur.MUST)
        bq.add(https_query, BooleanClause.Occur.MUST)
        bq_search = searcher.search(bq, range_filter)
        num_http_emoticons = bq_search.length()
    
        print "total tweets: ", num_tweets_in_range
        print "total emoticons: ", num_emoticon_tweets_in_range
        print "num_http_emoticons: ", num_http_emoticons

        emoticon_propagation_hash[daytshash[sorted_dayts]['days since start']]['total tweets'] = num_tweets_in_range
        emoticon_propagation_hash[daytshash[sorted_dayts]['days since start']]['total emoticon tweets'] = num_emoticon_tweets_in_range
        
        
    print "outputting propagation curve to flat file at: ", time.time()
    countrylist = list(countryset)
    emo_propagation_by_time = sorted(emoticon_propagation_hash.items(), key=itemgetter(0))
    emoticon_file = open(emoticon_file_name,'w')
    emoticon_file.write("day,"+",".join(countrylist)+",total,alltweets,emoticontweets\n")        
    for emo_day_entry in emo_propagation_by_time:
        emoticon_file.write(str(emo_day_entry[0])+","+",".join([str(emo_day_entry[1].get(ctry,0)) for ctry in countrylist]) + "," + \
                            str(emo_day_entry[1]["total"]) + "," + str(emo_day_entry[1]['total tweets']) + "," + \
                            str(emo_day_entry[1]["total emoticon tweets"]) + "\n")
    emoticon_file.close()
    print "done at: ", time.time()




if __name__ == '__main__':
    STORE_DIR =  "/Volumes/TerraFirma/SharedData/vdb5/lucene_index"
    initVM(CLASSPATH, maxheap='1024m')
    print 'lucene', VERSION
    directory = FSDirectory.getDirectory(STORE_DIR, False)
    searcher = IndexSearcher(directory)
    analyzer = WhitespaceAnalyzer()
    getEmoticonPropagationCurves(":)", searcher, analyzer)
    #getEmoticonPropagationCurves(":(", searcher, analyzer)
    #getEmoticonPropagationCurves("^_^", searcher, analyzer)
    #getEmoticonPropagationCurves(";)", searcher, analyzer)
    searcher.close()
