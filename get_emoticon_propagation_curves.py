#!/usr/bin/env python
# coding=utf-8

from emoticon_utilities.string_utils import normalizeEmoticonName
from lucene import \
    Integer, QueryParser, IndexSearcher, WhitespaceAnalyzer, FSDirectory, Hit, \
    VERSION, initVM, CLASSPATH, NumericRangeFilter, MatchAllDocsQuery, PrefixQuery, \
    QueryFilter, Term, BooleanFilter, FilterClause, BooleanClause, BooleanQuery

from operator import itemgetter
import json, string, time

def getBaselineStatistics(searcher, analyzer):
    baseline_stats_hash = {}
    day_one = time.strptime("01 01 2005", "%d %m %Y")
    day_one_ts = int(time.mktime(day_one))
    max_day_ctr = 1830
    day_ctr = 0
    while day_ctr < max_day_ctr:
        if day_ctr%100 == 0: print "on day ctr: ", day_ctr, " at time: ", time.time()
        curr_day_ts = day_one_ts + 86400*day_ctr
        next_day_ts = day_one_ts + 86400*(day_ctr+1)
        day_ctr+=1

        range_filter = NumericRangeFilter.newIntRange("timestamp", Integer(curr_day_ts), Integer(next_day_ts), True, True)
        
        #all tweets in day range
        all_docs_query = MatchAllDocsQuery()
        tweets_in_range_search = searcher.search(all_docs_query, range_filter)
        num_tweets_in_range = tweets_in_range_search.length()

        #all tweets in day range US
        US_tweets_base_query = MatchAllDocsQuery()
        US_tweets_country_query = QueryParser("country", analyzer).parse("United.States")
        US_tweets_query_filter = QueryFilter(US_tweets_country_query)
        compound_filter_US_tweets = BooleanFilter()
        compound_filter_US_tweets.add(FilterClause(range_filter, BooleanClause.Occur.MUST))
        compound_filter_US_tweets.add(FilterClause(US_tweets_query_filter, BooleanClause.Occur.MUST))
        US_tweets_in_range_search = searcher.search(US_tweets_base_query, compound_filter_US_tweets)
        num_US_tweets_in_range = US_tweets_in_range_search.length()
        
        #all tweets in day range japan
        JP_tweets_base_query = MatchAllDocsQuery()
        JP_tweets_country_query = QueryParser("country", analyzer).parse("Japan")
        JP_tweets_query_filter = QueryFilter(JP_tweets_country_query)
        compound_filter_JP_tweets = BooleanFilter()
        compound_filter_JP_tweets.add(FilterClause(range_filter, BooleanClause.Occur.MUST))
        compound_filter_JP_tweets.add(FilterClause(JP_tweets_query_filter, BooleanClause.Occur.MUST))
        JP_tweets_in_range_search = searcher.search(JP_tweets_base_query, compound_filter_JP_tweets)
        num_JP_tweets_in_range = JP_tweets_in_range_search.length()
        
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
        bq.add(http_query, BooleanClause.Occur.SHOULD)
        bq.add(https_query, BooleanClause.Occur.SHOULD)
        bq_search = searcher.search(bq, range_filter)
        num_http_emoticons = bq_search.length()
        
        baseline_stats_hash[day_ctr] = {'total tweets':num_tweets_in_range, 'emoticons':num_emoticon_tweets_in_range, 'http':num_http_emoticons, 'US tweets':num_US_tweets_in_range, \
                                        'JP tweets':num_JP_tweets_in_range}

    baseline_stats_text_file = open("/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/emoticon_stats.txt","w")
    raw_stats_list = sorted(baseline_stats_hash.items(), key = lambda x: int(x[0]))
    baseline_stats_text_file.write("day total emoticons http US JP\n")
    for rs in raw_stats_list: baseline_stats_text_file.write("%s %s %s %s %s %s\n" %(rs[0], rs[1]["total tweets"], rs[1]["emoticons"], rs[1]["http"], rs[1]['US tweets'], \
                                                             rs[1]['JP tweets']))
    baseline_stats_text_file.close()
    baseline_stats_file = open("/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/emoticon_stats.json","w")
    baseline_stats_file.write(json.dumps(baseline_stats_hash))
    baseline_stats_file.close()

def getEmoticonPropagationCurves(emoticon, searcher, analyzer):
    raw_stats_dir = "/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/"
    emoticon_file_name = raw_stats_dir + normalizeEmoticonName(emoticon).rstrip('_')+".timehash"
    emoticon_stats_file = open("/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/emoticon_stats.json","r") 
    emoticon_stats_hash = json.loads(emoticon_stats_file.read())
    print "Searching for: ", emoticon, " at: ", time.time()
    escaped_emoticon = QueryParser.escape(emoticon)
    query = QueryParser("emoticons", analyzer).parse(escaped_emoticon)
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
            if hctr%100000==0: print "on hit: ", hctr
            if hctr == hits.length(): break
            uid, timestamp, country, emoticons, user_id_replied = hit.get("user_id"), hit.get("timestamp"), hit.get('country'), hit.get('emoticons'), hit.get('user_id_replied')
            num_replies = int(user_id_replied != '0')
            countryset.add(country)
            timestruct = time.gmtime(int(timestamp))
            daysincestart = (timestruct[0]-2005)*365+timestruct[7]
            daystartts = int(timestamp)-60*60*timestruct[3]-60*timestruct[4]-timestruct[5]
            nextdaystartts = daystartts+86400
            daytshash[daystartts] = {'days since start':daysincestart, 'next day ts':nextdaystartts}
            total_emoticon_count = string.count(emoticons, emoticon)
            if daysincestart in emoticon_propagation_hash:
                #emoticon_propagation_hash[daysincestart]['total'] += total_emoticon_count
                emoticon_propagation_hash[daysincestart]['total'] += 1
                #emoticon_propagation_hash[daysincestart][country] = emoticon_propagation_hash[daysincestart].get(country,0) + total_emoticon_count
                emoticon_propagation_hash[daysincestart][country] = emoticon_propagation_hash[daysincestart].get(country,0) + 1
                emoticon_propagation_hash[daysincestart]['total_in_replies'] += num_replies
            else:
                emoticon_propagation_hash[daysincestart] = {'total':total_emoticon_count, 'total_in_replies':num_replies, country:total_emoticon_count, \
                                                            'total tweets':0, 'total emoticon tweets':0, 'total http emoticons':0}
    except Exception, e: 
        print "failed to list hit: ", e

    #test_all_docs_query = MatchAllDocsQuery()
    #all_tweets = searcher.search(test_all_docs_query)
    #print "total tweet docs: ", all_tweets.length()
    #adding total tweets / day for normalization
    sorted_daytslist = sorted(daytshash.keys())
    print "number of days to process: ", len(sorted_daytslist)
    for i, sorted_dayts in enumerate(sorted_daytslist):
        if i%100 == 0: print "on day number: ", i, " at: ", time.time()

        emoticon_propagation_hash[daytshash[sorted_dayts]['days since start']]['total tweets'] = emoticon_stats_hash[str(daytshash[sorted_dayts]['days since start'])]['total tweets']
        emoticon_propagation_hash[daytshash[sorted_dayts]['days since start']]['total US tweets'] = emoticon_stats_hash[str(daytshash[sorted_dayts]['days since start'])]['US tweets']
        emoticon_propagation_hash[daytshash[sorted_dayts]['days since start']]['total JP tweets'] = emoticon_stats_hash[str(daytshash[sorted_dayts]['days since start'])]['JP tweets']
        emoticon_propagation_hash[daytshash[sorted_dayts]['days since start']]['total emoticon tweets'] = emoticon_stats_hash[str(daytshash[sorted_dayts]['days since start'])]['emoticons']
        emoticon_propagation_hash[daytshash[sorted_dayts]['days since start']]['total http emoticons'] = emoticon_stats_hash[str(daytshash[sorted_dayts]['days since start'])]['http']
        
    print "outputting propagation curve to flat file at: ", time.time()
    countrylist = list(countryset)
    emo_propagation_by_time = sorted(emoticon_propagation_hash.items(), key=itemgetter(0))
    emoticon_file = open(emoticon_file_name,'w')
    emoticon_file.write("day,"+",".join(countrylist)+",total,totalinreplies,alltweets,emoticontweets,httpemoticons,USemoticons,JPemoticons\n")        
    for emo_day_entry in emo_propagation_by_time:
        emoticon_file.write(str(emo_day_entry[0])+","+",".join([str(emo_day_entry[1].get(ctry,0)) for ctry in countrylist]) + "," + \
                            str(emo_day_entry[1]["total"]) + "," + str(emo_day_entry[1]["total_in_replies"]) + "," + str(emo_day_entry[1]['total tweets']) + "," + \
                            str(emo_day_entry[1]["total emoticon tweets"]) + "," + str(emo_day_entry[1]["total http emoticons"]) + \
                            str(emo_day_entry[1]["US tweets"]) + "," + str(emo_day_entry[1]["JP tweets"]) "\n")
    emoticon_file.close()
    print "done at: ", time.time()

if __name__ == '__main__':
    STORE_DIR =  "/Volumes/TerraFirma/SharedData/vdb5/lucene_index"
    initVM(CLASSPATH, maxheap='1024m')
    print 'lucene', VERSION
    directory = FSDirectory.getDirectory(STORE_DIR, False)
    searcher = IndexSearcher(directory)
    analyzer = WhitespaceAnalyzer()
    #getBaselineStatistics(searcher, analyzer)
    #emoticon_list = [":)", ":(", ";)", ":P", ":0", "^^", "TT", ":p", ":/", "^_^", "T_T"]
    #emoticon_list = [":)", ":(", ":'(", ":-|", "^^", "+_+", "-_-", "T_T"]
    emoticon_list = [":)",":D",":(",";)",":P","^^","^_^","-_-","T_T",":o","@_@"]
    for prop_emoticon in emoticon_list: getEmoticonPropagationCurves(prop_emoticon, searcher, analyzer)
    #getEmoticonPropagationCurves(":)", searcher, analyzer)
    #getEmoticonPropagationCurves(":(", searcher, analyzer)
    #getEmoticonPropagationCurves("^_^", searcher, analyzer)
    #getEmoticonPropagationCurves(";)", searcher, analyzer)
    #getEmoticonPropagationCurves("TT", searcher, analyzer)
    #getEmoticonPropagationCurves("=^", searcher, analyzer)
    searcher.close()
