#!/usr/bin/env python
# coding=utf-8

from emoticon_utilities.string_utils import normalizeEmoticonName
from lucene import \
    Integer, QueryParser, IndexSearcher, WhitespaceAnalyzer, FSDirectory, Hit, \
    VERSION, initVM, CLASSPATH, NumericRangeFilter, MatchAllDocsQuery, PrefixQuery, \
    QueryFilter, Term, BooleanFilter, FilterClause, BooleanClause, BooleanQuery

from operator import itemgetter
import json, string, time

def getBaselineStatistics():
    docsfile = gzip.open("/Volumes/Luna/twitter_germans/tweets.txt.gz",)
    lctr = 0
    linecutoff = 50000000
    all_users_set = {}
    for line in docsfile:
        lctr+=1
        if lctr%100000 == 0: print "on line: ", lctr, " at: ", time.time()
        if lctr > linecutoff: break
        tweet_id, user_id, date, tweet_id_replied, user_id_replied, source, some_flag, another_flag, location, text = unicode(line, 'utf-8').split('\t')
        all_users_set[user_id] = all_users_set.get(user_id,1)
    baseline_stats_text_file = open("/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/emoticon_diffusion_stats.txt","w")
    baseline_stats_text_file.write("%s\n" % (len(all_users_set)))
    baseline_stats_text_file.close()

def calculateEmoticonDiffusion(emoticon, searcher, analyzer, usage_threshold = 1, comm_threshold = 1):
    emoticon_stats_file = open("/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/emoticon_diffusion_stats.json","r") 
    total_users = int(emoticon_stats_file.read().strip())
    emoticon_stats_file.close()

    emoticon_file_name = raw_stats_dir + normalizeEmoticonName(emoticon).rstrip('_')+".diffusion"
    print "Calculating Diffusion for: ", emoticon, " at: ", time.time()
    escaped_emoticon = QueryParser.escape(emoticon)
    query = QueryParser("emoticons", analyzer).parse(escaped_emoticon)
    hits = searcher.search(query)
    print "%s total matching documents." % hits.length()
    if hits.length() == 0: return

    print " compiling diffusion stats at: ", time.time()
    emoticon_users_by_time_hash = {}
    emoticon_users_adopters_hash = {}
    emoticon_users_non_adopters_hash = {}
    try:
        hctr = 0
        for hit in hits:
            hctr += 1
            if hctr%100000==0: print "on hit: ", hctr
            if hctr == hits.length(): break
            uid, timestamp, country, emoticons, user_id_replied = hit.get("user_id"), hit.get("timestamp"), hit.get('country'), hit.get('emoticons'), hit.get('user_id_replied')
            emoticon_users_by_time_hash[uid] = emoticon_users_by_time_hash.get(uid,[])+[timestamp]
    except Exception, e:
        print "failed to list hit: ", e

    for uid in emoticon_users_by_time_hash:
        emoticon_users_by_time_hash[uid] = sorted(emoticon_users_by_time_hash[uid])
        emoticon_users_adopters_hash[uid] = {'sequential':0, 'simultaneous':0}

    for uid in emoticon_users_by_time_hash:
        if len(emoticon_users_by_time_hash[uid]) < usage_threshold: continue
        uquery = QueryParser("user_id_replied", analyzer).parse(uid)
        uhits = searcher.search(query)
        if uhits.length() == 0: continue

        try:
            for uhit in uhits:
                user_replying, user_id_replied = hit.get("user_id"), hit.get('user_id_replied')
                if user_replying in emoticon_users_by_time_hash and len(emoticon_users_by_time_hash[user_replying]) >= usage_threshold \
                and emoticon_users_by_time_hash[user_replying][0] > emoticon_users_by_time_hash[uid][usage_threshold-1]:
                    emoticon_users_adopters_hash[user_replying]['sequential'] += 1
                elif user_replying in emoticon_users_by_time_hash and len(emoticon_users_by_time_hash[user_replying]) >= usage_threshold \
	            and emoticon_users_by_time_hash[user_replying][usage_threshold-1] > emoticon_users_by_time_hash[uid][usage_threshold-1]:
                    emoticon_users_adopters_hash[user_replying]['simultaneous'] += 1
                elif user_replying not in emoticon_users_by_time_hash:
                    emoticon_users_non_adopters_hash[user_replying] = emoticon_users_non_adopters_hash.get(user_replying,0)+1
        except Exception, e:
            print "failed to list hit: ", e

    #users who were exposed and adopted: 
    num_exposed_adopted = len([x for x in emoticon_users_adopters_hash if emoticon_users_adopters_hash[x]['sequential'] > 0])
    #users who were exposed and did not adopt: 
    num_exposed_not_adopted = len([x for x in emoticon_users_non_adopters_hash if emoticon_users_non_adopters_hash[x] > 0])
    #users who were not exposed and did adopt: 
    num_not_exposed_adopted = len([x for x in emoticon_users_adopters_hash if emoticon_users_adopters_hash[x]['sequential'] == 0])
    #users who were not exposed and did not adopt: 
    num_not_exposed_not_adopted = total_users - len(emoticon_users_adopters_hash) - len(emoticon_users_non_adopters_hash)

    emoticon_file = open(emoticon_file_name,'w')
    emoticon_file.write("%s,%s,%s,%s\n" % (num_exposed_adopted, num_exposed_not_adopted, num_not_exposed_adopted, num_not_exposed_not_adopted))        
    emoticon_file.close()
    print "done at: ", time.time()

if __name__ == '__main__':
    STORE_DIR =  "/Volumes/TerraFirma/SharedData/vdb5/lucene_index"
    initVM(CLASSPATH, maxheap='1024m')
    print 'lucene', VERSION
    directory = FSDirectory.getDirectory(STORE_DIR, False)
    searcher = IndexSearcher(directory)
    analyzer = WhitespaceAnalyzer()
    getBaselineStatistics()
    emoticon_list = [":)"]
    for prop_emoticon in emoticon_list: calculateEmoticonDiffusion(prop_emoticon, searcher, analyzer)
    searcher.close()
