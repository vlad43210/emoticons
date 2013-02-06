#!/usr/bin/env python
# coding=utf-8

from emoticon_utilities.string_utils import normalizeEmoticonName
from lucene import \
    Integer, QueryParser, IndexSearcher, WhitespaceAnalyzer, FSDirectory, Hit, \
    VERSION, initVM, CLASSPATH, NumericRangeFilter, MatchAllDocsQuery, PrefixQuery, \
    QueryFilter, Term, BooleanFilter, FilterClause, BooleanClause, BooleanQuery

from operator import itemgetter
import gzip, json, string, time

def getBaselineStatistics():
    docsfile = gzip.open("/Volumes/Luna/twitter_germans/tweets.txt.gz")
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
    raw_stats_dir = "/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/"
    emoticon_stats_file = open("/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/emoticon_diffusion_stats.txt","r") 
    total_users = int(emoticon_stats_file.read().strip())
    emoticon_stats_file.close()

    emoticon_file_name = raw_stats_dir + normalizeEmoticonName(emoticon).rstrip('_')+".diffusion_bidir"
    print "Calculating Diffusion for: ", emoticon, " at: ", time.time()
    escaped_emoticon = QueryParser.escape(emoticon)
    query = QueryParser("emoticons", analyzer).parse(escaped_emoticon)
    hits = searcher.search(query)
    print "%s total matching documents." % hits.length()
    if hits.length() == 0: return

    print "compiling diffusion stats at: ", time.time()
    emoticon_users_by_time_hash = {}
    emoticon_users_adopters_hash = {}
    emoticon_users_non_adopters_hash = {}
    users_exposure_hash = {}
    reverse_users_exposure_hash = {}
    try:
        hctr = 0
        for hit in hits:
            hctr += 1
            if hctr%100000==0: print "on hit: ", hctr
            #if hctr > 100000: break
            if hctr == hits.length(): break
            uid, timestamp, country, emoticons, user_id_replied = hit.get("user_id"), int(hit.get("timestamp")), hit.get('country'), hit.get('emoticons'), hit.get('user_id_replied')
            emoticon_users_by_time_hash[uid] = emoticon_users_by_time_hash.get(uid,[])+[timestamp]
    except Exception, e:
        pass
        #print "failed to list hit: ", e

    if emoticon == ":P":
        ee_two = QueryParser.escape(":p")
    elif emoticon == "T_T":
        ee_two = QueryParser.escape("TT")
    elif emoticon == "^_^":
        ee_two = QueryParser.escape("^^")
    if emoticon in [":P","T_T","^_^"]:
        q_two = QueryParser("emoticons",analyzer).parse(ee_two)
        hits_two = searcher.search(q_two)
        try:
            hctr_two = 0
            for hit_two in hits_two:
                hctr_two += 1
                if hctr_two%100000==0: print "on hit: ", hctr_two
                #if hctr > 100000: break
                if hctr_two == hits_two.length(): break
                uid, timestamp, country, emoticons, user_id_replied = hit_two.get("user_id"), int(hit_two.get("timestamp")), hit_two.get('country'), hit_two.get('emoticons'), hit_two.get('user_id_replied')
                emoticon_users_by_time_hash[uid] = emoticon_users_by_time_hash.get(uid,[])+[timestamp]
        except Exception, e:
            pass

    print "making emoticon users by time hash at: ", time.time()
    for uid in emoticon_users_by_time_hash:
        emoticon_users_by_time_hash[uid] = sorted(emoticon_users_by_time_hash[uid])
        emoticon_users_adopters_hash[uid] = {'sequential':0, 'simultaneous':0}

    print "calculating sequential and simultaneous adoptions at: ", time.time()
    uidctr = 0
    number_users = len(emoticon_users_by_time_hash)
    for uid in emoticon_users_by_time_hash:
        uidctr += 1
        if uidctr > 1000: break
        #if uidctr < 5: print "uid: ", uid, " hash: ", emoticon_users_by_time_hash[uid]
        if uidctr%100==0: print "on uid number: ", uidctr, " out of: ", number_users, " at time: ", time.time()
        if len(emoticon_users_by_time_hash[uid]) < usage_threshold: continue
        uquery = QueryParser("user_id_replied", analyzer).parse(uid)
        uhits = searcher.search(uquery)
        if uhits.length() == 0: continue
        #print "uid replied: ", uid, " number of hits: ", uhits.length()

        try:
            for uhit in uhits:
                user_replying, user_id_replied, reply_timestamp = uhit.get("user_id"), uhit.get('user_id_replied'), int(uhit.get("timestamp"))
                replying_user_exposure_hash = users_exposure_hash.get(user_replying,{})
                replying_tstamp_set = replying_user_exposure_hash.get(uid,set())
                replying_tstamp_set.add(reply_timestamp)
                replying_user_exposure_hash[uid] = replying_tstamp_set
                users_exposure_hash[user_replying] = replying_user_exposure_hash
        except Exception, e:
            pass

        uquery_reverse = QueryParser("user_id", analyzer).parse(uid)
        uhits_reverse = searcher.search(uquery_reverse)
        if uhits_reverse.length() == 0: continue

        try:
            for uhit_reverse in uhits_reverse:
                user_replying, user_id_replied, reply_timestamp = uhit_reverse.get("user_id"), uhit_reverse.get('user_id_replied'), int(uhit_reverse.get("timestamp"))
                replied_user_exposure_hash = reverse_users_exposure_hash.get(user_id_replied,{})
                replied_tstamp_set = replied_user_exposure_hash.get(uid,set())
                replied_tstamp_set.add(reply_timestamp)
                replied_user_exposure_hash[uid] = replied_tstamp_setr
                reverse_users_exposure_hash[user_id_replied] = replied_user_exposure_hash
        except Exception, e:
                pass

    all_potentially_exposed_set = set(users_exposure_hash.keys()) | set(reverse_users_exposure_hash.keys())
    all_potentially_exposed_list = list(all_potentially_exposed_set)
    pe_user_ctr = 0
    all_pe_users = len(all_potentially_exposed_list)
    print "users exposure length: ", len(users_exposure_hash)
    print "reverse users exposure length: ", len(reverse_users_exposure_hash)
    print "all potentially exposed list: ", len(all_potentially_exposed_list)
 
    for potentially_exposed_user in all_potentially_exposed_list:
        pe_user_ctr += 1
        #if pe_user_ctr%100==0: print "on potentially exposed user number: ", pe_user_ctr, " out of: ", all_pe_users, "at time: ", time.time()
        #adopter
        if potentially_exposed_user in emoticon_users_by_time_hash and len(emoticon_users_by_time_hash[potentially_exposed_user]) >= usage_threshold:
            #potentially exposed both ways to some adopter
            #print "user: ", potentially_exposed_user, " has adopted!"
            if potentially_exposed_user in users_exposure_hash and potentially_exposed_user in reverse_users_exposure_hash:
                #print "user: ", potentially_exposed_user, " both exposed and reverse exposed!"
                for active_user in users_exposure_hash[potentially_exposed_user]:
                    '''
                    print "setting user sequential adopt for user: ", potentially_exposed_user
                    emoticon_users_adopters_hash[potentially_exposed_user]['sequential'] += 1
                    break                                     
                    if active_user in reverse_users_exposure_hash:
                        emoticon_users_adopters_hash[potentially_exposed_user]['sequential'] += 1
                        break
                    '''
                    if len(users_exposure_hash[potentially_exposed_user][active_user]) >= comm_threshold and \
                       sorted(list(users_exposure_hash[potentially_exposed_user][active_user]))[comm_threshold-1] \
                       <= emoticon_users_by_time_hash[potentially_exposed_user][0] and \
                       active_user in reverse_users_exposure_hash[potentially_exposed_user] and \
                       len(reverse_users_exposure_hash[potentially_exposed_user][active_user]) >= comm_threshold and \
                       sorted(list(reverse_users_exposure_hash[potentially_exposed_user][active_user]))[comm_threshold-1] \
                       <= emoticon_users_by_time_hash[potentially_exposed_user][0] and \
                       emoticon_users_by_time_hash[potentially_exposed_user][0] > emoticon_users_by_time_hash[active_user][usage_threshold-1]:
                        emoticon_users_adopters_hash[potentially_exposed_user]['sequential'] += 1
                        break
                    elif len(users_exposure_hash[potentially_exposed_user][active_user]) >= comm_threshold and \
                       sorted(list(users_exposure_hash[potentially_exposed_user][active_user]))[comm_threshold-1] \
                       <= emoticon_users_by_time_hash[potentially_exposed_user][comm_threshold-1] and \
                       active_user in reverse_users_exposure_hash[potentially_exposed_user] and \
                       len(reverse_users_exposure_hash[potentially_exposed_user][active_user]) >= comm_threshold and \
                       sorted(list(reverse_users_exposure_hash[potentially_exposed_user][active_user]))[comm_threshold-1] \
                       <= emoticon_users_by_time_hash[potentially_exposed_user][comm_threshold-1] and \
                       emoticon_users_by_time_hash[potentially_exposed_user][usage_threshold-1] > emoticon_users_by_time_hash[active_user][usage_threshold-1]:
                        emoticon_users_adopters_hash[potentially_exposed_user]['simultaneous'] += 1
        #non-adopter
        else:
            #potentially exposed both ways to adopter
            if potentially_exposed_user in users_exposure_hash and potentially_exposed_user in reverse_users_exposure_hash:
                #print "exposure hash: ", users_exposure_hash[potentially_exposed_user], "reverse exposure hash: ", reverse_users_exposure_hash[potentially_exposed_user]
                for active_user in users_exposure_hash[potentially_exposed_user]:
                    if len(users_exposure_hash[potentially_exposed_user][active_user]) >= comm_threshold and \
                       len(reverse_users_exposure_hash[potentially_exposed_user].get(active_user,[])) >= comm_threshold:
                        emoticon_users_non_adopters_hash[potentially_exposed_user] = emoticon_users_non_adopters_hash.get(potentially_exposed_user,0)+1
                        break

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
    #getBaselineStatistics()
    #emoticon_list = [":(", ";)", ":P", "^^", "TT", ":p", ":/", "^_^", "++"]
    #emoticon_list = [":)","^..^","^00^",":(",";)",":P",":D","^_^","-_-","T_T",":o","@_@","+_+"]
    #emoticon_list = [":P",":D","^_^","T_T"]
    emoticon_list = [":P"]
    for prop_emoticon in emoticon_list: calculateEmoticonDiffusion(prop_emoticon, searcher, analyzer, 3, 2)
    searcher.close()
