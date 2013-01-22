import codecs, gzip, math, re, time

from emoticon_utilities.string_utils import normalizeEmoticonName
from emoticon_utilities.term_count_collector import TermCountCollector
from emoticon_utilities.pmi_result import PMIResult

from lucene import \
    VERSION, initVM, CLASSPATH, FSDirectory, IndexReader, IndexSearcher, \
    MatchAllDocsQuery, PythonHitCollector, QueryFilter, QueryParser, Term, \
    WhitespaceAnalyzer

def cleanRawTerm(self, term):
    if term.endswith((".",",","?","!",":",")")):
        term = term[:-1]
    #elif term.endswith((":)",":(",":/",";o")):
    #    term = term[:-2]
    return term

def getBaselineStatistics():
    docsfile = gzip.open("/Volumes/Luna/twitter_germans/tweets.txt.gz")
    lctr = 0
    linecutoff = 50000000
    all_tweets_set = {}
    for line in docsfile:
        lctr+=1
        if lctr%10 == 0: print "on line: ", lctr, " at: ", time.time()
        if lctr > linecutoff: break
        tweet_id, user_id, date, tweet_id_replied, user_id_replied, source, some_flag, another_flag, location, text = unicode(line, 'utf-8').split('\t')
        is_rt = False
        tv_term_str = ""
        for tv_term in text.split():
            clean_term = cleanRawTerm(tv_term)
            if clean_term and clean_term not in [u'RT', u'rt', u'via'] and not clean_term.startswith("@") \
               and not clean_term.startswith("http://"):
                tv_term_str = tv_term_str + clean_term + ","
            if clean_term in [u'RT', u'rt', u'via']:
                is_rt = True
            ordered_term_str = sorted(tv_term_str.split(",")).join(",")
            all_tweets_set[ordered_term_str] = all_tweets_set.get(ordered_term_str,0)+1
    baseline_stats_text_file = open("/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/emoticon_pmi_stats.txt","w")
    baseline_stats_text_file.write("n:%s\n" % (len(all_tweets_set)))
    baseline_stats_text_file.close()

class PMICalculator(object):

    def __init__(self, emoticon, searcher, analyzer, english_only=False):
        super(PMICalculator, self).__init__()
    
        self.field = "emoticons"
        self.emoticon = emoticon
        self.searcher = searcher
        self.analyzer = analyzer
        self.escaped_emoticon = QueryParser.escape(self.emoticon)
        self.query = QueryParser("emoticons",self.analyzer).parse(self.escaped_emoticon)
        self.raw_stats_dir = "/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/"
        if english_only:
            country = "United States"
            country_prefix = "US"
        else:
            country = None
            country_prefix = ""
        self.pmi_file_name = self.raw_stats_dir + normalizeEmoticonName(self.emoticon).rstrip('_')+("_%s" %(country_prefix))*english_only+".pmidata"
        self.sample_tweets_name = self.raw_stats_dir + normalizeEmoticonName(self.emoticon).rstrip('_')+("_%s" %(country_prefix))*english_only+".samptweets"
        self.sample_tweets_file = codecs.open(self.sample_tweets_name, encoding='utf-8', mode='w')
        self.term_count_collector = TermCountCollector(searcher, emoticon, country)
        print "starting query at: ", time.time()
        hits = self.searcher.search(self.query, self.term_count_collector)
        #print "terms: ", self.terms
        if emoticon == ":P":
            ee_two = QueryParser.escape(":p")
        elif emoticon == "T_T":
            ee_two = QueryParser.escape("TT")
        if emoticon in [":P","T_T"]:
            q_two = QueryParser("emoticons",self.analyzer).parse(ee_two)
            hits_two = self.searcher.search(q_two, self.term_count_collector)
        self.terms = self.term_count_collector.getTerms()            
        self.query_result_count = self.term_count_collector.getDocCount()
        for p_term, p_term_tweets in self.term_count_collector.popular_terms_hash.items():
            for p_term_tweet in p_term_tweets:
                self.sample_tweets_file.write("term: " + p_term + " tweet: " + p_term_tweet + "\n")
        self.sample_tweets_file.close()
        self.n = searcher.getIndexReader().numDocs()

        print "computing PMI for query: ", self.emoticon, " at: ", time.time()
        
        self.p_query_result = self.query_result_count*1.0/self.n

    def getTermPMI(self, min_cooccurrence):

        self.emoticon = self.emoticon.replace("\"", "")
        self.pmi_file = codecs.open(self.pmi_file_name, encoding='utf-8', mode='w')
        term_re = "([a-z]+)|([#]\\w+)"
        cnt = 0
        result_set = set()

        for co_occurring_term in self.terms:
            cnt+=1
            if (self.terms[co_occurring_term] >= min_cooccurrence) and re.match(term_re, co_occurring_term):
                term_result = self.getPMI(co_occurring_term)
                result_set.add(term_result)
            if cnt%10000 == 0: print "processed term number: ", cnt, " out of: ", len(self.terms), " at: ", time.time()

        print "number of results: ", len(result_set)
        sorted_result_set = sorted(list(result_set), key=lambda x: x.getPMI(), reverse=True)
        for tr in sorted_result_set: self.pmi_file.write(tr.getTerm() + "," + str(tr.getPMI()) + "," + str(tr.getCooccurrenceCount()) + "\n")
        self.pmi_file.close()

    def getPMI(self, co_term):
        pmi = -1.0
        cooccurrence_count = 0
        term_count = 0
        try:
            cooccurrence_count = self.terms[co_term]*1.0
            term_count = self.getTermCount(co_term)*1.0
            if cooccurrence_count > 0:
                p_cooccurrence = cooccurrence_count / self.n
                p_term = term_count / self.n + .00000001
            pmi = math.log(p_cooccurrence / (self.p_query_result * p_term), 2)
            #print "term: ", co_term, " term count: ", term_count, " cooccurrence_count: ", cooccurrence_count, " P(seed-term,term): ", p_cooccurrence, " P(seedterm): ", p_term, " PMI: ", pmi
        except Exception, e:
            print "failed to calculate PMI: ", e
        return PMIResult(co_term, pmi, cooccurrence_count)

    def getTermCount(self, co_term):
        t_count = 0
        try:
            t_count = self.searcher.getIndexReader().docFreq(Term("text", co_term))
            #t_query = QueryParser("text",self.analyzer).parse(co_term)
            #t_term_count_collector = TermCountCollector(searcher)
            #t_hits = self.searcher.search(t_query, t_term_count_collector)
            #t_count = self.term_count_collector.getDocCount()
        except Exception, e:
            print "failed to get term count: ", e
        return t_count
 
if __name__ == '__main__':
    print "started PMI calculator at: ", time.time()
    getBaselineStatistics()
    STORE_DIR =  "/Volumes/TerraFirma/SharedData/vdb5/lucene_index"
    #STORE_DIR =  "/Volumes/TerraFirma/SharedData/vdb5/lucene_index_test"
    initVM(CLASSPATH, maxheap='1024m')
    print 'lucene', VERSION
    directory = FSDirectory.getDirectory(STORE_DIR, False)
    searcher = IndexSearcher(directory)
    analyzer = WhitespaceAnalyzer()
    min_doc_frequency = 50
    #emoticon_list = [":)", ":(", ";)", ":P", ":0", "^^", "TT",":p",":/","^_^","T_T"]
    #emoticon_list = [":)", ":(", ":'(", ":-|", "^^"]
    #emoticon_list = ["^^", "T_T"]
    emoticon_list = [":)","^..^","^00^",":(",";)",":P","^^","^_^","-_-","T_T",":o","@_@"]
    #pmi_emoticon = "^^"
    #if pmi_emoticon == "^^": min_doc_frequency = 100
    for pmi_emoticon in emoticon_list:
        #emoticonPmiCalculator = PMICalculator(pmi_emoticon, searcher, analyzer)
        emoticonPmiCalculator = PMICalculator(pmi_emoticon, searcher, analyzer, english_only=True)
        emoticonPmiCalculator.getTermPMI(min_doc_frequency)
        print "calculated PMI for ", pmi_emoticon, " at: ", time.time()
    searcher.close()