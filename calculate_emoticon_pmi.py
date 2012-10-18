import codecs, math, re

from emoticon_utilities.string_utils import normalizeEmoticonName
from emoticon_utilities.term_count_collector import TermCountCollector
from emoticon_utilities.pmi_result import PMIResult

from lucene import \
    VERSION, initVM, CLASSPATH, FSDirectory, Filter, IndexSearcher, \
    QueryParser, WhitespaceAnalyzer

class PMICalculator(object):

    def __init__(self, emoticon, searcher, analyzer):
        super(PMICalculator, self).__init__()
    
        self.field = "emoticons"
        self.emoticon = emoticon
        self.searcher = searcher
        self.analyzer = analyzer
        self.escaped_emoticon = QueryParser.escape(self.emoticon)
        self.query = QueryParser("text",self.analyzer).parse(self.escaped_emoticon)
        self.raw_stats_dir = "/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/"
        self.pmi_file_name = self.raw_stats_dir + normalizeEmoticonName(self.emoticon).rstrip('_')+".pmidata"
        self.term_count_collector = TermCountCollector(searcher)
        f = Filter()
        self.searcher.search(self.query, f, self.term_count_collector)
        self.terms = self.term_count_collector.getTerms()
        self.query_result_count = self.term_count_collector.getDocCount()
        self.n = searcher.getIndexReader().numDocs()

        print "computing PMI for query: ", self.emoticon
        
        self.p_query_result = query_result_count*1.0/self.n

    def getTermPMI(self, min_cooccurrence):

        self.emoticon = self.emoticon.replace("\"", "")
        self.pmi_file = codecs.open(self.pmi_file_name, encoding='utf-8', mode='w')
        term_re = "([a-z]+)|([#]\\w+)"
        cnt = 0
        result_set = set()

        for co_occurring_term in terms.uniqueSet():
            cnt+=1
            if (terms.getCount(co_occurring_term) >= min_cooccurrence) and re.match(term_re, co_occurring_term):
                term_result = getPMI(co_occurring_term)
                if term_result.getCooccurrenceCount() >= min_cooccurrence:
                    result_set.add(term_result)
            if cnt%1000 == 0: print "processed term number: ", cnt

        for tr in result_set: self.pmi_file.write(tr.getTerm() + "," + tr.getPmi() + "," + tr.getCooccurrenceCount() + "\n")
        self.pmi_file.close()

    def getPMI(self, co_term):
        pmi = -1.0
        cooccurrence_count = 0
        term_count = 0
        try:
            cooccurrence_count = self.terms.getCount(co_term)*1.0
            term_count = self.getTermCount(co_term)*1.0
            if cooccurrence_count > 0:
                p_cooccurrence = cooccurrence_count / self.n
                p_term = term_count / self.n + .00000001
            pmi = math.log(2, p_cooccurrence / (self.p_query_result * p_term))
            print "term: ", term, " term count: ", term_count, " cooccurrence_count: ", cooccurrence_count, " P(seed-term,term): ", p_cooccurrence, " P(seedterm): ", p_term, " PMI: ", pmi
        except Exception, e:
            print "failed to calculate PMI: ", e
        return PMIResult(co_term, pmi, cooccurrence_count)

    def getTermCount(self, co_term):
        t_count = 0
        try:
            t_count = self.searcher.getIndexReader().docFreq(Term(self.field, co_term))
        except Exception, e:
            print "failed to get term count: ", e
        return t_count
 
if __name__ == '__main__':
    STORE_DIR =  "/Volumes/TerraFirma/SharedData/vdb5/lucene_index"
    initVM(CLASSPATH)
    print 'lucene', VERSION
    directory = FSDirectory.getDirectory(STORE_DIR, False)
    searcher = IndexSearcher(directory)
    analyzer = WhitespaceAnalyzer()
    min_doc_frequency = 3
    emoticonPmiCalculator = PMICalculator(":)", searcher, analyzer)
    emoticonPmiCalculator.getTermPMI(min_doc_frequency)
    print "calculated PMI for :) at: ", time.time()
    searcher.close()