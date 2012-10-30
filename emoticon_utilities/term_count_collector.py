from lucene import \
    IndexSearcher, PythonHitCollector
import time

class TermCountCollector(PythonHitCollector): 

    def __init__(self, searcher):
        super(TermCountCollector, self).__init__()
        self.base_doc = 0
        self.doc_count = 0
        self.terms = {}
        self.searcher = searcher
        self.unique_tv_list = {}
        
    def getDocCount(self):
        return self.doc_count

    def getTerms(self):
        return self.terms

    def acceptsDocsOutOfOrder(self):
        return True

    def collect(self, arg0, score):
        if self.doc_count%10000 == 0: print "doc number: ", self.doc_count, " at: ", time.time()
        #print "doc count: ", self.doc_count
        doc = self.searcher.doc(arg0);
        #print "%s: %s" %(doc, score)
        tv = self.searcher.getIndexReader().getTermFreqVector(self.base_doc + arg0, "text")
        #tv_hash = dict([(t.split("/")[0].strip(), t.split("/")[1]) for for t in tv.split(",")])
        tv_term_str = ""
        for tv_term in tv.getTerms():
            if tv_term not in [u'RT', u'rt'] and not tv_term.startswith("@"):
                tv_term_str = tv_term_str + tv_term + ","
        if tv_term_str.rstrip(",") in self.unique_tv_list:
            #print "eliminated duplicated string: ", tv_term_str
            pass
        else:
            self.unique_tv_list[tv_term_str.rstrip(",")] = 1
            try:
                for tv_term in tv.getTerms(): self.terms[tv_term] = self.terms.get(tv_term,0)+1
            except:
                pass
            #print "terms: ", self.terms
            self.doc_count+=1

    def setNextReader(self, arg0, arg1):
        self.base_doc = arg1

    def setScorer(self, arg0):
        pass
