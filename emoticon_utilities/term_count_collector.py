from lucene import \
    IndexSearcher, PythonHitCollector


class TermCountCollector(PythonHitCollector): 

    def __init__(self, searcher):
        super(TermCountCollector, self).__init__()
        self.base_doc = 0
        self.doc_count = 0
        self.terms = []
        self.searcher = searcher
        
    def getDocCount(self):
        return self.doc_count

    def getTerms(self):
        return self.terms

    def acceptsDocsOutOfOrder(self):
        return True

    def collect(self, arg0, score):
        #print "doc count: ", self.doc_count
        doc = self.searcher.doc(arg0);
        #print "%s: %s" %(doc, score)
        tv = self.searcher.getIndexReader().getTermFreqVectors(self.base_doc + arg0)
        print "tv: ", tv
        #try:
        #    for tv_term in tv.getTerms: self.terms.append(tv_term)
        #except:
        #    pass
        #print "terms: ", self.terms
        self.doc_count+=1

    def setNextReader(self, arg0, arg1):
        self.base_doc = arg1

    def setScorer(self, arg0):
        pass
