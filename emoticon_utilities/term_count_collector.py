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
        tv = self.searcher.getIndexReader().getTermFreqVector(self.base_doc + arg0, "emoticons")
        for tv_term in tv.getTerms: self.terms.append(tv_term)
        self.doc_count+=1

    def setNextReader(self, arg0, arg1):
        self.base_doc = arg1

    def setScorer(self, arg0):
        pass
