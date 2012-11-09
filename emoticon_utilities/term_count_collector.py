from lucene import \
    IndexSearcher, PythonHitCollector
import time

class TermCountCollector(PythonHitCollector): 

    def __init__(self, searcher, emoticon):
        super(TermCountCollector, self).__init__()
        self.base_doc = 0
        self.doc_count = 0
        self.terms = {}
        self.searcher = searcher
        self.unique_tv_list = {}
        self.popular_terms_hash = {"hurt":[], "podcast":[], "general":[], "catalog":[], "medicine":[]}
        self.emoticon = emoticon
        
    def getDocCount(self):
        return self.doc_count

    def getTerms(self):
        return self.terms

    def cleanTerm(self, term):
        if term.endswith((".",",","?","!")):
            term = term[:-1]
        #elif term.endswith((":)",":(",":/",";o")):
        #    term = term[:-2]
        return term

    def acceptsDocsOutOfOrder(self):
        return True

    def collect(self, arg0, score):
        #if self.doc_count%10000 == 0: print "doc number: ", self.doc_count, " at: ", time.time()
        #print "doc count: ", self.doc_count
        doc = self.searcher.doc(arg0);
        #print "%s: %s" %(doc, score)
        tv = self.searcher.getIndexReader().getTermFreqVector(self.base_doc + arg0, "text")
        tv_term_str = ""
        is_rt = False
        for tv_term in tv.getTerms():
            clean_term = self.cleanTerm(tv_term)
            if clean_term and clean_term not in [u'RT', u'rt', u'via'] and not clean_term.startswith("@") \
               and not clean_term.startswith("http://") and self.emoticon not in clean_term and "&" not in clean_term:
                tv_term_str = tv_term_str + clean_term + ","
            if clean_term in [u'RT', u'rt', u'via']:
                is_rt = True
        if tv_term_str[:-1] in self.unique_tv_list and is_rt:
            print "eliminated duplicated string: ", tv_term_str
        else:
            #for p_term in self.popular_terms_hash:
            #    if p_term in tv_term_str:
            #        self.popular_terms_hash[p_term].append(tv)
            self.unique_tv_list[tv_term_str[:-1]] = 1
            try:
                for tv_term in tv.getTerms(): 
                    clean_tv_term = self.cleanTerm(tv_term)
                    if clean_tv_term and clean_tv_term not in [u'RT', u'rt', u'via'] and not clean_tv_term.startswith("@") \
                       and not clean_tv_term.startswith("http://") and self.emoticon not in clean_term and "&" not in clean_term:
                        self.terms[clean_tv_term] = self.terms.get(clean_tv_term,0)+1
            except Exception, e:
                print "failed to add terms: ", e
                pass
            #print "terms: ", self.terms
            self.doc_count+=1

    def setNextReader(self, arg0, arg1):
        self.base_doc = arg1

    def setScorer(self, arg0):
        pass
