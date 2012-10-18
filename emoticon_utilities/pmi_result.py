"""Created on Dec 28, 2011 by finkcr1"""

class PMIResult(object):
    
    def __init__(self, term, pmi, cooccurrence_count):
        super(PMIResult, self).__init__()
        self.term = term
        self.pmi = pmi
        self.cooccurrence_count = cooccurrence_count

    def getTerm(self):
        return self.term

    def getPMI(self):
        return self.pmi

    def getCooccurrenceCount(self):
        return self.cooccurrence_count
