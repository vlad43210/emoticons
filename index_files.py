#!/usr/bin/env python
# coding=utf-8
import codecs, gzip, re, sys, os, lucene, threading, time
from datetime import datetime
from operator import itemgetter

"""
This class is loosely based on the Lucene (java implementation) demo class 
org.apache.lucene.demo.IndexFiles.  It will take a directory as an argument
and will index all of the files in that directory and downward recursively.
It will index on the file path, the file name and the file contents.  The
resulting Lucene index will be placed in the current directory and called
'index'.
"""

class Ticker(object):

    def __init__(self):
        self.tick = True

    def run(self):
        while self.tick:
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(1.0)

class IndexFiles(object):
    """Usage: python IndexFiles <doc_directory>"""

    def __init__(self, root, storeDir, analyzer):
        self.root = root
        if not os.path.exists(storeDir):
            os.mkdir(storeDir)
        store = lucene.SimpleFSDirectory(lucene.File(storeDir))
        self.writer = lucene.IndexWriter(store, analyzer, True)
        self.writer.setMaxFieldLength(1048576)
    
    def optimizeIndexer(self):
        ticker = Ticker()
        print 'commit index',
        threading.Thread(target=ticker.run).start()
        self.writer.optimize()
        self.writer.close()
        ticker.tick = False
        print 'done'
    
    def runIndexer(self):
        self.indexDocs()
        self.optimizeIndexer()

    def indexDocs(self):       
        for root, dirnames, filenames in os.walk(self.root):
            for filename in filenames:
                if not filename.endswith('.txt'):
                    continue
                print "adding", filename
                try:
                    path = os.path.join(root, filename)
                    docsfile = open(path)
                    contents = unicode(docsfile.read(), 'iso-8859-1')
                    docsfile.close()
                    doc = lucene.Document()
                    doc.add(lucene.Field("name", filename, self.t1))
                    doc.add(lucene.Field("path", path, self.t2))
                    if len(contents) > 0:
                        doc.add(lucene.Field("contents", contents, self.t2))
                    else:
                        print "warning: no content in %s" % filename
                    self.writer.addDocument(doc)
                except Exception, e:
                    print "Failed in indexDocs:", e                             

class IndexTweets(IndexFiles):
    """Usage: python IndexTweets <tweet_directory> | <tweet_file_path>"""

    def __init__(self, root, storeDir, analyzer, location_hash):
        super(IndexTweets, self).__init__(root, storeDir, analyzer)
        self.location_hash = location_hash
        self.linecutoff = 50000000
        self.RTre = re.compile("RT @\w+")
        self.tzre = re.compile("\+\w+")
        self.emoticonre = re.compile(u"http(s)?[:]//|[=<>]?[;:]+[\^]?[\\\/)(\]\[}{PpboO0]+[X#]?|[+=>\^Tㅜㅠㅡ][ㅁㅇ._-]*[+=<\^Tㅜㅠㅡ]")
        self.emoticonhash = {}
        self.emoticonhashfile = codecs.open("/Volumes/TerraFirma/SharedData/vdb5/emoticons_raw_files/emoticons_list.txt", encoding='utf-8', mode='w')

    def runIndexer(self):
        if self.root.endswith('tweets.txt.gz'): 
            self.indexOneDoc()
            self.optimizeIndexer()

    def indexOneDoc(self):
        try: 
            docsfile = gzip.open(self.root)
            lctr = 0
            for line in docsfile:
                lctr+=1
                if lctr%100000 == 0: print "on line: ", lctr, " at: ", time.time()
                if lctr > self.linecutoff: break
                tweet_id, user_id, date, tweet_id_replied, user_id_replied, source, some_flag, another_flag, location, text = unicode(line, 'utf-8').split('\t')
                if not user_id_replied:
                    user_id_replied = '0'
                
                if date:
                    tz = re.search(self.tzre, date).group(0)
                    #timestamp = str(int(time.mktime(time.strptime(date, "%a %b %d %H:%M:%S " + tz + " %Y"))))
                    timestamp = int(time.mktime(time.strptime(date, "%a %b %d %H:%M:%S " + tz + " %Y")))
                else:
                    timestamp = '0'
                    
                country = self.location_hash.get(user_id,"Unknown") 
                RT_search = re.search(self.RTre, text)
                if RT_search: RT_name = RT_search.group(0).split()[1].lstrip("@")
                else: RT_name = ''
                
                emoticon_iter = re.finditer(self.emoticonre, text)
                emoticon_str = ''
                while True:
                    try:
                        emoticon_char = emoticon_iter.next().group(0)
                        emoticon_str += emoticon_char + " "
                        self.emoticonhash[emoticon_char] = self.emoticonhash.get(emoticon_char,0)+1
                    except Exception, e:
                        break
                
                doc = lucene.Document() 
                doc.add(lucene.Field("tweet_id", tweet_id, lucene.Field.Store.YES, lucene.Field.Index.NO))
                doc.add(lucene.Field("user_id", user_id, lucene.Field.Store.YES, lucene.Field.Index.UN_TOKENIZED))
                doc.add(lucene.Field("user_id_replied", user_id_replied, lucene.Field.Store.YES, lucene.Field.Index.UN_TOKENIZED))
                doc.add(lucene.Field("source", source, lucene.Field.Store.YES, lucene.Field.Index.UN_TOKENIZED))
                doc.add(lucene.Field("country", country, lucene.Field.Store.YES, lucene.Field.Index.UN_TOKENIZED))
                doc.add(lucene.NumericField("timestamp",4,lucene.Field.Store.YES, True).setIntValue(timestamp))
                #doc.add(lucene.Field("timestamp", timestamp, lucene.Field.Store.YES, lucene.Field.Index.UN_TOKENIZED))
                if len(text) > 0: doc.add(lucene.Field("text", text, lucene.Field.Store.NO, lucene.Field.Index.TOKENIZED))
                if len(emoticon_str) > 0: 
                    #print "emoticon_str: ", emoticon_str
                    doc.add(lucene.Field("emoticons", emoticon_str, lucene.Field.Store.YES, lucene.Field.Index.TOKENIZED))
                    
                self.writer.addDocument(doc)
            for emoticon_char, count in sorted(self.emoticonhash.items(), key=itemgetter(1), reverse=True):
                self.emoticonhashfile.write(emoticon_char + u"," + unicode(count) + u"\n")
            self.emoticonhashfile.close()
        except Exception, e:
            print "failed to index file: ", docsfile, " with error: ", e
    

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print IndexFiles.__doc__
        sys.exit(1)
    lucene.initVM()
    print 'lucene', lucene.VERSION
    start = datetime.now()
    try:
        IndexFiles(sys.argv[1], "index", lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT))
        end = datetime.now()
        print end - start
    except Exception, e:
        print "Failed: ", e
        raise e
