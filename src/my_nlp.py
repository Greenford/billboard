import nltk
nltk.download('opinion_lexicon')
from nltk.corpus import opinion_lexicon
from nltk.tokenize import treebank
from pymongo import MongoClient
class Sentimenter:
    def __init__(self):
        self.pos = set(opinion_lexicon.positive())
        self.neg = set(opinion_lexicon.negative())
        self.tok = treebank.TreebankWordTokenizer()


    def sentiment(self, text):
        pcount = ncount = 0
        words = [word.lower() for word in self.tok.tokenize(text)]
        for word in words:
            if word in self.pos:
                pcount += 1
            elif word in self.neg:
                ncount += 1
        return {'sentiment':(pcount-ncount)/(pcount+ncount+1), 
                'pos':pcount, 
                'neg':ncount, 
                'wordcount':len(words)}
    
def process_mongo_docs(verbose=1):
    collection = MongoClient().billboard.lyrics
    results = collection.find({'$and':[{'_id':{'$exists':'true'}}, 
                              {'lyrics':{'$exists':'true'}}]})
    s = Sentimenter()
    count = 0
    for track in results:
        if track.get('dict_sentiment')==None:
            scores = s.sentiment(track['lyrics'])
            collection.update({'_id':track['_id']}, {'$set':{'dict_sentiment':scores}})
            count += 1
            if verbose:
                print(count)

if __name__ == '__main__':
    process_mongo_docs()

