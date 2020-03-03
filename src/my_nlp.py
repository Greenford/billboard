import nltk
from nltk.corpus import opinion_lexicon
from nltk.tokenize import treebank
from pymongo import MongoClient
nltk.download('opinion_lexicon')


class Sentimenter:
    """
    Creates object to handle dictionary-lookup sentiment scoring

    Args: None
    Returns: None. 
    """


    def __init__(self):
        self.pos = set(opinion_lexicon.positive())
        self.neg = set(opinion_lexicon.negative())
        self.tok = treebank.TreebankWordTokenizer()


    def sentiment(self, text):
        """
        Returns the dictionary sentiment of a text.

        Args:
            text (str): text to get a sentiment score from
        
        Returns (dict): (sentiment score, positive wordcount, 
            negative wordcount, and total wordcount)
        """
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
    """
    Processes each of the unprocessed documents in billboard.lyrics and adds 
    a sentiment field to each of them with the calculates values. 

    Args: 
        verbose (int): more prints from this function with higher verbosity numbers. 

    Returns: None. Sets sentiment fields in lyrics data.
    """

    #initialize db collection
    collection = MongoClient().billboard.lyrics

    #gets documents from lyrics collection
    results = collection.find(
        {'$and':[
            {'_id':{'$exists':'true'}}, 
            {'lyrics':{'$exists':'true'}}
        ]}
    )

    s = Sentimenter()
    count = 0
    for track in results:

        #do nothing if a sentiment field already exists
        if track.get('dict_sentiment')==None:
            scores = s.sentiment(track['lyrics'])
            collection.update({'_id':track['_id']}, {'$set':{'dict_sentiment':scores}})
            count += 1
            if verbose:
                print(count)


if __name__ == '__main__':
    process_mongo_docs()

