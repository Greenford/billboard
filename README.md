# Predicting the Billboard 100
## with Machine Learning, Spotify and Genius data
Popular music makes money, plain and simple. Knowing the nature of popular music is important if you're in the business of scouting for musical talent or producing albums... and also if you're composing indie music, lest your tunes sound derivative.

## The Goal
Make a predictive ML model to predict whether a track will be on the Billboard 100, beat the score (88% accuracy) of a 2019 University-San Francisco Paper*, and make inferences about the nature of popular music. 

A note on definitions: 
1. A 'track' is the recording of a 'song', and a 'song' is composed only of the lyrics and sheet music.
2. I use 'Nillboard' to describe songs that have not been on the Billboard Hot 100.

## The Data
I gathered Billboard data for about 7,000 tracks that were on the Hot 100 within the years 2000-2019 inclusive. From there, I gathered Spotify and Genius data for those 7,000 tracks plus all other tracks that were on the same albums (another ~43,000, bringing the total to ~50,000 tracks). 
[image]()
[Click here]() for more initial data visualizations. 

I used Python libraries to inderface with the every single API; find them in the references.

It's important to note how Nillboard tracks are chosen in any project like this that hopes to be compared to previous work: For example, Spotify provides functionality to get only the bottom 10% of songs in terms of popularity by using tag:hipster in a search. My assumption is that a hipster Nillboard would inflate the accuracy of a predictive model. 

## Predictive Modeling
To start, I corrected the target class imbalance by undersampling the Nillboard tracks to match the Billboard tracks. Same as in the USF paper, though it wouldn't be appropriate in general. But then - what would the appropriate class ratio be? 

### Baseline
A random fair coin flip would produce 50% accuracy. 
Next was a Random Forest Classifier because it tends to produce decent (but not the best) results with no tuning and no data transformations. Its accuracy was 85%. Do note: RFC was the best model in the USF paper. 

### Feature Engineering
1. Let's be explicit: the target was both class balanced and year balanced. 
[image of class balancing before and after]()

2. Lyrical Sentiment Scores: In a previous project*, I found lyrical sentiment to be a useful feature. I used the Natural Language Toolkit's Opinion Lexicon to calculate a score for each track with the formula: score = (pos-neg)/(pos+neg+1). The +1 is to normalize. The opinion lexicon is simply two different sets of words: one of positive-signalling words and one of negative. For example, the below except from the late Amy Winehouse's "Love is a Losing Game" gets a score of -1/8. 
[Love is a losing game]()

3. Record Label Name: Each track was part of an album, and each album was produced by at least one record label. After mapping the label names to the tracks, I then changed label to be a measure of that label's success - the number of hits it had in the Billboard 100. If there were 2+ labels matched to a hit, each label got credit. Here's a distribution of that feature with each label counted only once. 
[]()
The label with 800+ hits? Columbia Records, who recorded for AC/DC, Louis Armstrong, Beyoncé, Mariah Carey, Johnny Cash, Bob Dylan, 50 Cent, Pink Floyd, Frank Sinatra... and the list goes on. 

Note: fuzzy matching the record label names might help improve accuracy of this feature: some labels seemed to have variations of the same name. 

### Final Model
Next was the XGBoost Classifier. However, even after a decent amount of hyperparameter tuning, the maximum accuracy of my XGB-C was 88%. Tied with USF! Lil ol' me in a week vs two USF students (and they're not super-clear about how they chose their Nillboard)

## Inferences
Here are the feature importances (by method of permutation importance) from the XGB-C. 
[]()
The top features are measures of popularity, which... well, stick around for the conclusion. It's also worth noting that I eliminated the features album_type, time_signature, mode, key, explicit, and instrumentalness in order; accuracy improved with each of the first 4 eliminations, and then only decreased in minute amounts from the last 2 eliminations. 

So what do the features mean? Unfortunately, most of them are not linearly interpretable. I definitely tried by using a logistic regressor and interpreting the beta coefficients, but they simply didn't make sense. Its worth noting that the two most important features have a positive correlation with odds of being on the Billboard. 

## Conclusion
If it seems like cheating to use Spotify's track popularity and album popularity, I'd agree. Unfortunately, accuracy takes a huge hit when those are removed: XX% Working by that rationale, it might be necessary to drop label success too. On the other hand, this simply might be indicative of the true nature of hit music - maybe it largely depends on marketing budgets, presence of a music video, social media engagement, etc.

### Future Work
1. Rigorous Natural Language Processing of the lyrics as an additional predictive feature set. At the minimum, spaCy could be used to detect negation for the sentimental words. 

2. Incorporating genres. Spotify has a field for them that is fetched with the album information, but all I got were empty lists. It might be possible with a deeper understanding of the Spotify API.  

## References: 
1. [Song Hit Prediction: Predicting Billboard Hits Using Spotify Data by Kai Middlebrook & Kian Sheik](https://arxiv.org/abs/1908.08609)
2. [billboard.py by Allen Guo](https://github.com/guoguo12/billboard-charts)
3. [Spotipy by Paul Lamere](https://github.com/plamere/spotipy)
4. [LyricsGenius by John W. Miller](https://github.com/johnwmillr/LyricsGenius)
5. [My previous project using the Million Song Dataset](https://github.com/Greenford/predicting.billboard.100)
