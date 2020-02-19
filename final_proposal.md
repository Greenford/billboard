### Purpose:
Predict whether a recent track will be on the Billboard 100 using Spotify and Genius data, and use the model to make inferences about the nature of Billboard 100 songs.

### Previous attempts
[My attempt using the Million Song Dataset](https://github.com/Greenford/predicting.billboard.100)
Suffered from a class imbalance that was exaggerated for tracks that were less recent, but demonstrated usefulness of having a lyrical sentiment score for each song. However, the relevance of the project was limited due to using the MSD, whose tracks are effectively limited to 2010 and prior. Patching the project with current Spotify data would introduce inconsistencies, so I'll be using 100% Spotify audio feature data for this project. 

[A 2019 attempt with 88% accuracy and balanced classes.](https://arxiv.org/abs/1908.08609)
These USF students used 4 models (RF, general NN, Logistic Reg, and SVM) and went to lengths to balance the classes perfectly and got the best result from RF. They didn't use any gradient boosting models, and I expect XGB will be able to outperform their RF. They did use a few extra features I plan to capture; see below!

### This project's added value: 
Will demonstrate importance of lyrical sentiment scores when predicting the billboard. Will explore importance of these commonly excluded features: 
number on album, i.e. 1st, 7th (rumored to be the best place for a hit song)
label
album type (single, album, or compilation)
genre

### Impact
One hit track can bring in tons of profit for a record label and music artists, so it's important to know what makes a hit track. Conversely, there's a sizeable market for indie music/music that offers a novel experience apart from the mainstream. 

### Presentation
3 parts:
A well-made markdown file, a taped presentation, and a Flask App where a user can lookup a song from Spotify and see it's likelihood of making the Billboard 100. 

### Data 
The minimum scope of this project is tracks released 2000-2019, as time to collect data is a concern. The sources will be the Spotify API, Genius API, and Billboard, and I'll be going for a perfect class balance like the USF authors. The data requirement before new features will be about 1 GB, and most of that will be the lyrics. It'll all be freshly scraped with the Billboard as a starting point. 

Choosing Nillboard (non-Billboard) tracks is topic worth discussing, as it wouldn't bee too difficult to inflate a prediction score by scraping bottom-of-the-barrel Nillboard tracks. I anticipate an opposite problem would occur if tracks from the same albums as Billboard tracks were used for the Nillboard class - predictions would be less accurate because the track audio features would be more similar just by virtue of being on the same album, as well as popularity. 

### Potential Problems
Deciding how to choose the Nillboard songs will be an early problem that will need to be solved soon. 

Other problems will be the availability of extra features - many of the extra features are only applied to albums and the completeness of the data is unknown. Additionally, getting dummies for genre might be cumbersome because I don't know how many genres there will be. 

### Next steps
1. Scrape the Billboard!
2. Determine how to get Nillboard and start the Spotify and Genius scrapers.
