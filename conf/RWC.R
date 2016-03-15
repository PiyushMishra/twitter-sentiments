library("tm")
library("wordcloud")
library("slam")
library("topicmodels")
library("rjson")
#Load Text
tweets <- Search(index = "twitter", type = "tweet", field="text", q=paste("term:", term), size=9999999)$hits$hits

#Clean Text
tweets = gsub("(RT|via)((?:\\b\\W*@\\w+)+)","",tweets)
tweets = gsub("http[^[:blank:]]+", "", tweets)
tweets = gsub("@\\w+", "", tweets)
tweets = gsub("[ \t]{2,}", "", tweets)
tweets = gsub(".*= list","",tweets)
tweets = gsub("^\\s+|\\s+$", "", tweets)
tweets = gsub(term,"",tweets, ignore.case = TRUE)
tweets <- gsub('\\d+', '', tweets)


tweets = gsub("[[:punct:]]", " ", tweets)
corpus = Corpus(VectorSource(tweets))
corpus = tm_map(corpus,removePunctuation)
corpus = tm_map(corpus,stripWhitespace)
corpus = tm_map(corpus,content_transformer(tolower))
corpus = tm_map(corpus,removeWords,stopwords("english"))


CorpusObj.tdm <- TermDocumentMatrix(corpus, control = list(minWordLength = 3))
inspect(CorpusObj.tdm[1:10,1:10])
findFreqTerms(CorpusObj.tdm, lowfreq=1003)
dim(CorpusObj.tdm)
CorpusObj.tdm.sp <- removeSparseTerms(CorpusObj.tdm, sparse=0.88)
dim(CorpusObj.tdm.sp)
## Show Remining words per 15 Document.
inspect(CorpusObj.tdm.sp[1:10,1:15])


## visualizing  the TD --  

## Words Cloud Visualizing
library(wordcloud)
library(RColorBrewer)


mTDM <- as.matrix(CorpusObj.tdm)
v <- sort(rowSums(mTDM),decreasing=TRUE)
d <- data.frame(word = names(v),freq=v)
toJSON(head(v, 50), "C")