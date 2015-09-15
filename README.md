# Information_Retrieval
CpSc8810 Information Retrieval project

This is the source code for an advanced computer science course named "Information retrieval". Please check file '881report.pdf' 
under directory '/Information_Retrieval/Okapi_BM25/' for detailed design ideas, algorithms, system design, software implementation
and compared results.

All the system development are based on Clemson Hadoop system named "Palmetto Cluster". And the development language is Java.

Basically, this course project is devided into three stages. The first stage is to implement a boolean retrieval system using Mapreduce
and indexing. The second stage is to design a text based information retrival system using TF-IDF algorithm. These two stages are 
individual tasks. The third stage is to build a web based application which has text based search engine in the front end, and distributed
index files among 10-20 data nodes using Okapi_BM25 algorithm in the back end. This stage is a group task and I'm responsible for Okapi_BM25 algorithm 
implementation.  

For the first stage, I implement three boolean retrieval strategies which are Uniword, Biword and Position.Source code for Uniword
are located in Information_Retrieval/Boolean/Boolean/, it generates index files for one key word search. Source code for Biword 
are located in Information_Retrieval/Boolean/Biword/, it generates index files which can support key words up to two, and my algorithm for 
this Biword search can also support key words with logic words like 'and','or','not'. Position search is located in Information_Retrieval/Boolean/Position/. 
When you search key words by position index, it not only tells you which files contain these words, but also tells you the exact position
these words locates in.

All the indexing files are generated using articles from 'New York Times' newspapers。
