# Lecture Note

- Compression techniques
- windows techniques
⋅⋅* ex: 15 mins (then consider the next window in the period of 15 mins)
- challenge: finding the right bandwidth of the window length. 

Text Mining:
- delete the stops words to extract information from the data

Solution to too much information
- Thread: parallel computing
- Volatile solution: delete after use

DSMS solution 
- DB(relation, multimedia, graphs) <- keep the memory largest as possible
- simplification: not storing all the tweets but only partially
- filtering: not storing all the tweets
- data replacement
⋅⋅* data deleted after a period of time (delete the past)
⋅⋅* only store the latest period of data

Time Frame Segmentation: (related to window techniques)


## Graph model for social network

- Nodes: individual actors within the network
⋅⋅* Nodes with label: user names

- Ties: relationships betweens the actors
⋅⋅* Edges: not labelled
⋅⋅* directional: (path)  predecessors $(father)$, successor
- links may have attributes, directed or undirected
- Network Type
⋅⋅* Homogeneous network: only one type of relationship (like friendship) 
⋅⋅* Geterogeneous network: (follower relationship "bold line" + similarity relationship "dot line" )

### SNA application
finding the inherent regularities in data (frequent pattern, repeated behaviral actions)


Mining Subgraphs
- Topological ordering: spread analysis of tweeter <- why does the news become popular?
- Strong Connected Componenets

Graph Analysis
- Page Rank
- HITS

Metrics:
- Degree Centrality
- Betweeness Centrality (alwasy have to pass through it, because the node is crucial for the information network)
- Closeness Centrality


Example:
- Identify influencial person in a subgroup (force of changing)

#### Structural Analysis Benefits (incomplete)
Short distance, transmit information accurately
highly expert 










Hits Algorithms- find subset of authority
Provider (authority)




