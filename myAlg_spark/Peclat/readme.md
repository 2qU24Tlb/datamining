* based on the paper "Parallel Eclat for Opportunistic Mining of Frequent Itemsets"(http://link.springer.com/chapter/10.1007%2F978-3-319-22849-5_27)

* improvements:
1. Original algorithm needs to calculate both tidset and diffset for each itemsets before finishing mrlargeK job. This is not necessary since we can choose one format at the beginning and later turn into another one by looking at the turning point.
2. The author didn't talk about this determining K too much. Actually it's will effect the efficiency a lot. This variable will rely on the characteristics of the datasets. 
3. The way to calculate minSup is not clearly elaborate. It is quite different for calculating minSup for tidset and diffset.
