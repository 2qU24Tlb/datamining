* based on the paper "Parallel Eclat for Opportunistic Mining of Frequent Itemsets"(http://link.springer.com/chapter/10.1007%2F978-3-319-22849-5_27)

* improvements:
1. Original algorithm needs to calculate both tidset and diffset for each itemsets before finishing mrlargeK job. This is not necessary since we can choose one format at the beginning and later turn into another one by looking at the turning point.
2. The author didn't talk about this determining K too much. Actually it's will effect the efficiency a lot. This variable will rely on the characteristics of the datasets. 
3. The way to calculate minSup is not clearly elaborate. It is quite different for calculating minSup for tidset and diffset.

sample DB:

1 a   c d   f g   i       m     p
2 a b c     f           l m   o
3   b       f   h   j         o
4   b c               k         p   s
5 a   c   e f           l m n   p

minSup = 3

|a| = 3
|b| = 3
|c| = 4
|f| = 4
|m| = 3
|p| = 3

|ac| = 3
|af| = 3
|am| = 3
|ap| = 3
|cf| = 3
|cm| = 3
|cp| = 3
|fm| = 3
|fp| = 3

|acf| = 3
|acm| = 3
|afm| = 3
|cfm| = 3 
