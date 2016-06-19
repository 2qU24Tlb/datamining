# Scalable Vertical Mining

Stages:
1. Generate Global Frequent Singletons:
2. Generate Local 2-itemsets from global frequent list: 
3. Put 3-itemsets input one worker with same prefix: 

Key Points:
1. switch from eclat to declat.
2. determine when to mining locally.
3. dynamic partitions decisions.

Improvements:
1. decide partition number at the beginning.
partitions = transactions / number of cores.

2. combine local frequent items instead of using KeyBy.

3. use int instead of string.
