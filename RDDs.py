# RDDS can be created from in-memory Python collections (parallelize) or external storage (like HDFS/local files).

# From a Python collection
nums = list(range(1, 11))
rdd = sc.parallelize(nums, numSlices=2)  # 2 partitions
print('Partitions:', rdd.getNumPartitions())
print('Data:', rdd.collect())

# From a text file (uncomment and set a real path)
text_rdd = sc.textFile('file:///path/to/data.txt', minPartitions=2)

# map: apply a function to each element
squares = rdd.map(lambda x: x * x)
print('Squares:', squares.take(5))

# flatMap: map + flatten (useful for tokenization)
sentences = sc.parallelize(['the quick brown fox', 'jumps over the lazy dog'])
tokens = sentences.flatMap(lambda s: s.split())
print('Tokens:', tokens.collect())

# filter: keep elements matching a predicate
evens = rdd.filter(lambda x: x % 2 == 0)
print('Evens:', evens.collect())

# distinct: remove duplicates
dupes = sc.parallelize([1,2,2,3,3,3,4])
print('Distinct:', dupes.distinct().collect())

# sample: random sample without replacement
print('Sample (~30%):', rdd.sample(False, 0.3, seed=42).collect())

# sortBy: distributed sort by a key function
import random
pairs = sc.parallelize([(c, random.randint(1, 100)) for c in list('spark')])
print('Unsorted:', pairs.collect())
print('Sorted by value:', pairs.sortBy(lambda kv: kv[1]).collect())

# Build a (key, value) RDD
words = sc.parallelize(['red', 'blue', 'red', 'green', 'blue', 'blue'])
pairs = words.map(lambda w: (w, 1))

# reduceByKey: associative, commutative reduce per key (shuffles + combines)
counts = pairs.reduceByKey(lambda a, b: a + b)
print('Counts via reduceByKey:', counts.collect())

# groupByKey: groups all values per key (can be heavy); prefer reduceByKey for sums
grouped = pairs.groupByKey()
print('Group sizes:', grouped.mapValues(lambda it: len(list(it))).collect())

# mapValues & flatMapValues: transform only the value side
lengths = counts.mapValues(lambda n: ('freq', n))
print('mapValues example:', lengths.collect())

# aggregateByKey: separate seq and comb functions with a zero value
zero = (0, 0)  # (sum, count)
seq = lambda acc, v: (acc[0] + v, acc[1] + 1)
comb = lambda a, b: (a[0] + b[0], a[1] + b[1])
stats = pairs.aggregateByKey(zero, seq, comb)
avg = stats.mapValues(lambda t: t[0] / float(t[1]))
print('aggregateByKey (avg of ones):', avg.collect())

# combineByKey: most general per-key aggregation
create = lambda v: (v, 1)
merge_val = lambda acc, v: (acc[0] + v, acc[1] + 1)
merge_comb = lambda a, b: (a[0] + b[0], a[1] + b[1])
scores = sc.parallelize([('alice', 90), ('bob', 80), ('alice', 95), ('bob', 70)])
avg_scores = scores.combineByKey(create, merge_val, merge_comb).mapValues(lambda t: t[0]/float(t[1]))
print('Average scores:', avg_scores.collect())

# Joins (inner, leftOuter, rightOuter, fullOuter)
a = sc.parallelize([('k1', 1), ('k2', 2)])
b = sc.parallelize([('k1', 'A'), ('k3', 'C')])
print('Inner join:', a.join(b).collect())
print('Left outer:', a.leftOuterJoin(b).collect())
print('Right outer:', a.rightOuterJoin(b).collect())
print('Full outer:', a.fullOuterJoin(b).collect())

print('count:', rdd.count())
print('first:', rdd.first())
print('take(3):', rdd.take(3))
print('top(3):', rdd.top(3))
print('takeOrdered(3):', rdd.takeOrdered(3))
print('sum via reduce:', rdd.reduce(lambda a,b: a+b))
print('sum via fold (zero=0):', rdd.fold(0, lambda a,b: a+b))

# aggregate: different return type than element type
zero = (0, 0)  # (sum, count)
seq = lambda acc, v: (acc[0] + v, acc[1] + 1)
comb = lambda a, b: (a[0] + b[0], a[1] + b[1])
total_sum, total_count = rdd.aggregate(zero, seq, comb)
print('avg via aggregate:', total_sum / float(total_count))

print('countByValue:', sc.parallelize([1,2,2,3,3,3]).countByValue())

# Save (creates an output directory with part files)
rdd.saveAsTextFile('output/rdd_numbers')
