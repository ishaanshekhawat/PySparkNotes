text = sc.parallelize([
    'to be or not to be',
    'that is the question',
    'whether tis nobler in the mind to suffer'
])
wc = (text
      .flatMap(lambda line: line.split())
      .map(lambda w: (w.lower(), 1))
      .reduceByKey(lambda a,b: a+b)
      .sortBy(lambda kv: (-kv[1], kv[0]))
)
print('Top 10 words:', wc.take(10))
