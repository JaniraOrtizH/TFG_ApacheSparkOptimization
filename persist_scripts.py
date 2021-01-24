from pyspark import SparkContext, SparkConf, StorageLevel
import sys
import string
import time

def mymapeo(x):
    for y in string.punctuation:
        x = x.replace(y,'').lower()
    line = x.split(" ")
    return line
    
def word_count_persist_mem_only(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])

      sc = SparkContext(conf=conf, pyFiles=['run_app.py', 'config_scriptsDf.py'])
      spark = SparkSession.builder.config(conf=conf).getOrCreate()

      data = sc.textFile(data_file).flatMap(lambda x: x.split(" ")).collect()
      paralData = sc.parallelize(data, 100)
      print(paralData.getNumPartitions())
      print(sc.getConf().get("spark.executor.instances"))
      
      df = paralData.map(lambda r: Row(r)).toDF(["word"]).persist(StorageLevel(True, False, False, False, int(internal_param[7])))
      df.show()
      
      cleanDf = df.filter(col('word') != '').withColumn('word', regexp_replace(col('word'),'[^\sa-zA-Z0-9]', ''))
      
      result = cleanDf.withColumn('count', lit(1)).groupBy('word').sum('count').withColumnRenamed('sum(count)','frequencies')
      result.show()

      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()
      
      

def word_count_sort_pesist_mem_only(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])

      sc = SparkContext(conf=conf, pyFiles=['run_app.py', 'config_scriptsDf.py'])
      spark = SparkSession.builder.config(conf=conf).getOrCreate()

      data = sc.textFile(data_file).flatMap(lambda x: x.split(" ")).collect()
      paralData = sc.parallelize(data, 200)

      df = paralData.map(lambda r: Row(r)).toDF(["word"])
      cleanDf = df.filter(col('word') != '').withColumn('word', regexp_replace(col('word'),'[^\sa-zA-Z0-9]', '')).persist(StorageLevel(True, False, False, False, int(internal_param[7])))
      freqDf = cleanDf.withColumn('count', lit(1)).groupBy('word').sum('count').withColumnRenamed('sum(count)','frequencies')
      
      topFreqsDf = freqDf.orderBy('frequencies').limit(5)      
      
      print('Number of words: ', cleanDf.count())
      print('Top 5 frequencies:')
      topFreqsDf.show()
      
      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_plus_pesist_mem_only(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo)
      
      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b).persist(StorageLevel(True, False, False, False, int(internal_param[7])))
      
      topFreqs = frequencies.sortBy(lambda x: x[1], ascending=False).persist(StorageLevel(True, False, False, False, int(internal_param[7])))
      print('Top 5 frequencies:', topFreqs.take(5))
      
      leastFreqs = frequencies.sortBy(lambda x: x[1], ascending=True)
      print('Leats 5 frequencies:', leastFreqs.take(5))

      topLenFreqs = topFreqs.sortBy(lambda x: len(x[0]), ascending=False).persist(StorageLevel(True, False, False, False, int(internal_param[7])))
      print('Top 5 length:', topLenFreqs.take(5))
      
      containsAwords = words.filter(lambda x: 'a' in x)
      print('Number of words containing a:', containsAwords.count())
      
      containsXwords = words.filter(lambda x: 'x' in x)
      print('Number of words containing x:', containsXwords.count())

      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()
      
      
      

def word_count_persist_mem_only_ser(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo).persist(StorageLevel(False, True, False, False, int(internal_param[7])))
      
      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
      frequencies.collect()
      print(frequencies.take(5))

      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_sort_pesist_mem_only_ser(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo).persist(StorageLevel(False, True, False, False, int(internal_param[7])))

      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b).persist(StorageLevel(False, True, False, False, int(internal_param[7])))
      numWords = data.count()
      sortFreq = frequencies.sortBy(lambda x: x[1], ascending=False)
      topFreqs = sortFreq.take(5)
      
      print('Number of words: ', numWords)
      print('Words frequencies:', sortFreq.collect())
      print('Top 5 frequencies:', topFreqs)
      
      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_plus_pesist_mem_only_ser(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo)
      
      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b).persist(StorageLevel(False, True, False, False, int(internal_param[7])))
      
      topFreqs = frequencies.sortBy(lambda x: x[1], ascending=False).persist(StorageLevel(False, True, False, False, int(internal_param[7])))
      print('Top 5 frequencies:', topFreqs.take(5))
      
      leastFreqs = frequencies.sortBy(lambda x: x[1], ascending=True)
      print('Leats 5 frequencies:', leastFreqs.take(5))

      topLenFreqs = topFreqs.sortBy(lambda x: len(x[0]), ascending=False).persist(StorageLevel(False, True, False, False, int(internal_param[7])))
      print('Top 5 length:', topLenFreqs.take(5))
      
      containsAwords = words.filter(lambda x: 'a' in x)
      print('Number of words containing a:', containsAwords.count())
      
      containsXwords = words.filter(lambda x: 'x' in x)
      print('Number of words containing x:', containsXwords.count())

      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()



def word_count_persist_mem_and_disk(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo).persist(StorageLevel(True, True, False, False, int(internal_param[7])))
      
      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
      frequencies.collect()
      print(frequencies.take(5))

      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_sort_pesist_mem_and_disk(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo).persist(StorageLevel(True, True, False, False, int(internal_param[7])))

      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b).persist(StorageLevel(True, True, False, False, int(internal_param[7])))
      numWords = data.count()
      sortFreq = frequencies.sortBy(lambda x: x[1], ascending=False)
      topFreqs = sortFreq.take(5)
      
      print('Number of words: ', numWords)
      print('Words frequencies:', sortFreq.collect())
      print('Top 5 frequencies:', topFreqs)
      
      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_plus_pesist_mem_and_disk(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo)
      
      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b).persist(StorageLevel(True, True, False, False, int(internal_param[7])))
      
      topFreqs = frequencies.sortBy(lambda x: x[1], ascending=False).persist(StorageLevel(True, True, False, False, int(internal_param[7])))
      print('Top 5 frequencies:', topFreqs.take(5))
      
      leastFreqs = frequencies.sortBy(lambda x: x[1], ascending=True)
      print('Leats 5 frequencies:', leastFreqs.take(5))

      topLenFreqs = topFreqs.sortBy(lambda x: len(x[0]), ascending=False).persist(StorageLevel(True, True, False, False, int(internal_param[7])))
      print('Top 5 length:', topLenFreqs.take(5))
      
      containsAwords = words.filter(lambda x: 'a' in x)
      print('Number of words containing a:', containsAwords.count())
      
      containsXwords = words.filter(lambda x: 'x' in x)
      print('Number of words containing x:', containsXwords.count())

      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()
      
      

def word_count_persist_mem_and_disk_ser(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo).persist(StorageLevel(True, True, False, False, int(internal_param[7])))
      
      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
      frequencies.collect()
      print(frequencies.take(5))

      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_sort_pesist_mem_and_disk_ser(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo).persist(StorageLevel(True, True, False, False, int(internal_param[7])))

      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b).persist(StorageLevel(True, True, False, False, int(internal_param[7])))
      numWords = data.count()
      sortFreq = frequencies.sortBy(lambda x: x[1], ascending=False)
      topFreqs = sortFreq.take(5)
      
      print('Number of words: ', numWords)
      print('Words frequencies:', sortFreq.collect())
      print('Top 5 frequencies:', topFreqs)
      
      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_plus_pesist_mem_and_disk_ser(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo)
      
      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b).persist(StorageLevel(True, True, False, False, int(internal_param[7])))
      
      topFreqs = frequencies.sortBy(lambda x: x[1], ascending=False).persist(StorageLevel(True, True, False, False, int(internal_param[7])))
      print('Top 5 frequencies:', topFreqs.take(5))
      
      leastFreqs = frequencies.sortBy(lambda x: x[1], ascending=True)
      print('Leats 5 frequencies:', leastFreqs.take(5))

      topLenFreqs = topFreqs.sortBy(lambda x: len(x[0]), ascending=False).persist(StorageLevel(True, True, False, False, int(internal_param[7])))
      print('Top 5 length:', topLenFreqs.take(5))
      
      containsAwords = words.filter(lambda x: 'a' in x)
      print('Number of words containing a:', containsAwords.count())
      
      containsXwords = words.filter(lambda x: 'x' in x)
      print('Number of words containing x:', containsXwords.count())

      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()
      


def word_count_persist_disk_only(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo).persist(StorageLevel(False, True, False, False, int(internal_param[7])))
      
      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
      frequencies.collect()
      print(frequencies.take(5))

      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_sort_pesist_disk_only(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])

      sc = SparkContext(conf=conf, pyFiles=['run_app.py', 'config_scriptsDf.py'])
      spark = SparkSession.builder.config(conf=conf).getOrCreate()

      data = sc.textFile(data_file).flatMap(lambda x: x.split(" ")).collect()
      paralData = sc.parallelize(data, 100)
      print(paralData.getNumPartitions())
      print(sc.getConf().get("spark.executor.instances"))
      
      df = paralData.map(lambda r: Row(r)).toDF(["word"])
      df.show()
      
      cleanDf = df.filter(col('word') != '').withColumn('word', regexp_replace(col('word'),'[^\sa-zA-Z0-9]', ''))
      
      result = cleanDf.withColumn('count', lit(1)).groupBy('word').sum('count').withColumnRenamed('sum(count)','frequencies')
      result.show()
      
      numWords = data.count()
      sortFreq = frequencies.sortBy(lambda x: x[1], ascending=False)
      topFreqs = sortFreq.take(5)
      
      print('Number of words: ', numWords)
      print('Words frequencies:', sortFreq.collect())
      print('Top 5 frequencies:', topFreqs)
      
      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_plus_pesist_disk_only(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo)
      
      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b).persist(StorageLevel(False, True, False, False, int(internal_param[7])))
      
      topFreqs = frequencies.sortBy(lambda x: x[1], ascending=False).persist(StorageLevel(False, True, False, False, int(internal_param[7])))
      print('Top 5 frequencies:', topFreqs.take(5))
      
      leastFreqs = frequencies.sortBy(lambda x: x[1], ascending=True)
      print('Leats 5 frequencies:', leastFreqs.take(5))

      topLenFreqs = topFreqs.sortBy(lambda x: len(x[0]), ascending=False).persist(StorageLevel(False, True, False, False, int(internal_param[7])))
      print('Top 5 length:', topLenFreqs.take(5))
      
      containsAwords = words.filter(lambda x: 'a' in x)
      print('Number of words containing a:', containsAwords.count())
      
      containsXwords = words.filter(lambda x: 'x' in x)
      print('Number of words containing x:', containsXwords.count())

      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()