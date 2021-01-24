from pyspark import SparkContext, SparkConf
import sys
import string
import time

def mymapeo(x):
    for y in string.punctuation:
        x = x.replace(y,'').lower()
    line = x.split(" ")
    return line

def word_count_repartition_n(internal_param, data_file):
    try:

      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app.py', 'repartition_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      
      repartitionData = data.repartition(int(int(sc.getConf().get("spark.executor.cores")) * int(sc.getConf().get("spark.executor.instances")) * float(internal_param[7])))
      
      words = repartitionData.flatMap(mymapeo)
      
      print(str(int(data.getNumPartitions())))
      print(str(int(words.getNumPartitions())))
      
      print('NUM WORDS PARTITIONS '+str(int(int(sc.getConf().get("spark.executor.cores")) * int(sc.getConf().get("spark.executor.instances")) * float(internal_param[7]))))
      
      frequencies= words.filter(lambda x: x != '').map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
      print('Words frequencies:',frequencies.take(5))
      
      print('NUM FREQUENCIES PARTITIONS '+str(int(frequencies.getNumPartitions())))


      app_id = sc.applicationId
      
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_sort_repartition_n(internal_param, data_file):      
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      repartitionData = data.repartition(int(int(sc.getConf().get("spark.executor.cores")) * int(sc.getConf().get("spark.executor.instances")) * float(internal_param[7])))
      
      words = repartitionData.flatMap(mymapeo)
      
      print(str(int(data.getNumPartitions())))
      print(str(int(words.getNumPartitions())))
      
      print('NUM WORDS PARTITIONS '+str(int(int(sc.getConf().get("spark.executor.cores")) * int(sc.getConf().get("spark.executor.instances")) * float(internal_param[7]))))
      
      frequencies= words.filter(lambda x: x != '').map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
      
      repartfrequencies = frequencies.repartition(int(int(sc.getConf().get("spark.executor.cores")) * int(sc.getConf().get("spark.executor.instances")) * float(internal_param[7])))

      numWords = data.count()
      sortFreq = frequencies.sortBy(lambda x: x[1], ascending=False)
      topFreqs = sortFreq.take(5)
      
      print('Number of words: ', numWords)
      print('Words frequencies:', sortFreq.take(5))
      print('Top 5 frequencies:', topFreqs)
      
      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_plus_repartition_n(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_app_small.py', 'run_app.py', 'sesgo_scripts.py', 'persist_scripts.py', 'repartition_scripts.py', 'config_scripts.py', 'wordCountConfig.py'])

      data = sc.textFile(data_file)
      repartitionData = data.repartition(int(int(sc.getConf().get("spark.executor.cores")) * int(sc.getConf().get("spark.executor.instances")) * float(internal_param[7])))
      
      words = repartitionData.flatMap(mymapeo)
      
      print(str(int(data.getNumPartitions())))
      print(str(int(words.getNumPartitions())))
      
      print('NUM WORDS PARTITIONS '+str(int(int(sc.getConf().get("spark.executor.cores")) * int(sc.getConf().get("spark.executor.instances")) * float(internal_param[7]))))
      
      frequencies= words.filter(lambda x: x != '').map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
      repatFrequencies = frequencies.repartition(int(int(sc.getConf().get("spark.executor.cores")) * int(sc.getConf().get("spark.executor.instances")) * float(internal_param[7])))
      
      topFreqs = repatFrequencies.sortBy(lambda x: x[1], ascending=False)
      repartTopFreqs = topFreqs.repartition(int(int(sc.getConf().get("spark.executor.cores")) * int(sc.getConf().get("spark.executor.instances")) * float(internal_param[7])))
      print('Top 5 frequencies:', repartTopFreqs.take(5))
      
      leastFreqs = repatFrequencies.sortBy(lambda x: x[1], ascending=True)
      repartLeastFreqs = leastFreqs.repartition(int(int(sc.getConf().get("spark.executor.cores")) * int(sc.getConf().get("spark.executor.instances")) * float(internal_param[7])))
      print('Leats 5 frequencies:', repartLeastFreqs.take(5))

      topLenFreqs = repartTopFreqs.sortBy(lambda x: len(x[0]), ascending=False)
      repartTopLenFreqs = topLenFreqs.repartition(int(int(sc.getConf().get("spark.executor.cores")) * int(sc.getConf().get("spark.executor.instances")) * float(internal_param[7])))
      print('Top 5 length:', repartTopLenFreqs.take(5))
      
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