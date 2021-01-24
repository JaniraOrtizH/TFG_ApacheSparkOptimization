from pyspark import SparkContext, SparkConf
import sys
import string
import time

def mymapeo(x):
    for y in string.punctuation:
        x = x.replace(y,'').lower()
    line = x.split(" ")
    return line
    
def word_count_join_vocals(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_job.py', 'run_app.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo)

      vocalsDim = sc.textFile('/user/janira/vocalDim.txt').map(lambda x: (x[0], x[1]))
      
      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
      frequencies.collect()
      print(frequencies.take(5))
      
      startWithFreqs = frequencies.map(lambda x: (x[0][0], (x[0], x[1])))
      
      wordsVocalsDim = words.join(vocalsDim)
      wordsVocalsDim.collect()
      print(wordsVocalsDim.take(5))

      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_join_vocals_broadcast(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_job.py', 'run_app.py'])

      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo)

      vocalsDim = sc.textFile('/user/janira/vocalDim.txt').map(lambda x: (x[0], x[1]))

      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
      frequencies.collect()
      print(frequencies.take(5))
      
      startWithFreqs = frequencies.map(lambda x: (x[0][0], (x[0], x[1])))
      
      wordsVocalsDim = words.join(broadcast(vocalsDim.collect()))
      wordsVocalsDim.collect()
      print(wordsVocalsDim.take(5))
      
      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_join_vocals_vs_constants(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_job.py', 'run_app.py'])

      def clasify(x):
      	if x[0][0] in ('a', 'e', 'i', 'o', 'u'):
      		return (x[0][0], (x[0], x[1]))
      	else:
      		return ('C', (x[0], x[1]))
      		
      	
      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo)
      
      letterDim = sc.textFile('/user/janira/vocalVSconstantDim.txt').map(lambda x: (x[0], x[1]))
      
      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
      frequencies.collect()
      print(frequencies.take(5))
      
      startWithFreqs = frequencies.map(clasify)
      
      wordsVocalsDim = words.join(letterDim)
      wordsVocalsDim.collect()
      print(wordsVocalsDim.take(5))

      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()

def word_count_join_vocals_vs_constants2(internal_param, data_file):
    try:
      conf = SparkConf().setMaster("spark://dana:7077").setAppName(internal_param[1]).setAll([('spark.driver.cores', internal_param[2]), ('spark.driver.memory', internal_param[3]), ('spark.executor.instances', internal_param[4]), ('spark.executor.memory', internal_param[5]), ('spark.executor.cores', internal_param[6])])
      sc = SparkContext(conf=conf, pyFiles=['run_job.py', 'run_app.py'])

      def clasify(x):
      	if x[0][0] in ('a', 'e', 'i', 'o', 'u'):
      		return (x[0][0], (x[0], x[1]))
      	else:
      		return (('C',x[0][0]), (x[0], x[1]))
      		
      	
      data = sc.textFile(data_file)
      words = data.flatMap(mymapeo)
      
      letterDim = sc.textFile('/user/janira/vocalVSconstantDim.txt').map(lambda x: (x[0], x[1]))
      
      frequencies= words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
      frequencies.collect()
      print(frequencies.take(5))
      
      startWithFreqs = frequencies.map(clasify)
      
      wordsVocalsDim = words.map(lambda x: (x[0][0], x[1])).join(letterDim)
      wordsVocalsDim.collect()
      print(wordsVocalsDim.take(5))

      app_id = sc.applicationId
      sc.stop()
      return app_id
    except:
      print("Configuration error: "+str(internal_param))
      sc.stop()