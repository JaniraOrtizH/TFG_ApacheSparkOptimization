import sys
from pyspark import SparkContext, SparkConf
import subprocess
import os

def main():

    resultsFile = open(first_line+"mediumTotalTimes.txt","a")
    resultsFileShuffle = open(first_line+"mediumTotalTimesWithShuffleInfo.txt","a")

    sc = SparkContext()

    data = sc.textFile('/user/janira/totals.txt')
    execTime = data.map(lambda x: x.replace('word_count', 'word_count_a') if 'word_count' == x.split(',')[1] else x).map(lambda x: x.replace('persist', 'a_persist') if 'word_count_persist' in x.split(',')[1] else x).map(lambda x: x.replace('repartition', 'a_repartition') if 'word_count_repartition' in x.split(',')[1] else x).map(lambda x: ((x.split(";")[0]).strip(), [float(x.split(";")[1]), 1]))
    
    grupedExecTime = execTime.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])).map(lambda x: (x[0], x[1][0]/x[1][1]))

    for i in grupedExecTime.filter(lambda x: x[0].split(']')[1].split('-')[0] in ('APPTotalTime','MediumTaskTime')).sortBy(lambda x: x[0].split(',')[1].split('_')[0:2]).sortBy(lambda x: x[1]).sortBy(lambda x: x[0].split(']')[1].split('-')[0]).collect():
      resultsFile.write(str(i)+"\n")
      
    for i in grupedExecTime.sortBy(lambda x: x[1]).sortBy(lambda x: x[0].split(']')[1].split('-')[0]).sortBy(lambda x: x[0].split(',')[1].split('_')[0:2]).collect():
      resultsFileShuffle.write(str(i)+"\n")
        
if __name__ == '__main__':
    with open("/home/janira/results/pwd.txt", "r") as file:
      first_line = file.readline()
      data_file = first_line+"totals.txt"
      os.system('hdfs dfs -put -f ' + data_file)
      os.system('rm '+first_line+"mediumTotalTimes.txt")
      os.system('rm '+first_line+"mediumTotalTimesWithShuffleInfo.txt")
      
    main()
    