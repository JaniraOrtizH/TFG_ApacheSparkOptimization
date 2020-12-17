#! /usr/bin/python3.7
import sys
import json
from datetime import datetime
import os
import string
import os.path
import shutil
from pyspark import SparkContext, SparkConf
import config_scripts
from config_scripts import *
from repartition_scripts import *
from persist_scripts import *
from sesgo_scripts import *


def main(mode, log_dir, rep, internal_param, dataset):
    
    word_files=['words_10e7_1.txt']
    word_paths=map(lambda x: '/public_data/words/'+x, word_files)#generamos los paths para la llamada
    
    dict_files=['dic_15.txt']
    dict_paths=map(lambda x: '/public_data/words/'+x, dict_files)#generamos los paths para la llamada
    
    gsod_files=['gsod_s05_s.txt']
    gsod_paths=map(lambda x: '/public_data/gsod/'+x, gsod_files)#generamos los paths para la llamada
    
    data_paths={'dic':dict_paths,'word':word_paths,'gsod':gsod_paths}
    
    dateTime = datetime.now()
    
    for data_f in data_paths[dataset]:
    
        if not os.path.exists("/home/janira/results"+str(data_f)):
          os.makedirs("/home/janira/results"+str(data_f))
          
        if mode == 'test':
            print('test',internal_param,'@', data_f, '+', rep, log_dir)
            
        elif mode == 'run':
          resultsFile = open("/home/janira/results"+str(data_f)+"/result-"+str(internal_param[1:])+".txt","a")
          totalsFile = open("/home/janira/results"+str(data_f)+"/totals.txt","a")
          os.system('rm /home/janira/results/pwd.txt')
          pwdFile = open("/home/janira/results/pwd.txt","w")
          pwdFile.write("/home/janira/results"+str(data_f)+'/')
          print('exec',internal_param,'@', data_f, '+', rep, log_dir)
          resultsFile.write("---------------------------------------------------------------------------------------------\n")
          resultsFile.write("---------------------------------------------------------------------------------------------\n")
          resultsFile.write("------------DATASET "+ str(data_f) + str(dateTime)+"-- REP NUMBER " + str(rep) +"\n")
          resultsFile.write("---------------------------------------------------------------------------------------------\n")
          resultsFile.write("---------------------------------------------------------------------------------------------\n\n")
          
          try:
            app = str(internal_param[1])
            app_id = eval(app)(internal_param, data_f)
            print('mv', app_id,'to', log_dir)
            shutil.move('/opt/spark/current/logs/'+app_id, log_dir+"/"+str(app)+"_"+app_id)
            os.system('python3 /home/janira/logscript.py '+str(app_id)+' '+str(log_dir)+' '+str(data_f)+' '+str(internal_param))
          except:
            print("Configuration error: "+str(internal_param))                
        else:
            print('mode error, select (test|run)')
        
if __name__ == '__main__':
    sep = sys.argv.index('+')
    internal_param = sys.argv[:sep]
    mode, log_dir, rep, dataset = sys.argv[sep+1:]
    print ('internal parameters', internal_param)
    print ('general parameters', mode, log_dir, rep)
    print ('dataset', dataset)
    if not os.path.exists(log_dir):
            os.makedirs(log_dir)
    main(mode, log_dir, rep, internal_param, dataset)