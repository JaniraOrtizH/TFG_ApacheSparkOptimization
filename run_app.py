import sys
import json
from datetime import datetime
import os
import string
import os.path

sep = sys.argv.index('+')
internal_param = sys.argv[:sep]
mode, log_dir, rep, dataset = sys.argv[sep+1:]

scriptsConfig = ['word_count']

scriptsRepartition = ['word_count_plus_repartition_n']#'word_count_repartition_n', 'word_count_sort_repartition_n', 'word_count_plus_repartition_n']

scriptsPersist = ['word_count_persist_mem_only', 'word_count_sort_pesist_mem_only', 'word_count_plus_pesist_mem_only', 'word_count_persist_mem_and_disk', 'word_count_sort_pesist_mem_and_disk', 'word_count_plus_pesist_mem_and_disk', 'word_count_persist_disk_only', 'word_count_sort_pesist_disk_only','word_count_plus_pesist_disk_only']

scriptsSkew = ['word_count_join_vocals', 'word_count_join_vocals_broadcast', 'word_count_join_vocals_vs_constants', 'word_count_join_vocals_vs_constants2']

scripts = scriptsConfig + scriptsRepartition + scriptsPersist

word_files=['words_10e7_1.txt']#['words_10e7_1.txt']
word_paths=map(lambda x: '/public_data/words/'+x, word_files)#generamos los paths para la llamada

dict_files=['words_10e7_100.txt']
dict_paths=map(lambda x: '/public_data/words/'+x, dict_files)#generamos los paths para la llamada

gsod_files=['gsod_s05_s.txt']
gsod_paths=map(lambda x: '/public_data/gsod/'+x, gsod_files)#generamos los paths para la llamada

data_paths={'dic':dict_paths,'words':word_paths,'gsod':gsod_paths}

apps={'config':scriptsConfig, 'repartition':scriptsRepartition, 'persist':scriptsPersist, 'skew':scriptsSkew, 'all':scripts}
driver_cores = [1]
driver_mem = ['2g']
executors = [5]
executor_mem = ['6g']#'2g', '2500m', '3g', '3500m', '4g', '4500m', '5g', '5500m', '6g']
executor_cores = [4]

for data_f in data_paths[dataset]:
  for app in apps[internal_param[1]]:
    if app in scriptsRepartition:
      mult_partition = ['3', '4', '5', '6', '7', '8', '9', '10']# '20', '100', '0.2', '0.25', '0.75', '0.5', ]
      for n in mult_partition:
        for r in driver_cores:
          for s in driver_mem:
            for t in executor_mem:    
              for x in executors:
                for y in executor_cores:    
                  for i in range(int(rep)):
                    print("Repetition: "+str(i))
                    print("Driver Cores: "+str(r))
                    print("Driver Memory: "+str(s))
                    print("Executors: "+str(x))
                    print("Executor Memory: "+str(t))
                    print("Executor Cores: "+str(y))
                    os.system('python3 run_job.py '+str(app)+' '+str(r)+' '+str(s)+' '+str(x)+' '+str(t)+' '+str(y)+' '+n+' + '+str(mode)+' '+str(log_dir)+' '+str(rep)+' '+str(data_f))
    elif app in scriptsPersist:
      replicates = ['2', '3', '4', '5']
      for n in replicates:
        for r in driver_cores:
          for s in driver_mem:
            for t in executor_mem:    
              for x in executors:
                for y in executor_cores:    
                  for i in range(int(rep)):
                    print("Repetition: "+str(i))
                    print("Driver Cores: "+str(r))
                    print("Driver Memory: "+str(s))
                    print("Executors: "+str(x))
                    print("Executor Memory: "+str(t))
                    print("Executor Cores: "+str(y))
                    os.system('python3 run_job.py '+str(app)+' '+str(r)+' '+str(s)+' '+str(x)+' '+str(t)+' '+str(y)+' '+n+' + '+str(mode)+' '+str(log_dir)+' '+str(rep)+' '+str(data_f))
    else:
      for r in driver_cores:
        for s in driver_mem:
          for t in executor_mem:    
            for x in executors:
              for y in executor_cores:    
                for i in range(int(rep)):
                  print("Repetition: "+str(i))
                  print("Driver Cores: "+str(r))
                  print("Driver Memory: "+str(s))
                  print("Executors: "+str(x))
                  print("Executor Memory: "+str(t))
                  print("Executor Cores: "+str(y))
                  os.system('python3 run_job.py '+str(app)+' '+str(r)+' '+str(s)+' '+str(x)+' '+str(t)+' '+str(y)+' + '+str(mode)+' '+str(log_dir)+' '+str(rep)+' '+str(data_f))
                  
  os.system('python3 totals.py '+str(data_f)+' totals.txt')

                  

