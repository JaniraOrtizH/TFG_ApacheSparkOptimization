# TFG_ApacheSparkOptimization

A continuación se describe el flujo de ejecución de las aplicaciones objeto de estudio del TGF Optimización en Apache Spark para la finalización del Grado en Matemáticas - Itinerario de Ciencias de la Computación, en la Facultad de Matemáticas de la Universidad Complutense de Madrid

Comandos para las ejecuciones:

**1. run_app.py**

  Este es el script principal donde se establecen:
  - Las listas de los scripts a ejecutar según el punto de estudio: config, repartition, pesist, skew, all.
  
      *apps={'config':scriptsConfig, 'repartition':scriptsRepartition, 'persist':scriptsPersist, 'skew':scriptsSkew, 'all':scripts}*
      
  - Los paths de cada tipo de datasets junto a los archivos de datos de ese tipo que se van a ejecutar:
      
      *data_paths={'dic':dict_paths,'word':word_paths,'gsod':gsod_paths}*
  
  - Las variables de configuración con las que se van a ejecutar las aplicaciones:
  
      *driver_cores = [1]*
      
      *driver_mem = ['1g']*
      
      *executors = [1,2,3,4]*
      
      *executor_mem = ['1g', '1500m', '2g', '2500m', '3g']*
      
      *executor_cores = [3,4]*
      
  - Bucles de ejecución de cada aplicación con las varibles de configuración anteriormente establecidas.
  
    * Existe el caso especial de los scripts 'word_count_repartition_n', 'word_count_sort_repartition_n', 'word_count_plus_repartition_n', en los que hay una variable adicional que establece el número por el que se multiplica la cantidad de particiones que queremos establecer (executor.cores * executor.instances * num):
    
    *if app in ('word_count_repartition_n', 'word_count_sort_repartition_n', 'word_count_plus_repartition_n'):*
    *num = ['1.5', '2', '3.5', '4']*
    
  El comando para la ejecución de este script tiene la siguiente estructura:
  
    python3 run_app.py [apps] + [mode] [log_dir] [rep] [path]
  
  donde,
  
  - [apps] indica los script que queremos ejecutar: *config* (estudio de la configuración del cluster), *repartition* (estudio de las particiones en los RDDs), persist (estudio de la persistencia de los RDDs), skew (estudio del sesgo de datos) o all (ejecutar todos los scripts anteriores)
  
  - [mode] indica el modo de ejecución: test (muestra los parámetros sin realizar la ejecución) o run (ejecución completa)
  
  - [log_dir] indica la ruta de almacenamiento de los logs de ejecución
  
  - [rep] indica el número de ejecuciones por configuración que se realizarán
  
  - [path] indica el tipo de dataset con el que se van a realizar las ejecuciones: *words*, *dic* o *gsod*
  
  Ejemplo: Ejecución de los script de estudio de la configuración en modo run, con cuatro repeticiones por ejecución para los dataset correspondientes a word:
  
    python3 run_app.py config + run /home/janira/logs 4 words
    

**2. run_job.py**

   Este script es llamado por *run_app.py* de forma automática y es en el que se generan las rutas y los archivos, según los parámetros de ejecución, para alamacenar los resulados y se realiza la llamada a la función del script correspondiente a la aplicación que se va a ejecutar:
    
   *eval(app)(internal_param, data_f)* -> llamada a la función de la aplicación*
      
   *os.system('python3 /home/janira/logscript.py '+str(app_id)+' '+str(log_dir)+' '+str(data_f)+' '+str(internal_param))* -> llamada al script *logscript.py* de generación de resultados*
    
**3. logscript.py**

    Script de generación del archivo totals.txt, en la ruta de resultados correspondiente al dataset con el que estemos ejecutando, en el que se recogen tiempos totales y otros datos de interés sobre las ejecuciones. Ejemplo de ruta de archivo generado:
    
      */home/janira/results/public_data/words/dic_15.txt/totals.txt*
    
**4. config_scripts.py, repartition_scripts.py, persist_scripts.py, skew_scripts.py**

  Estos scripts recogen las diferentes funciones que corresponden a cada aplicación que se va a ejecutar según los argumentos de elección de run_app.py
    
**5. totals.py**

  Tras la finalización de nuestras ejecuciones mediante el lanzamiento de run_app.py como se describe en el punto 1, se ejecutará el siguiente comando:
    
        python3 totals.py
    
  que generará un archivo de texto *mediumTotalTimes.txt* con la media de las repeticiones de los datos de ejecución alamacenados en totals.txt. Ejemplo de ruta de archivo generado:
    
      */home/janira/results/public_data/words/dic_15.txt/mediumTotalTimes.txt*
    
  Este script depende de la información almacenada en el archivo */home/janira/results/pwd.txt* (que se genera en run_job.py), en el que se especifica la ruta en la que se encuentra el archivo totals.txt.
  Este paso está pensado para que se ejecute al terminar la ejecución de run_app.py, pero si se quiere aplicar total.py sobre otro archivo totals.txt que no sea el último que se ha generado, basta con editar el archivo */home/janira/results/pwd.txt* para indicar la ruta deseada.
    
