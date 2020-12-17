#! /usr/bin/python3.7
import sys
import json
from datetime import datetime
import os
import string
import os.path
import shutil
import sys
import ast

def write_results(data_f, internal_param, resultsFile, totalsFile, file):
    loaded_json = json.dumps(file.readlines())
    log = json.loads(loaded_json)  
    
    appStart = 0
    appEnd = 0
    jobsInfo = []
    stagesInfo = []
    tasksInfo = []
    tasksTimes = []
    JVCGCTimeMetrics = []
    ShuffleRecordsMetrics = []
    ShuffleWriteMetrics = []
    ShuffleBytesWrittenMetrics = []
    
    for event in log:
        x = json.loads(event)
        if x["Event"] == "SparkListenerApplicationStart":
            appStart = datetime.utcfromtimestamp(x["Timestamp"]/1000)
            resultsFile.write("App Start Time: "+str(appStart)+"\n\n")
    
        elif  x["Event"] == "SparkListenerApplicationEnd":
            appEnd = datetime.utcfromtimestamp(x["Timestamp"]/1000)
            resultsFile.write("App Finnish Time: "+str(appEnd)+"\n\n")
    
        elif x["Event"] == "SparkListenerJobStart":
            startTime = datetime.utcfromtimestamp(x["Submission Time"]/1000)
            jobsInfo.append((x["Job ID"], startTime))
            resultsFile.write("  Job "+ str(x["Job ID"]) + " Start, Number of Stages " + str(len(x["Stage IDs"]))+"\n\n")
    
        elif x["Event"] == "SparkListenerStageSubmitted":
            stagesInfo.append((x["Stage Info"]["Stage ID"], x["Stage Info"]["Number of Tasks"]))
            resultsFile.write("    Stage "+ str(x["Stage Info"]["Stage ID"]) + " Start, Number of Tasks " + str(x["Stage Info"]["Number of Tasks"])+"\n\n")
    
        elif x["Event"] == "SparkListenerTaskStart":
            startTime = datetime.utcfromtimestamp(x["Task Info"]["Launch Time"]/1000)
            tasksInfo.append((x["Task Info"]["Task ID"], startTime))
    
        elif x["Event"] == "SparkListenerTaskEnd":     
            for i in range(len(tasksInfo)):
                if tasksInfo[i][0] == x["Task Info"]["Task ID"]:
                    finnishTime = datetime.utcfromtimestamp(x["Task Info"]["Finish Time"]/1000)
                    taskTime = (finnishTime - tasksInfo[i][1]).total_seconds()
                    tasksTimes.append(taskTime)
            JVCGCTime = x["Task Metrics"]["JVM GC Time"]
            JVCGCTimeMetrics.append(int(JVCGCTime))
            ShuffleRecords = x["Task Metrics"]["Shuffle Write Metrics"]["Shuffle Records Written"]
            ShuffleRecordsMetrics.append(int(ShuffleRecords))
            ShuffleWrite = x["Task Metrics"]["Shuffle Write Metrics"]["Shuffle Write Time"]
            ShuffleWriteMetrics.append(int(ShuffleWrite))
            ShuffleBytesWritten = x["Task Metrics"]["Shuffle Write Metrics"]["Shuffle Bytes Written"]
            ShuffleBytesWrittenMetrics.append(int(ShuffleBytesWritten))
            
            resultsFile.write("      Finnish Task "+ str(x["Task Info"]["Task ID"]) +", Total time: "+ str(taskTime)+ "\n")
            resultsFile.write("           JVM GC Time "+ str(x["Task Metrics"]["JVM GC Time"])+ "\n")
            resultsFile.write("           Shuffle Read Metrics "+ str(x["Task Metrics"]["Shuffle Read Metrics"])+ "\n")
            resultsFile.write("           Shuffle Records Written "+ str(x["Task Metrics"]["Shuffle Write Metrics"]["Shuffle Records Written"])+ "\n")
            resultsFile.write("           Shuffle Write Time "+ str(x["Task Metrics"]["Shuffle Write Metrics"]["Shuffle Write Time"])+ "\n")
            resultsFile.write("           Shuffle Bytes Written "+ str(x["Task Metrics"]["Shuffle Write Metrics"]["Shuffle Bytes Written"])+ "\n")
            resultsFile.write("           Input Records Read "+ str(x["Task Metrics"]["Input Metrics"]["Records Read"])+ "\n")
            resultsFile.write("           Input Bytes Read "+ str(x["Task Metrics"]["Input Metrics"]["Bytes Read"])+ "\n")
            resultsFile.write("           Output Records Written " + str(x["Task Metrics"]["Output Metrics"]["Records Written"])+ "\n")
            resultsFile.write("           Output Bytes Written " + str(x["Task Metrics"]["Output Metrics"]["Bytes Written"])+ "\n\n\n")
                    
        elif x["Event"] == "SparkListenerStageCompleted":
            resultsFile.write("    ..........................................")
            
        elif x["Event"] == "SparkListenerJobEnd":
            for i in range(len(jobsInfo)):
                if jobsInfo[i][0] == x["Job ID"]:
                    finnishTime = datetime.utcfromtimestamp(x["Completion Time"]/1000)
                    jobTime = (finnishTime - jobsInfo[i][1]).total_seconds()
                    jobsInfo.append((x["Job ID"], jobTime))
                    resultsFile.write("  Finnish Job "+ str(x["Job ID"]) + ", Total time: "+ str(jobTime)+"\n\n")               
    
    for i in range(len(stagesInfo)):
        resultsFile.write("    Stage "+str(stagesInfo[i][0])+ " completed info:")
        resultsFile.write("\n")
        
        k=0
        if i != 0:
            k = k + int(stagesInfo[i-1][1])
    
        tasks = tasksTimes[k:int(stagesInfo[i][1])+k]
        resultsFile.write("      Max task time: " + str(max(tasks))+ ", task ID: "+ str(tasks.index(max(tasks))+k)+"\n")
        resultsFile.write("      Min task time: " + str(min(tasks))+ ", task ID: "+ str(tasks.index(min(tasks))+k)+"\n")
        mediumTime = sum(tasks)/int(stagesInfo[i][1])
        resultsFile.write("      Medium time of tasks: "+ str(mediumTime))
        totalsFile.write(str(data_f)+str(internal_param[1:])+"MediumTaskTime-Stage"+str(stagesInfo[i][0])+";"+str(mediumTime)+"\n")
        
        JVCGCTimeMetricsStage = JVCGCTimeMetrics[k:int(stagesInfo[i][1])+k]
        mediumJVCGCTimeMetrics = sum(JVCGCTimeMetricsStage)/int(stagesInfo[i][1])
        resultsFile.write("      Medium JVCGCTimeMetrics of tasks: "+ str(mediumJVCGCTimeMetrics))
        totalsFile.write(str(data_f)+str(internal_param[1:])+"MediumTaskJVCGCTimeMetrics-Stage"+str(stagesInfo[i][0])+";"+str(mediumJVCGCTimeMetrics)+"\n")
        
        ShuffleRecordsMetricsStage = ShuffleRecordsMetrics[k:int(stagesInfo[i][1])+k]
        mediumShuffleRecordsMetrics = sum(ShuffleRecordsMetricsStage)/int(stagesInfo[i][1])
        resultsFile.write("      Medium ShuffleRecordsMetrics of tasks: "+ str(mediumShuffleRecordsMetrics))
        totalsFile.write(str(data_f)+str(internal_param[1:])+"MediumTaskShuffleRecordsMetrics-Stage"+str(stagesInfo[i][0])+";"+str(mediumShuffleRecordsMetrics)+"\n")
        
        ShuffleWriteMetricsStage = ShuffleWriteMetrics[k:int(stagesInfo[i][1])+k]
        mediumShuffleWriteMetrics = sum(ShuffleWriteMetricsStage)/int(stagesInfo[i][1])
        resultsFile.write("      Medium ShuffleWriteMetrics of tasks: "+ str(mediumShuffleWriteMetrics))
        totalsFile.write(str(data_f)+str(internal_param[1:])+"MediumTaskShuffleWriteMetrics-Stage"+str(stagesInfo[i][0])+";"+str(mediumShuffleWriteMetrics)+"\n")
        
        ShuffleBytesWrittenMetricsStage = ShuffleBytesWrittenMetrics[k:int(stagesInfo[i][1])+k]
        mediumShuffleBytesWritten = sum(ShuffleBytesWrittenMetricsStage)/int(stagesInfo[i][1])
        resultsFile.write("      Medium ShuffleBytesWritten of tasks: "+ str(mediumShuffleBytesWritten))
        totalsFile.write(str(data_f)+str(internal_param[1:])+"MediumTaskShuffleBytesWritten-Stage"+str(stagesInfo[i][0])+";"+str(mediumShuffleBytesWritten)+"\n")
    
    resultsFile.write("\nApp Total Time " + str((appEnd - appStart).total_seconds()) + " in seconds, " + str((appEnd - appStart).total_seconds()/60))
    resultsFile.write("\n\n\n\n\n")
    resultsFile.write("---------------------------------------------------------------------------------------------\n")
    totalsFile.write(str(data_f)+str(internal_param[1:])+"APPTotalTime;"+str((appEnd - appStart).total_seconds())+"\n")

if __name__ == '__main__':
    
    if sys.argv[5].replace(',', '') in ('word_count_repartition_n', 'word_count_sort_repartition_n', 'word_count_plus_repartition_n'):
      args = sys.argv[1]+";"+sys.argv[2]+";"+sys.argv[3]+";"+str(sys.argv[4])+str(sys.argv[5])+str(sys.argv[6])+str(sys.argv[7])+str(sys.argv[8])+str(sys.argv[9])+str(sys.argv[10])+str(sys.argv[11])

    else:
      args = sys.argv[1]+";"+sys.argv[2]+";"+sys.argv[3]+";"+str(sys.argv[4])+str(sys.argv[5])+str(sys.argv[6])+str(sys.argv[7])+str(sys.argv[8])+str(sys.argv[9])+str(sys.argv[10])
      
    argList = args.split(';')
    
    app_id = argList[0]
    log_dir = argList[1]
    data_f = argList[2]
    internal_param = argList[3]

    file = open(log_dir+"/"+internal_param.strip('][').split(',')[1]+"_"+app_id)
    resultsFile = open("/home/janira/results"+str(data_f)+"/result-"+str(internal_param.strip('][').split(',')[1:])+".txt","a")
    totalsFile = open("/home/janira/results"+str(data_f)+"/totals.txt","a")
    
    write_results(data_f, internal_param, resultsFile, totalsFile, file)