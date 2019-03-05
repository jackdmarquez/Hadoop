# -*- coding: utf-8 -*-
import numpy as np
import csv
import subprocess
import time
import os


DATA_FILES = ['15_TA_GRG.csv','15_TA_S.csv','15_TA_E.csv','15_TA_GA.csv','35_TA_GRG.csv','35_TA_S.csv','35_TA_E.csv','35_TA_GA.csv','50_TA_GRG.csv','50_TA_S.csv','50_TA_E.csv','50_TA_GA.csv']

data=np.zeros([50,4],dtype=np.int)

ruta=""
totalTimePerFile=0;
totalTimePerFile_2=0;




fileCont=0

while (fileCont < len(DATA_FILES)):

    cont=0;
    with open(DATA_FILES[fileCont]) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        print(DATA_FILES[fileCont])
        for row in readCSV:
            print((np.asarray(row)))
            data[cont,:]=(np.asarray(row))  
            nameFile="file_"+str(data[cont,0])+".txt"
            
            if (data[cont,1]==1):
                ruta="LAZY"
            if (data[cont,2]==1):
                ruta="ALL_SSD"
            if (data[cont,3]==1):
                ruta="HOT"
        
            direc="filesTest/"
   
            os.system("head -c "+str(data[cont,0])+"MB /dev/zero >"+direc+nameFile)

            start_time = time.time()
            subprocess.call(['hdfs','dfs', '-put', direc+nameFile, ruta])            
            finalTime=(time.time() - start_time)
            totalTimePerFile= totalTimePerFile + finalTime            
            
            start_time_2 = time.time()
            subprocess.call(['hdfs','dfs', '-get', ruta+'/'+nameFile])            
            finalTime_2=(time.time() - start_time_2)
            totalTimePerFile_2= totalTimePerFile_2 + finalTime_2
            cont=cont+1

            os.system("rm -rf "+direc+nameFile)
            os.system("rm -rf /home/hadoop/"+nameFile)
        file = open("results.txt","a")
        file.write(DATA_FILES[fileCont] + ": " + str(totalTimePerFile) + "\n")
        file.write(DATA_FILES[fileCont] + ": " + str(totalTimePerFile_2) + "\n")
    file.close()

    subprocess.call(['hdfs','dfs', '-rm', '-r', '-f', 'LAZY/*'])
    subprocess.call(['hdfs','dfs', '-rm', '-r', '-f', 'HOT/*'])
    subprocess.call(['hdfs','dfs', '-rm', '-r', '-f', 'ALL_SSD/*'])    
    totalTimePerFile=0
    totalTimePerFile_2=0
    fileCont=fileCont+1;
    if(fileCont==4):
        data=np.zeros([35,4],dtype=np.int)
    if(fileCont==8):
        data=np.zeros([50,4],dtype=np.int)
    

