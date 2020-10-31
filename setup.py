import os


#===================================================================
#create subfolders required by the code

dir_path = os.path.dirname(os.path.realpath(__file__))

data_path = dir_path + "/data/"
log_path = dir_path + "/log/"
zip_path = dir_path + "/archived/"

#if dir doesn't exist create it 
if not os.path.exists(data_path):
    os.makedirs(data_path)
if not os.path.exists(log_path):
    os.makedirs(log_path)
if not os.path.exists(zip_path):
    os.makedirs(zip_path)
    
#===================================================================

##PACKAGE DEPENDANCY

#conda install --name etl-processing  psycopg2
