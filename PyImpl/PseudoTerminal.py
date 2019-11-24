import re
# from fabric.api import local
# from fabric.context_managers import settings
import os
from generateCode import generate

def file_on_hdfs(file):

    file_exists = False

    with settings(warn_only = True):

        results = local('hadoop fs -stat hdfs://' + file, capture = True)
        file_exists = results.succeeded

    if(file_exists):

        return 1

    return 0


def exists(file):

    # if(not file_on_hdfs(file)):

    #     return 0

    with open('metastore.txt', 'r') as file:

        lines = file.readlines()

    file.close()

    for line in lines:

        if(file in line):

            return 1
    
    return 0

def parse_load(query):

    if(re.search(r"^([a-zA-Z0-9_\-\.]+)\/([a-zA-Z0-9_\-\.]+)\.[csv$]", query[1])):

        if(query[2] == 'as'):
        
            for i in query[3][1:-1].split(","):
        
                if(re.search(r"^([a-zA-Z0-9_\-\.]+)\:([a-zA-Z0-9_\-\.]+)", i)):
        
                    return 1
    return 0
    

def parse_select(query):

    if(re.search(r"(([a-zA-Z0-9_\-\.]+))", query[1]) or query[1] == '*'):

        if(query[2] == 'from'):
        
            if(re.search(r"^([a-zA-Z0-9_\-\.]+)\/([a-zA-Z0-9_\-\.]+)\.[csv$]", query[3]) and exists(query[3])):
        
                if(query[4] == 'where'):

                    if(re.search(r"([a-zA-Z0-9_\-\.]+)\=([a-zA-Z0-9_\-\.]+)", query[5])):
                
                        return 1

                elif(query[3][-1] == ';'):

                    return 1
    return 0

def parse_delete(query):

    if(re.search(r"^([a-zA-Z0-9_\-\.]+)\/([a-zA-Z0-9_\-\.]+)\.[csv$]", query[1]) and exists(query[1])):

        return 1

    return 0
    

def load(query):

    query = query.split(' ')

    table_columns = dict()
    database = query[1]
    columns = []
    datatypes = []


    for i in query[3][1:-2].split(","):
        
        columns.append((i.split(":")[0], i.split(":")[1]))

    table_columns[database] = columns

    with open('metastore.txt', 'a+') as file:
        
        lines = file.readlines()
        

    flag = 0

    with open('metastore.txt', 'a+') as file:

        for line in lines:
            
            if(query[1] in line):
                flag = 1
                file.write(str(table_columns) + '\n')
            else:
                file.write(line)
        
        if flag == 0:
            file.write(str(table_columns) + '\n')


def select(query):

    # Add the codegen stuff here

    generate(query)

    query = query.split(' ')

    comd =  '$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-*streaming*.jar \
            -file /home/hduser/MiniSQL/PyImpl/mapper_generated.py \
            -mapper /home/hduser/MiniSQL/PyImpl/mapper_generated.py \
            -file /home/hduser/MiniSQL/PyImpl/reducer_generated.py  \
            -reducer /home/hduser/MiniSQL/PyImpl/reducer_generated.py  \
            -input /' +  query[3][:-1]  + '\
            -output /out9/'


    # print(comd)

    os.system(comd)

    

def delete(query):

    query = query.split(' ')

    with open('metastore.txt', 'r') as file:
        lines = file.readlines()

    with open('metastore.txt', 'w') as file:
        for line in lines:
            if query[1] not in line:
                 file.write(line)

    with open('metastore-datatypes.txt', 'r') as file:
        lines = file.readlines()

    with open('metastore-datatypes.txt', 'w') as file:
        for line in lines:
            if query[1] not in line:
                 file.write(line)


while(True):
    
    print('>')
    query = input()  
    query_copy = query.split(' ')

    if(query_copy[0] == 'exit'):
        exit(1)
    
    elif(query_copy[0] == 'load' and parse_load(query_copy)):
        load(query)
    
    elif(query_copy[0] == 'select'):
        select(query)

    elif(query_copy[0] == 'delete' and parse_delete(query_copy)):
        delete(query)

    else:
        print("Query error!")
