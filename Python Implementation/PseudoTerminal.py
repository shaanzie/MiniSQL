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

    if(not file_on_hdfs(file)):

        return 0

    with open('metastore.txt', 'r') as file:

        lines = file.readlines()

    file.close()

    for line in lines:

        if(file in line):

            return 1
    
    return 0

def parse_load(query):

    if(re.search(r"^([a-zA-Z0-9_\-\.]+)\/([a-zA-Z0-9_\-\.]+)\.[csv$]", query[1]) and exists(query[1])):

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

    table_columns = dict()
    table_datatypes = dict()
    database = query[1]
    columns = []
    datatypes = []


    for i in query[3][1:-1].split(","):
        
        columns.append(i.split(":")[0])
        datatypes.append(i.split(":")[1])

    table_columns[database] = columns
    table_datatypes[database] = datatypes

    file = open('metastore.txt', 'a+')

    file.write(str(table_columns) + '\n')

    file.close()

    file = open('metastore-datatypes.txt', 'a+')

    file.write(str(table_datatypes) + '\n')

    file.close()


def select(query):

    # Add the codegen stuff here

    generate(query)

    comd =  '$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-*streaming*.jar \
            -file /home/shaanzie/Desktop/College/Sem\ 5/MiniSQL/Python\ Implementation/mapper.py \
            -mapper /home/shaanzie/Desktop/College/Sem\ 5/MiniSQL/Python\ Implementation/mapper.py \
            -file /home/shaanzie/Desktop/College/Sem\ 5/MiniSQL/Python\ Implementation/reducer.py  \
            -reducer /home/shaanzie/Desktop/College/Sem\ 5/MiniSQL/Python\ Implementation/reducer.py  \
            -input' +  query[3]  + '\
            -output /home/shaanzie/Desktop/College/Sem\ 5/MiniSQL/Python\ Implementation/hello'

    os.system(comd)

    

def delete(query):

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
    query = input().split(' ')    

    if(query[0] == 'exit'):
        exit(1)
    
    elif(query[0] == 'load' and parse_load(query)):
        load(query)
    
    elif(query[0] == 'select'):
        select(query)

    elif(query[0] == 'delete' and parse_delete(query)):
        delete(query)

    else:
        print("Query error!")
