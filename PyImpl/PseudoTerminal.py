import re
# from fabric.api import local
# from fabric.context_managers import settings
import os
from pyfiglet import Figlet
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


def parse_delete(query):

    if(re.search(r"^([a-zA-Z0-9_\-\.]+)\/([a-zA-Z0-9_\-\.]+)\.[csv$]", query[1])):

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

    os.system('$HADOOP_HOME/bin/hadoop dfs -rmr /out*')

    pid = os.getpid()

    generate(query, str(pid))

    query = query.replace(', ', ',').split(' ')


    comd =  '$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-*streaming*.jar \
            -file /home/hduser/MiniSQL/PyImpl/mapper_generated_'+ str(pid) + '.py \
            -mapper /home/hduser/MiniSQL/PyImpl/mapper_generated_'+ str(pid) + '.py \
            -file /home/hduser/MiniSQL/PyImpl/reducer_generated_'+ str(pid) + '.py \
            -reducer /home/hduser/MiniSQL/PyImpl/reducer_generated_'+ str(pid) + '.py  \
            -input /' +  query[3].replace(";", "")  + '\
            -output /out/'

    os.system(comd)

    os.system('$HADOOP_HOME/bin/hadoop dfs -cat /out/part-00000')

    os.system('rm -rf /home/hduser/MiniSQL/PyImpl/reducer_generated_'+ str(pid) + '.py /home/hduser/MiniSQL/PyImpl/mapper_generated_'+ str(pid) + '.py')

def delete(query):

    query = query.split(' ')

    with open('metastore.txt', 'r') as file:
        lines = file.readlines()

    with open('metastore.txt', 'w') as file:
        for line in lines:
            if query[1][:-1] not in line:
                 file.write(line)


os.system('clear')
print('____________________________________________________________________________')
print()
print()
f = Figlet(font='slant')
print(f.renderText('     MiniSQL'))

print('____________________________________________________________________________')
print()

while(True):

    print('>', end = ' ')
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
