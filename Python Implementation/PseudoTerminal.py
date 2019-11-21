import re

def parse_load(query):

    if(re.search(r"^([a-zA-Z0-9_\-\.]+)\/([a-zA-Z0-9_\-\.]+)\.[csv$]", query[1])):
        if(query[2] == 'as'):
            # if(re.search(r"(^\[ (([a-zA-Z0-9_\-\.]+) \: ([a-zA-Z0-9_\-\.]+) (\,) (\s) +) \]\;$)", query[3])):
            for i in query[3][1:-1].split(","):
                if(re.search(r"^([a-zA-Z0-9_\-\.]+)\:([a-zA-Z0-9_\-\.]+)", i)):
                    return 1
            else:
                return -1
        else:
            return -2
    else:
        return -3

    return 0

def load(query):

    table = dict()

    database = query[1]
    columns = []


    for i in query[3][1:-1].split(","):
        
        columns.append(i.split(":")[0])

    table[database] = columns

    file = open('metastore.txt', 'a+')

    file.write(str(table) + '\n')

    file.close()


def delete(query):

    with open('metastore.txt', 'r') as file:
        lines = file.readlines()

    with open('metastore.txt', 'w') as file:
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
    
    # elif(query[0] == 'select' and parse_select(query)):
    #     select(query)

    elif(query[0] == 'delete' and parse_delete(query)):
        delete(query)

    else:
        print("Query error!")
