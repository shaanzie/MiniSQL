def parse_load(query):

    if(query[1] == "*.csv"):

        if(query[2] == 'as'):

            if(query[3] == "*.;"):

                return True

    return False



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
    
    elif(query[0] == 'load'):
        load(query)
    
    # elif(query[0] == 'select' and parse_select(query)):
    #     select(query)

    elif(query[0] == 'delete'):
        delete(query)

    else:
        print("Query error!")
