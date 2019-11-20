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



def select(query):

    if(query[1] == '*'):

        mapper(query[3], query[5])

    else:

        project(query[1], query[3], query[5])


def delete(query):

    delete_table(query[1])


while(True):
    
    print('>')
    query = input().split(' ')

    if(query[0] == 'exit'):
        exit(1)
    
    elif(query[0] == 'load'):
        load(query)
    
    # elif(query[0] == 'select' and parse_select(query)):
    #     select(query)

    # elif(query[0] == 'delete' and parse_delete(query)):
    #     delete(query)

    else:
        print("Query error!")
