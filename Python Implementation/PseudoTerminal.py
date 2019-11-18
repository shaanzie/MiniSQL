def parse_load(query):

    if(query[1] == "*.csv"):

        if(query[2] == 'as'):

            if(query[3] == "*.;"):

                return True

    return False



def load(query):

    columns = []

    for i in query[3].split(","):
            
        columns.append(i)



def select(query):

    if(query[1] == '*'):

        identity_map(query[3], query[5])

    else:

        project(query[1], query[3], query[5])


def delete(query):

    delete_table(query[1])


while(True):
    
    print('>')
    query = input().split(' ')

    if(query[0] == 'exit'):
        exit(1)
    
    elif(query[0] == 'load' and parse_load(query)):
        if(parse_load(query)):
            load(query)
    
    elif(query[0] == 'select' and parse_select(query)):
        select(query)

    elif(query[0] == 'delete' and parse_delete(query)):
        delete(query)

    else:
        print("Query error!")
