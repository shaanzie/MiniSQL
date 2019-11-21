import sys
from operator import itemgetter

def mapper(where_dict):

    for line in sys.stdin:
        
        line = line.strip()

        columns = line.split(",")

        if(where_dict.empty()):

            print("%s\t%s", (columns, 1))

        else:
            for keys in where_dict.keys:

                if(columns['\i'] == where_dict['\i']):

                    print("%s\t%s", (columns, 1))

  