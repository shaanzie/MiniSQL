import sys
from operator import itemgetter

def reducer():


    current_column = None
    current_count = 0
    word = None



    for line in sys.stdin:
        
        line = line.strip()
    
        columns, count = line.split('\t', 1)

        try:
            count = int(count)

        except ValueError:
            
            continue


        if current_column == columns:
            current_count += count
        else:
            if current_column:
               
               for column in current_column:

                   print(column, end = "\t")

            current_count = count
            current_column = word

    if current_column == columns:
    
        for column in current_column:

            print(column, end = "\t")


