query = input().lower()
query.replace(';', ' ;')
tokens = query.split()

tables = set()
columns = set()
aggregations = {"sum", "min", "max"}


columnsInQuery = []
aggregationsInQuery = []

whereClauses = []
whereClausesMapper = set()
whereClausesReducer = set()

i = 0
valid = 1

if tokens[i] != "select":
    valid = 0

projections = []

# //assuming the query has a valid structure
while valid and tokens[i] != "from":
    projections.append(tokens[i])
    i += 1

columnsInQuery, aggregationsInQuery = parseProjections(projections)

i += 1
table = tokens[i]
# //check if table is in tables set

i += 1
if valid and tokens[i] == "where":
    clause = ""
    while(tokens[i] != ';'):
        if tokens[i] = "and" or tokens[i] in "or":
            whereClauses.append(clause)
            i += 1
            clause = ""
        else:
            clause += tokens[i] + " "
            i += 1
    whereClausesMapper, whereClausesReducer = parseClauses(whereClauses)

else if valid and tokens[i] != ";":
        valid = 0
