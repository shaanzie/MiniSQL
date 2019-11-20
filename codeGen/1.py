query = input().lower()
query = query.replace(';', ' ;')
tokens = query.split()

# //assuming that only one table is used per query
# //store output of a query?
# // point to hdfs location?

def getIndex(column, table):
    return tables[table].index(column.strip())

def parseProjections(projections, table):
    # //only columns for now
    indices = []
    print(projections)
    for projection in projections:
        if projection == "*":
            return (list(range(0, len(tables[table]))), [])
        indices.append(getIndex(projection, table))
    return (indices, [])

def parseClauses(whereClauses, table):
    # assuming clauses only based on preexisting data
    parsedClauses = []
    for clause in whereClauses:
        if "<" in clause:
            s = "<"
        elif ">" in clause:
            s = ">"
        elif "!" in clause:
            s = "!"
        elif "=" in clause:
            s = "="
        col, condn = clause.split(s)[0], s + clause.split(s)[1]
        parsedClauses.append((getIndex(col, table), condn))
    return parsedClauses, []


tables = {"table1": ["1", "2", "3"]}
# columns = set()
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

i += 1
projections = []

# //assuming the query has a valid structure
while valid and tokens[i] != "from":
    projections.append(tokens[i])
    i += 1


i += 1
table = tokens[i]
print(table)
# //check if table is in tables set

columnsInQuery, aggregationsInQuery = parseProjections(projections, table)
# print(parseProjections(projections, table))
i += 1

conjunctions = []

if valid and tokens[i] == "where":
    i += 1
    clause = ""
    while(tokens[i] != ';'):
        if tokens[i] == "and" or tokens[i] == "or":
            whereClauses.append(clause)
            conjuntions.append(tokens[i])
            i += 1
            clause = ""
        else:
            clause += tokens[i] + " "
            i += 1
    whereClauses.append(clause)
    print(whereClauses)

    whereClausesMapper, whereClausesReducer = parseClauses(whereClauses, table)

elif valid and tokens[i] != ";":
        valid = 0

if valid:
    print(columnsInQuery)
    print(whereClausesMapper)


globalVars = "" # if aggregations like sum or max etc
outputString = genOpString(columns)

imports = "import csv\nimport sys\n"

processAndPrint = "for line in sys.stdin:\n\tvalues = line.split(",")\n\t" + whereBlock + genOpString
final = imports + globalVars + processAndPrint
