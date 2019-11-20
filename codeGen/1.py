query = input().lower()
query.replace(';', ' ;')
tokens = query.split()

//store output of a query?
// point to hdfs location?

def parseProjections(projections, table):
    # //only columns for now
    indices = []
    for projection in projections:
        indices.append(indexof(projection, table))
    return indices, []

def parseClauses(whereClauses, table):
    # assuming clauses only based on preexisting data
    parsedClauses = []
    for clause in whereClauses:
        if "<" in clause:
            s = "<"
        else if ">" in clause:
            s = ">"
        else if "!" in clause:
            s = "!"
        else if "=" in clause:
            s = "="
        col, condn = clause.split(s)[0]
        parsedClauses.append((getIndex(col, table), condn))
    return parsedClauses


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


i += 1
table = tokens[i]
# //check if table is in tables set

columnsInQuery, aggregationsInQuery = parseProjections(projections, table)
i += 1

conjunctions = []

if valid and tokens[i] == "where":
    clause = ""
    while(tokens[i] != ';'):
        if tokens[i] = "and" or tokens[i] in "or":
            whereClauses.append(clause)
            conjuntions.append(tokens[i])
            i += 1
            clause = ""
        else:
            clause += tokens[i] + " "
            i += 1
    whereClausesMapper, whereClausesReducer = parseClauses(whereClauses)

else if valid and tokens[i] != ";":
        valid = 0
