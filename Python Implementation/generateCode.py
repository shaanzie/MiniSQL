import ast
#
# query = input().lower()
# query = query.replace(';', ' ;').replace(",", ", ")
# tokens = query.split()

# //assuming that only one table is used per query
# //store output of a query?
# // point to hdfs location?

def getIndex(column, table, tables):
    i = 0
    colNames = [x[0] for x in tables[table]]
    return colNames.index(column.strip())
    # # for i in range(0, len(tables[table])):
    # #     print(tables[table][i][0], "searching for: ", column)
    # #     if str(tables[table][i][0]) == str(column):
    # #         return i
    # raise Exception("column not found")

def parseProjections(projections, table, tables):
    # //only columns for now
    aggregations = []
    indices = []
    print(projections)
    for projection in projections:
        if '(' in projection:
            # this ones an aggregation.
            aggr, col = projection.split('(')[0], projection.split('(')[1][:-1]
            dt = getDataTypeFromName(col, table, tables)
            if dt == "str" and aggr != "count":
                raise Exception("cannot perform the following aggregation on a string")
            else:
                aggregations.append([aggr, getIndex(col, table, tables)])
        else:

            if projection == "*":
                indices.extend(list(range(0, len(tables[table]))))
            else:
                indices.append(getIndex(projection, table, tables))
    return (indices, aggregations)

def getDataTypeFromName(col, table, tables):
    return tables[table][getIndex(col, table, tables)][1]

def getDataTypeFromIndex(ind, table, tables):
    return tables[table][ind][1]

def parseClauses(whereClauses, table, tables):
    # assuming clauses only based on preexisting data
    parsedClauses = []
    for clause in whereClauses:
        if '==' in clause:
            s = '=='
        elif "<" in clause:
            s = "<"
        elif ">" in clause:
            s = ">"
        elif "!" in clause:
            s = "!"

        col, condn = clause.split(s)[0], s + clause.split(s)[1]

        print(col, "aggr")
        dt = getDataTypeFromName(col, table, tables)

        print(dt)
        if dt == "str" and (">" in condn or "<" in condn):
            raise Exception("cannot perform operation on a string data tpye.\n")
        elif dt in {"int", "float", "str"}:
            parsedClauses.append((getIndex(col, table, tables), condn))
        else:
            raise Exception("invalid data type")
    return parsedClauses, []


def genOpString(cols):
    s = "print("
    for col in cols:
        s += "values[" + str(col) + "], "
    s += "sep = ' ')\n"
    return s

def genWhereBlock(clauses, conjunctions, table, tables):
    if len(clauses) == 0:
        return ""

    s = "if "
    i = 0
    for clause in clauses:
        dt = getDataTypeFromIndex(clause[0], table, tables)
        if dt != "string":
            ex0 = dt + "("
            ex1 = ")"
        else:
            ex0 = ""
            ex1 = ""
        s += ex0 + "values[" + str(clause[0]) + "]" + ex1 + " " + clause[1]
        if len(conjunctions) == i:
            s += ":"
        else :
            s += " " + conjunctions[i] + " "
        i += 1
    s += "\n\t\t"
    return s

def genGlobalVars(aggregations):
    s = ""

    for aggr in aggregations:
        if aggr[0] == "avg":
            if ["count", aggr[1]]  not in aggregations:
                s += "countcol" + str(aggr[1]) + " = 0\n"
            if ["sum", aggr[1]] not in aggregations:
                s += "sumcol"+ str(aggr[1]) + " = 0\n"
        else:
            s += aggr[0] + "col" + str(aggr[1]) + " = 0\n"
    return s

def updateAggrs(aggrs, table, tables):
    s = ""
    # print(aggrs)
    for aggr in aggrs:
        dt = getDataTypeFromIndex(aggr[1], table, tables)
        if dt != "string":
            ex0 = dt + "("
            ex1 = ")"
        else:
            ex0 = ""
            ex1 = ""

        if (aggr[0] == "avg" and ["sum", aggr[1]] not in aggrs) or aggr[0] == "sum":
            s += "sumcol" + str(aggr[1]) + " += " + ex0 + "values[" + str(aggr[1]) + "]" + ex1 + "\n\t"

            # print(s)
        if (aggr[0] == "avg" and ["count", aggr[1]] not in aggrs) or aggr[0] == "count":
            s += "countcol" + str(aggr[1]) + " += " + "1\n\t"
        elif aggr[0] == "max":
            s += "if maxcol" + str(aggr[1]) + " < " + ex0 + "values[" + str(aggr[1]) + "]" + ex1 + ":\n\t\tmaxcol" + str(aggr[1]) + " = values[" + str(aggr[1]) + "]\n\t"
        elif aggr[0] == "min":
            s += "if mincol" + str(aggr[1]) + " > " + ex0 + "values[" + str(aggr[1]) + "]:\n\t\tmincol" + str(aggr[1]) + " = values[" + str(aggr[1]) + "]\n\t"
    return s

def printGlobalVars(aggrs):
    s = ""
    for aggr in aggrs:
        if aggr[0] == "avg":
                s += "print(\"average: \", sumcol" + str(aggr[1]) + "/countcol" + str(aggr[1]) + ")\n"
        else:
            s += "print(\"" + aggr[0] + ": \", " + aggr[0] + "col" + str(aggr[1]) + ")\n"
    return s


#
#
# tables = {"table1": ["1", "2", "3"]}
# tableDataTypes = {"table1": ["int", "str", "float"]}


def generate(query):
    query = query.lower()
    query = query.replace(';', ' ;').replace(",", ", ")
    tokens = query.split()
    aggregations = {"sum", "min", "max", "avg", "count"}

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


    # data types must be taken into account for aggregations min, max, sum, avg for strings
    # only allowed string comparisons are "==" and "!="
    # type casting is necessary for comparisons and updates


    # doesnt check if the thing being checked against in where is a string or not

    # //assuming the query has a valid structure
    while valid and tokens[i] != "from":
        projections.append(tokens[i].replace(",", ""))
        i += 1


    i += 1
    table = tokens[i]
    print(table)

    tables = dict()
    with open('metastore.txt', 'r') as file:
        lines = file.readlines()
        # print(line)
        for line in lines:
            if table in line:
                print(line)
                tables.update(ast.literal_eval(line))

    print(tables)

    # //check if table is in tables set

    columnsInQuery, aggregationsInQuery = parseProjections(projections, table, tables)
    print(aggregationsInQuery)
    # print(parseProjections(projections, table))
    i += 1

    conjunctions = []

    if valid and tokens[i] == "where":
        i += 1
        clause = ""
        while(tokens[i] != ';'):
            if tokens[i] == "and" or tokens[i] == "or":
                whereClauses.append(clause)
                conjunctions.append(tokens[i])
                i += 1
                clause = ""
            else:
                clause += tokens[i] + " "
                i += 1
        whereClauses.append(clause)
        print(whereClauses)

        whereClausesMapper, whereClausesReducer = parseClauses(whereClauses, table, tables)

    elif valid and tokens[i] != ";":
            valid = 0

    if valid:
        print(columnsInQuery)
        print(whereClausesMapper)
        print(conjunctions)


    # all aggregations will be done in the reducer
    outputString = genOpString(columnsInQuery)
    whereBlock = genWhereBlock(whereClausesMapper, conjunctions, table, tables)

    imports = "import csv\nimport sys\n"

    processAndPrint = "for line in sys.stdin:\n\tvalues = line.split(',')\n\t" + whereBlock + "print(line)\n"
    mapper = imports + processAndPrint

    print('mapper : \n')
    print(mapper)


    globalVars = genGlobalVars(aggregationsInQuery)
    updateStatements = updateAggrs(aggregationsInQuery, table, tables)
    globalVarString = printGlobalVars(aggregationsInQuery)
    process = "for line in sys.stdin:\n\tvalues = line.split(',')\n\t" +  updateStatements + outputString + globalVarString

    reducer = imports + globalVars + process
    print("reducer: \n")
    print(reducer)

if __name__ == '__main__':
    q = input()
    generate(q)
