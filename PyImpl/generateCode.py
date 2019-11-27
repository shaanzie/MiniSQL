import ast
import os


def getIndex(column, table, tables):
    i = 0
    colNames = [x[0] for x in tables[table]]
    return colNames.index(column.strip())

def parseProjections(projections, table, tables):
    # //only columns for now
    aggregations = []
    indices = []


    for projection in projections:
        if 'count(*)' in projection:
            projection = projection.replace('count(*)', 'count(' + tables[table][0][0] + ')')
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

        dt = getDataTypeFromName(col, table, tables)

        if dt == "str" and (">" in condn or "<" in condn):
            raise Exception("cannot perform operation on a string data tpye.\n")
        elif dt in {"int", "float", "str"}:
            parsedClauses.append((getIndex(col, table, tables), condn))
        else:
            raise Exception("invalid data type")
    return parsedClauses, []


def genOpString(cols):
    s = ""
    if len(cols):
        s += "print("
        for col in cols:
            s += "values[" + str(col) + "], "
        s += "sep = ' ')\n"
    return s + '\n'

def genWhereBlock(clauses, conjunctions, table, tables, indentation):
    if len(clauses) == 0:
        return ""

    s = indentation + "if "
    i = 0
    for clause in clauses:
        dt = getDataTypeFromIndex(clause[0], table, tables)
        if dt != "str":
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
    s += "\n" + indentation
    return s

def genGlobalVars(aggregations):
    s = ""

    for aggr in aggregations:
        if aggr[0] == "avg":
            if ["count", aggr[1]]  not in aggregations:
                s += "countcol" + str(aggr[1]) + " = 0\n"
            if ["sum", aggr[1]] not in aggregations:
                s += "sumcol"+ str(aggr[1]) + " = 0\n"
        if aggr[0] == "min":
            s += "mincol"+ str(aggr[1]) + " = sys.maxsize\n"
        else:
            s += aggr[0] + "col" + str(aggr[1]) + " = 0\n"

    return s

def updateAggrs(aggrs, table, tables, indentation):
    s = ""
    for aggr in aggrs:
        dt = getDataTypeFromIndex(aggr[1], table, tables)
        if dt != "string":
            ex0 = dt + "("
            ex1 = ")"
        else:
            ex0 = ""
            ex1 = ""

        if (aggr[0] == "avg" and ["sum", aggr[1]] not in aggrs) or aggr[0] == "sum":
            s += indentation + "sumcol" + str(aggr[1]) + " += " + ex0 + "values[" + str(aggr[1]) + "]" + ex1 + "\n"

        if (aggr[0] == "avg" and ["count", aggr[1]] not in aggrs) or aggr[0] == "count":
            s += indentation + "countcol" + str(aggr[1]) + " += " + "1\n"

        elif aggr[0] == "max":
            s += indentation + "if maxcol" + str(aggr[1]) + " < " + ex0 + "values[" + str(aggr[1]) + "]" + ex1 + ":\n" + indentation + "\tmaxcol" + str(aggr[1]) + " = float(values[" + str(aggr[1]) + "])\n"

        elif aggr[0] == "min":
            s += indentation + "if mincol" + str(aggr[1]) + " > " + ex0 + "values[" + str(aggr[1]) + "]" + ex1 + ":\n" + indentation + "\tmincol" + str(aggr[1]) + " = float(values[" + str(aggr[1]) + "])\n"

    return s

def printGlobalVars(aggrs):
    s = ""
    for aggr in aggrs:
        if aggr[0] == "avg":
                s += "print(\"average: \", sumcol" + str(aggr[1]) + "/countcol" + str(aggr[1]) + ")\n"
        else:
            s += "print(\"" + aggr[0] + ": \", " + aggr[0] + "col" + str(aggr[1]) + ")\n"
    return s


def generate(query, pid):
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

    tables = dict()
    with open('metastore.txt', 'r') as file:
        lines = file.readlines()
        for line in lines:
            if table in line:
                tables.update(ast.literal_eval(line))

    # tables = {'table1': [('1', 'int'), ('2', 'str')]}

    # //check if table is in tables set

    columnsInQuery, aggregationsInQuery = parseProjections(projections, table, tables)


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
        whereClausesMapper, whereClausesReducer = parseClauses(whereClauses, table, tables)

    elif valid and tokens[i] != ";":
            valid = 0


    # all aggregations will be done in the reducer
    outputString = genOpString(columnsInQuery)
    whereBlock = genWhereBlock(whereClausesMapper, conjunctions, table, tables, '\t\t')

    imports = "#!/usr/bin/python3\nimport csv\nimport sys\n\n"

    processAndPrint = "for line in sys.stdin:\n"
    processAndPrint += "\tvalues1 = line.lower().split(',')\n"
    processAndPrint += "\tvalues = [x.strip() for x in values1]\n"
    processAndPrint += "\ttry:\n"
    processAndPrint += whereBlock
    processAndPrint += "\t\tprint(line)\n"
    processAndPrint += "\texcept:\n"
    processAndPrint += "\t\tpass\n"
    mapper = imports + processAndPrint


    globalVars = genGlobalVars(aggregationsInQuery) + '\n'
    updateStatements = updateAggrs(aggregationsInQuery, table, tables, "\t\t\t")
    globalVarString = printGlobalVars(aggregationsInQuery)

    process = "for line in sys.stdin:\n"
    process += "\ttry:\n"
    process += "\t\tif (len(line.strip()) > 0):\n"
    process += "\t\t\tvalues1 = line.split(',')\n"
    process += "\t\t\tvalues = [x.strip() for x in values1]\n"
    process += updateStatements
    process += "\t\t\t" + outputString + "\n"
    process += "\texcept:\n"
    process += "\t\tpass\n"

    reducer = imports + globalVars + process + globalVarString

    if valid:
        mFile = open("./mapper_generated_"+ pid + ".py", "w")
        rFile = open("./reducer_generated_"+ pid + ".py", "w")
        mFile.write(mapper)
        rFile.write(reducer)
        mFile.close()
        rFile.close()

if __name__ == '__main__':
    q = input()
    generate(q, "0000")
