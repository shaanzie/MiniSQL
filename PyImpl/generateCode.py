import ast
import os


def getIndex(column, table, tables):
    """return the index of a column in a table given its name"""
    i = 0
    colNames = [x[0] for x in tables[table]]
    return colNames.index(column.strip())

def parseProjections(projections, table, tables):

    """
    parseProjections: function returns a tuple of lists.
    indices contains the columns that need to be output in the end
    aggregations is a list of lists where the first element is the aggregation
    and the second is the index of the column it is performed on

    projections: list of strings.
    each of the strings is either a column name
    or an aggregation in the format aggr(column_name)

    table: current table

    tables: the list of all tables that have been loaded at some point"""

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
    """return datatype of a column based on the schema, takes column name as input"""
    return tables[table][getIndex(col, table, tables)][1]

def getDataTypeFromIndex(ind, table, tables):
    """returns datatype of a column based on the column index and the schema loaded"""
    return tables[table][ind][1]

def parseClauses(whereClauses, table, tables):
    # assumption: where clauses only on data that is available before the query is run
    """ input : whereClauses - a list of strings containing all the where clauses
        table : current table
        tables : a dictionary of all tables that were loaded

        return a list of lists where the first element is the column index of the column used in comparison
        and the second element is the condition that needs to be checked. The second element is output as plain Python
        so its behaviour is the same as it would be in python

        Datatype checking to make sure we are not comparing strings with a non equality based operator
    """

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

    """ input is a list of indices
    generates the output string based on what projections are necessary based on their column indicies """
    s = ""
    if len(cols):
        s += "print("
        for col in cols:
            s += "values[" + str(col) + "], "
        s += "sep = ' ')\n"
    return s + '\n'

def genWhereBlock(clauses, conjunctions, table, tables, indentation):

    """ generates a conditional where block.
        clauses come from parseClauses, and contain a list of (col_index, condition)s
        conjunctions is a list of conjunctions (in this case "and" and "or"), in the order they were specified.
        No support for nesting, and will be rendered in plain python, so behaviour remains consistent with plain python
        indentation to make further nesting within other blocks easier

        outputs a string that is an if condition
        """
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

    """
        input is a list of lists [[aggr, col_index]]
        output will be all the global variables required for the aggregation
        variable naming to avoid clashes = aggregation + col_number
        However, there is no duplicates checking

        average needs count and sum, and is handled as a special case to avoid duplicate statements.
        """
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

    """
    input is a list of lists [[aggr, col_index]]
    output will be all update statements on the global variables
    using the same naming convention as above
    each aggregation is dealt with as a separate case and avg is taken as a part of sum and/or count

    Outputs a block of code based on the indentation level required that consist of the required update statements

    """
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

    """
    prints all the global variables toward the end.
    using the same naming convention as above, so we know the variables exist, and are updated before we reach this stage
    """

    s = ""
    for aggr in aggrs:
        if aggr[0] == "avg":
                s += "print(\"average: \", sumcol" + str(aggr[1]) + "/countcol" + str(aggr[1]) + ")\n"
        else:
            s += "print(\"" + aggr[0] + ": \", " + aggr[0] + "col" + str(aggr[1]) + ")\n"
    return s


def generate(query, pid):

    """ query based on which the code is generated
    pid to ensure uniqueness in a session (not across)"""

    #standardizing input
    query = query.lower()
    query = query.replace(';', ' ;').replace(",", ", ")
    tokens = query.split()

    """exhaustive set of aggregations handled"""
    aggregations = {"sum", "min", "max", "avg", "count"}

    columnsInQuery = []
    aggregationsInQuery = []

    whereClauses = []
    whereClausesMapper = set()

    #dummy
    whereClausesReducer = set()

    i = 0
    valid = 1

    # dealing with selects only
    if tokens[i] != "select":
        valid = 0

    i += 1
    projections = []


    # only allowed string comparisons are "==" and "!="
    # type casting is necessary for comparisons and updates

    # assuming the query has a valid structure
    while valid and tokens[i] != "from":
        projections.append(tokens[i].replace(",", ""))
        i += 1


    i += 1
    table = tokens[i]


    # read schema from the metastore
    tables = dict()
    with open('metastore.txt', 'r') as file:
        lines = file.readlines()
        for line in lines:
            if table in line:
                tables.update(ast.literal_eval(line))

    # tables = {'table1': [('1', 'int'), ('2', 'str')]}


    columnsInQuery, aggregationsInQuery = parseProjections(projections, table, tables)


    i += 1

    conjunctions = []

    # checking for a where clause. All clauses encountered will be processed by parseClauses
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
    # mapper only changes with the where clauses
    # sends the whole record to reducer, room for improvement here
    outputString = genOpString(columnsInQuery)
    whereBlock = genWhereBlock(whereClausesMapper, conjunctions, table, tables, '\t\t')

    # mapper: skeletal code with where clauses being the only variable factor here
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


    # reducer must handle projection and aggregations
    # projections are handled in the output string
    # aggregations are divided into initialization, update and print blocks

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

    # mostly for debugging
    q = input()
    generate(q, "0000")
