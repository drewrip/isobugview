#!/usr/bin/env python3

import datetime

from pglast import Node, parse_sql
from enum import Enum
import re
import argparse
import time


class KeyType(Enum):
    pri = 1
    uni = 2
    mul = 3


class DbColType(Enum):
    intlike = 1
    floatlike = 2
    stringlike = 3
    datetimelike = 5


class QueryType(Enum):
    notQuery = 1
    other = 2
    select = 3
    update = 4
    insert = 5
    delete = 6
    startTxn = 7
    endTxn = 8
    startTxnCond = 9  # set autocommit=0 starts a transaction if none is started
    endTxnCond = 10  # set autocommit=1 commits a transaction if there is one
    rollBack = 11


class DbSchema:
    def __init__(self):
        self.tables = {}
        self.colTypes = {}
        self.colDefaults = {}
        self.colKeys = {}

    def add(self, tableName, colName, colType, colDefault, colKey):
        colList = self.tables.get(tableName, [])
        colList.append(colName)
        self.tables[tableName] = colList
        key = (tableName, colName)
        if colKey == 'PRI':
            self.colKeys[key] = KeyType.pri
        elif colKey == 'UNI':
            self.colKeys[key] = KeyType.uni
        elif colKey == 'MUL':
            self.colKeys[key] = KeyType.mul
        if 'int' in colType:
            self.colTypes[key] = DbColType.intlike
        elif 'decimal' in colType or 'double' in colType:
            self.colTypes[key] = DbColType.floatlike
        elif 'datetime' in colType:
            self.colTypes[key] = DbColType.datetimelike
        else:
            self.colTypes[key] = DbColType.stringlike
        self.colDefaults[key] = colDefault


# Reads the db schema info csv (format is table_name,col_name,col_key)
def readDbSchemaFile(fName):
    dbSchema = DbSchema()
    with open(fName, 'r') as f:
        for line in f:
            # Remove newline at end of string
            line = line[:-1]
            csplit = line.split(',')
            dbSchema.add(csplit[0].lower(), csplit[1].lower(), csplit[2], csplit[3], csplit[4])
    return dbSchema


# Reads the raw logs and ensures that every line is a single
# log line (no comments, combines lines if formatted strangely)
def readRawLogs(fName):
    logs = []
    with open(fName, 'r') as f:
        currentLine = ''
        for line in f:
            # Skip comments
            if line[0] == '#':
                continue
            # Remove newline at end of string
            line = line[:-1]
            continuation = False
            # Check if the line is part of the previous log or a new log
            for i in range(len(line)):
                c = line[i]
                if c == ' ' or c == '\t':
                    continue
                try:
                    int(c)
                    logs.append(currentLine)
                    currentLine = line
                except ValueError:
                    currentLine += ' ' + line[i:]
                break
        logs.append(currentLine)
    # The first time currentLine is appended, it will be empty; remove that log
    logs = logs[1:]
    return logs


def getQueryType(stmt):
    splitList = stmt.split()
    if len(splitList) != 0:
        command = str(splitList[0]).upper()
        if command == 'SELECT':
            return QueryType.select
        elif command == 'INSERT':
            return QueryType.insert
        elif command == 'UPDATE':
            return QueryType.update
        elif command == 'DELETE':
            return QueryType.delete
        elif (command == 'BEGIN' or
              command == 'START'):
            return QueryType.startTxn
        elif command == 'COMMIT':
            return QueryType.endTxn
        elif  command == 'ROLLBACK':
            return QueryType.rollBack
        elif command == 'SET':
            txncommand = str(splitList[1]).upper()
            if txncommand == 'AUTOCOMMIT=0':
                return QueryType.startTxnCond
            elif txncommand == 'AUTOCOMMIT=1':
                return QueryType.endTxnCond
            else:
                return QueryType.other
        else:
            return QueryType.other
    return QueryType.notQuery


# We are only interested in reads, writes, and transaction statements
def filterUninterestingLogs(stmts):
    filteredStmts = []
    for stmt in stmts:
        # print(stmt)
        if (getQueryType(stmt) == QueryType.other or
                getQueryType(stmt) == QueryType.notQuery):
            continue
        if (getQueryType(stmt) == QueryType.select and
                "FROM" not in stmt and
                "from" not in stmt):
            # Simple form of keep alive statements
            continue
        filteredStmts.append(stmt)
        print(stmt)
    return filteredStmts

# Applications may have a log of empty transactions.
def filterEmptyTxns(stmts):
    filteredStmts = []
    tempList = []
    for stmt in stmts:
        if (getQueryType(stmt) == QueryType.startTxn or
            getQueryType(stmt) == QueryType.startTxnCond):
            tempList.append(stmt)
        elif (getQueryType(stmt) == QueryType.endTxn or
              getQueryType(stmt) == QueryType.endTxnCond):
            if len(tempList) > 0:
                tempList.pop()
            else:
               filteredStmts.append(stmt)
        else:
            filteredStmts = filteredStmts + tempList
            filteredStmts.append(stmt)
            tempList = []
    return filteredStmts

# split string s by splitChar not inside quotes
def splitUnquoted(s, splitChar):
    inQuotes = False
    splitIndices = []
    splitList = []
    # Find all splitChar not in quotes
    for idx in range(0, len(s)):
        c = s[idx]
        if c == "'":
            inQuotes = not inQuotes
        if c == splitChar and not inQuotes:
            splitIndices.append(idx)
    start = 0
    for idx in splitIndices:
        splitList.append(s[start:idx])
        start = idx + 1
    splitList.append(s[start:])
    return splitList


# db schema value type check to ensure the value(pglast will parse number values in quotes as string, not as int/float)
def _insertIntoWriteMapWithCast(self, writeMapping, col, val):
    pt = self.getPrimaryTable()
    inQuotes = False
    if ((val[0] == "'" and val[-1] == "'")
            or (val[0] == '"' and val[-1] == '"')):
        inQuotes = True
        val = val[1:-1]
    if self.dbSchema.colTypes[(pt, col)] == DbColType.stringlike:
        if not inQuotes and val != 'NULL' and val != '\n':
            val = None
    if self.dbSchema.colTypes[(pt, col)] == DbColType.intlike:
        try:
            val = int(val)
        except:
            val = None
    if self.dbSchema.colTypes[(pt, col)] == DbColType.floatlike:
        try:
            val = float(val)
        except:
            val = None
    writeMapping[col] = val


def getSqlStmts(logs):
    sqlStmts = []
    count = 0
    keywordList = ["query", "execute"]
    for logLine in logs:
        if not logLine:
            continue
        splitTuples = logLine.split()
        # print("{0}: {1}".format(count, splitTuples))
        count += 1
        i = 0
        while i < len(splitTuples):
            if splitTuples[i].lower() in keywordList:
                # print("Add stmt {0}".format(splitTuples[i+1:]))
                sqlStmts.append(" ".join(splitTuples[i + 1:]))
            i += 1
    return sqlStmts


"""
def getSqlStmts(logs):
    sqlStmts = []
    count = 0
    for logLine in logs:
        tabsplit = logLine.split('\t')
        print(tabsplit)
        count += 1
        if (tabsplit[0] == '' and tabsplit[1] == ''):
            # This happened at the same time as the previous timestamp
            threadAndCommand = tabsplit[2]
            commArg = ' '.join(tabsplit[3:])
            print("1")
        # original: (tabsplit[0][0] == '1'), tpcc & new apps: (tabsplit[0][0] == '2')
        elif (tabsplit[0][0] == '2'):
            # There is a new timestamp for TPC-C and new applications except ACIDrain mentioned
            threadAndCommand = tabsplit[1]
            commArg = ' '.join(tabsplit[2:])
            print("2")
        # original:
        elif (tabsplit[0][0] == '1'):
            threadAndCommand = tabsplit[1]
            commArg = ' '.join(tabsplit[2:])
            print(tabsplit[1])
            print("3")
        else:
            # These should be the only two cases, something strange is going on
            print('\n'.join(tabsplit))
            raise Exception
        threadAndCommSplit = threadAndCommand.split(' ')
        command = threadAndCommSplit[-1]
        print ("command is {0}".format(command))
        if command == 'Query' or command == 'Execute':
            sqlStmts.append(commArg)
    return sqlStmts
"""

def generateTxnList(sqlStmts, application_name):
    tempTxnList = []
    txnList = []
    count = 0
    beginString = 'Start Transaction'
    endString = 'Commit'
    for l in sqlStmts:
        queryType = getQueryType(l)
        # print ("Statement {0} has type {1}".format(l, queryType))
        if (queryType == QueryType.endTxn or
                queryType == QueryType.endTxnCond):
            if len(tempTxnList) > 0:
                tempTxnList.append(endString)
                txnList.extend(tempTxnList[:])
                tempTxnList.clear()
                if application_name == 'tpcc':
                    # for tpcc/tatp traces: no begin stmts
                    tempTxnList.append(beginString)
            else:
                if application_name == 'tpcc':
                    # for tpcc/tatp traces: no begin stmts
                    tempTxnList.append(beginString)
                else:
                    continue
        elif (queryType == QueryType.startTxn or
              queryType == QueryType.startTxnCond):
            if len(tempTxnList) > 0:
                tempTxnList.clear()
                tempTxnList.append(beginString)
            else:
                tempTxnList.append(beginString)
        elif queryType == QueryType.rollBack:
            if len(tempTxnList) > 0:
                tempTxnList.clear()
                if application_name == 'tpcc':
                    # for tpcc/tatp traces: no begin stmts
                    tempTxnList.append(beginString)
            else:
                if application_name == 'tpcc':
                    # for tpcc/tatp traces: no begin stmts
                    tempTxnList.append(beginString)
                else:
                    continue
        else:
            count += 1
            sqlCommand = l
            if len(tempTxnList) > 0:
                tempTxnList.append(sqlCommand)
            else:
                tempTxnList.append(beginString)
                tempTxnList.append(sqlCommand)
                tempTxnList.append(endString)
                txnList.extend(tempTxnList[:])
                tempTxnList.clear()

    #print(count)
    return txnList


# input: column fields list, selectTbl, aliasDict, schema
# return: (table, column)
def columnRefParse(colFields: [], aliasDict: {}, selectTbl: [], schema: DbSchema):
    tableName = ''
    # column ref only has column name
    if len(colFields) == 1:
        colName = colFields[0].get('String').get('str')
        for table in selectTbl:
            # print(table)
            if table.startswith('?'):
                continue
            colList = schema.tables[table]
            if colName in colList:
                if tableName == '':
                    tableName = table
                else:
                    tableName = selectTbl[0]
                    break
                    # raise ValueError("Multiple select tables have same column name %r" % colName)
        if tableName == '':
            # cannot find the main table, because there is a subquery in fromClause.
            tbl_col = ()
            # raise ValueError("Cannot find this column in the schema table: %r" % colName)
        else:
            tbl_col = (tableName, colName)
    # column ref has table and column name
    else:
        tblOrAlias = colFields[0].get('String').get('str')
        colName = colFields[1].get('String').get('str')
        tableName = aliasDict.get(tblOrAlias) if aliasDict.get(tblOrAlias) is not None else tblOrAlias
        tbl_col = (tableName, colName)
    return tbl_col

# input: a qual expression dict:  {A_Expr: {}}, selectTbl, aliasDict
# return: dict: {(table, column): ('Type_#', val) or [('Type_#', val)]}
# used to get an element in form of {(left tuple): [(right tuple)]} in an equality or inequality expression
def qualExprParse(aexprDict: {}, aliasDict: {}, selectTbl: [], schema: DbSchema, passAnchor: int, opsList: []):
    mapDict = {}
    operator = aexprDict.get('A_Expr').get('name')[0].get('String').get('str')
    # case 1: left expr is a const: left and right reversed
    if list(aexprDict.get('A_Expr').get('lexpr').keys())[0] == 'A_Const':
        value = list(list(aexprDict.get('A_Expr').get('lexpr').get('A_Const').get('val').values())[0].values())[
            0]
        right = ('Type_1', value)
        if isinstance(aexprDict.get('A_Expr').get('rexpr'), list):
            print("Expression kind value: %r" % aexprDict.get('A_Expr').get('kind'))
            if aexprDict.get('A_Expr').get('kind') == 11:
                # case: A_Const between col1 and col2
                for colDict in aexprDict.get('A_Expr').get('rexpr'):
                    colField = colDict.get('ColumnRef').get('fields')
                    left = columnRefParse(colField, aliasDict, selectTbl, schema)
                    newright = ('Type_3', '?')
                    mapDict[left] = mapDict.get(left, [])
                    if isinstance(mapDict[left], list):
                        mapDict[left].append(newright)
                    else:
                        print('This %r column has a value or a equality in foregoing conditions' % str(left))
            else:
                ValueError("Unexpected A_Const expression right list arg kind: %r" % aexprDict.get('A_Expr').get('kind'))
        else:
            rightType = list(aexprDict.get('A_Expr').get('rexpr').keys())[0]
            if rightType == 'ColumnRef':
                leftField = aexprDict.get('A_Expr').get('rexpr').get('ColumnRef').get('fields')
                left = columnRefParse(leftField, aliasDict, selectTbl, schema)
                if operator == '=':
                    mapDict[left] = right
                else:
                    newRight = (right[0], str(right[1]) + ' ' + operator)
                    mapDict[left] = mapDict.get(left, [])
                    if isinstance(mapDict[left], list):
                        mapDict[left].append(newRight)
                    else:
                        print('This %r column has a value or an equality in foregoing conditions' % str(left))
            elif rightType == 'A_Const':
                print('No column refs in this expression(A_Const =/</> A_Const).\n')
            elif rightType == 'FuncCall':
                rightFunctionDict = aexprDict.get('A_Expr').get('rexpr')
                funcTupleList = funcParse(rightFunctionDict, aliasDict, selectTbl, schema, passAnchor, opsList)
                newright = ('Type_3', '?')
                for tu in funcTupleList:
                    mapDict[tu] = mapDict.get(tu, [])
                    if isinstance(mapDict[tu], list):
                        mapDict[tu].append(newright)
                    else:
                        print('This %r column has a value or a equality in foregoing expressions' % str(tu))
            elif rightType == 'SubLink':
                passAnchor += 1
                subselectTbl = []
                subselectTbl.append(selectTbl[0])
                subaliasDict = aliasDict.copy()
                exprSubselectStmtDict = aexprDict.get('A_Expr').get('rexpr').get('SubLink').get('subselect')
                exprSubselectList = selectParse(exprSubselectStmtDict, subaliasDict, subselectTbl, passAnchor, schema)
                opsList.extend(exprSubselectList)
            elif rightType == 'A_Expr':
                right = ('Type_3', '?')
                leftExpr = aexprDict.get('A_Expr').get('rexpr').get('A_Expr').get('lexpr')
                rightExpr = aexprDict.get('A_Expr').get('rexpr').get('A_Expr').get('rexpr')
                lExprTuList = lrExprParse(leftExpr, aliasDict, selectTbl, schema, passAnchor, opsList)
                rExprTuList = lrExprParse(rightExpr, aliasDict, selectTbl, schema, passAnchor, opsList)
                for tu in rExprTuList:
                    if tu not in lExprTuList:
                        lExprTuList.append(tu)
                for tbl_col in lExprTuList:
                    mapDict[tbl_col] = mapDict.get(tbl_col, [])
                    if isinstance(mapDict[tbl_col], list):
                        mapDict[tbl_col].append(right)
                    else:
                        print('This %r column has a value or a equality in foregoing expressions' % str(tbl_col))
            else:
                raise ValueError("Unexpected A_Const expression right arg type: %r" % rightType)
    # case 2: left expr is a column ref
    elif list(aexprDict.get('A_Expr').get('lexpr').keys())[0] == 'ColumnRef':
        lexpr = aexprDict.get('A_Expr').get('lexpr').get('ColumnRef').get('fields')
        left = columnRefParse(lexpr, aliasDict, selectTbl, schema)
        if isinstance(aexprDict.get('A_Expr').get('rexpr'), list):
            if aexprDict.get('A_Expr').get('kind') == 7:
                # expression contains an IN clause
                rexprType = 'IN_List'
            elif aexprDict.get('A_Expr').get('kind') == 11:
                # expression contains a BETWEEN...AND... clause
                rexprType = 'BETWEEN_List'
            else:
                raise ValueError("Unexpected expression operator %r and kind %r" % operator, aexprDict.get('A_Expr').get('kind'))
        else:
            rexprType = next(iter(aexprDict.get('A_Expr').get('rexpr')))
        if rexprType == 'IN_List':
            if len(aexprDict.get('A_Expr').get('rexpr')) == 1:
                value = list(
                    list(aexprDict.get('A_Expr').get('rexpr')[0].get('A_Const').get('val').values())[0].values())[
                    0]
                right = ('Type_1', value)
            else:
                # multiple values in IN clause
                right = ('Type_3', '?')
        elif rexprType == 'BETWEEN_List':
            right = ('Type_3', '?')
        elif rexprType == 'ColumnRef':
            rexprFields = aexprDict.get('A_Expr').get('rexpr').get('ColumnRef').get('fields')
            tbl_col = columnRefParse(rexprFields, aliasDict, selectTbl, schema)
            right = ('Type_4', tbl_col)
        elif rexprType == 'A_Const':
            value = \
                list(list(aexprDict.get('A_Expr').get('rexpr').get('A_Const').get('val').values())[0].values())[
                    0]
            right = ('Type_1', value)
        elif rexprType == 'SubLink':
            # qual expression right hand (columnref = (select ....)) is a subquery
            passAnchor += 1
            right = ('Type_2', passAnchor)
            subselectTbl = []
            subselectTbl.append(selectTbl[0])
            subaliasDict = aliasDict.copy()
            exprSubselectStmtDict = aexprDict.get('A_Expr').get('rexpr').get('SubLink').get('subselect')
            exprSubselectList = selectParse(exprSubselectStmtDict, subaliasDict, subselectTbl, passAnchor, schema)
            opsList.extend(exprSubselectList)
        elif rexprType == 'A_Expr':
            leftExpr = aexprDict.get('A_Expr').get('rexpr').get('A_Expr').get('lexpr')
            nestOperator = aexprDict.get('A_Expr').get('rexpr').get('A_Expr').get('name')[0].get('String').get('str')
            rightExpr = aexprDict.get('A_Expr').get('rexpr').get('A_Expr').get('rexpr')
            if next(iter(leftExpr)) == 'ColumnRef':
                leftOp = leftExpr.get('ColumnRef').get('fields')[0].get('String').get('str')
            else:
                leftOp = list(list(leftExpr.get('A_Const').get('val').values())[0].values())[0]
            if next(iter(rightExpr)) == 'ColumnRef':
                rightOp = rightExpr.get('ColumnRef').get('fields')[0].get('String').get('str')
            else:
                rightOp = list(list(rightExpr.get('A_Const').get('val').values())[0].values())[0]
            expr = '(' + str(leftOp) + nestOperator + str(rightOp) + ')'
            right = ('Type_1', expr)
        elif rexprType == 'FuncCall':
            right = ('Type_3', '?')
            rightFunctionDict = aexprDict.get('A_Expr').get('rexpr')
            funcTupleList = funcParse(rightFunctionDict, aliasDict, selectTbl, schema, passAnchor, opsList)
            newright = ('Type_3', '?')
            for tu in funcTupleList:
                mapDict[tu] = mapDict.get(tu, [])
                if isinstance(mapDict[tu], list):
                    mapDict[tu].append(newright)
                else:
                    print('This %r column has a value or a equality in foregoing expressions' % str(tu))
        else:
            raise ValueError("Unexpected type of expression right expr: %r" % rexprType)
        if operator == '=':
            mapDict[left] = right
        else:
            # original:
            # newRight = (right[0], operator + ' ' + str(right[1]))
            newRight = (right[0], right[1])
            mapDict[left] = mapDict.get(left, [])
            if isinstance(mapDict[left], list):
                mapDict[left].append(newRight)
            else:
                print(
                    'This %r column has a value or an equality in foregoing expressions' % str(
                        left))
    # case 3: left expr is a function call
    elif list(aexprDict.get('A_Expr').get('lexpr').keys())[0] == 'FuncCall':
        funcName = list(aexprDict.get('A_Expr').get('lexpr').get('FuncCall').get('funcname'))[0].get('String').get('str')
        leftFunctionDict = aexprDict.get('A_Expr').get('lexpr')
        funcTupleList = funcParse(leftFunctionDict, aliasDict, selectTbl, schema, passAnchor, opsList)
        newright = ('Type_3', '?')
        for tu in funcTupleList:
            mapDict[tu] = mapDict.get(tu, [])
            if isinstance(mapDict[tu], list):
                mapDict[tu].append(newright)
            else:
                print('This %r column has a value or a equality in foregoing expressions' % str(tu))
        rightExpr = aexprDict.get('A_Expr').get('rexpr')
        rexprType = list(rightExpr.keys())[0]
        if rexprType == 'ColumnRef':
            rexprFields = rightExpr.get('ColumnRef').get('fields')
            left = columnRefParse(rexprFields, aliasDict, selectTbl, schema)
            right = ('Type_3', '?')
            if operator == '=':
                mapDict[left] = right
            else:
                newRight = (right[0], operator + ' ' + str(right[1]))
                mapDict[left] = mapDict.get(left, [])
                if isinstance(mapDict[left], list):
                    mapDict[left].append(newRight)
                else:
                    print('This %r column has a value or an equality in foregoing expressions' % str(left))
        elif rexprType == 'A_Const':
            print('This A_Const has no column ref in the qual expression.')
        elif rexprType == 'SubLink':
            passAnchor += 1
            subselectTbl = []
            subselectTbl.append(selectTbl[0])
            subaliasDict = aliasDict.copy()
            exprSubselectStmtDict = aexprDict.get('A_Expr').get('rexpr').get('SubLink').get('subselect')
            exprSubselectList = selectParse(exprSubselectStmtDict, subaliasDict, subselectTbl, passAnchor, schema)
            opsList.extend(exprSubselectList)
        elif rexprType == 'A_Expr':
            exprRight = ('Type_3', '?')
            lexpr = aexprDict.get('A_Expr').get('rexpr').get('A_Expr').get('lexpr')
            rexpr = aexprDict.get('A_Expr').get('rexpr').get('A_Expr').get('rexpr')
            lExprTuList = lrExprParse(lexpr, aliasDict, selectTbl, schema, passAnchor, opsList)
            rExprTuList = lrExprParse(rexpr, aliasDict, selectTbl, schema, passAnchor, opsList)
            for tu in rExprTuList:
                if tu not in lExprTuList:
                    lExprTuList.append(tu)
            for tu in lExprTuList:
                mapDict[tu] = mapDict.get(tu, [])
                if isinstance(mapDict[tu], list):
                    mapDict[tu].append(exprRight)
                else:
                    print('This %r column has a value or a equality in foregoing expressions' % str(tu))
        elif rexprType == 'FuncCall':
            funRight = ('Type_3', '?')
            rightFunctionDict = aexprDict.get('A_Expr').get('rexpr')
            funcTupleList = funcParse(rightFunctionDict, aliasDict, selectTbl, schema, passAnchor, opsList)
            for tu in funcTupleList:
                mapDict[tu] = mapDict.get(tu, [])
                if isinstance(mapDict[tu], list):
                    mapDict[tu].append(funRight)
                else:
                    print('This %r column has a value or a equality in foregoing expressions' % str(tu))
    # case 4: left expr is a subquery
    elif list(aexprDict.get('A_Expr').get('lexpr').keys())[0] == 'SubLink':
        passAnchor += 1
        right = ('Type_2', passAnchor)
        subselectTbl = []
        subselectTbl.append(selectTbl[0])
        subaliasDict = aliasDict.copy()
        leftSubselectStmtDict = aexprDict.get('A_Expr').get('lexpr').get('SubLink').get('subselect')
        leftSubselectList = selectParse(leftSubselectStmtDict, subaliasDict, subselectTbl, passAnchor, schema)
        opsList.extend(leftSubselectList)
        if isinstance(aexprDict.get('A_Expr').get('rexpr'), list):
            if aexprDict.get('A_Expr').get('kind') == 7:
                # where contains an IN clause
                rightType = 'IN_List'
            elif aexprDict.get('A_Expr').get('kind') == 11:
                # where contains a BETWEEN...AND... clause
                rightType = 'BETWEEN_List'
            else:
                raise ValueError("Unexpected where operator %r and kind %r" % operator, aexprDict.get('A_Expr').get('kind'))
        else:
            rightType = list(aexprDict.get('A_Expr').get('rexpr').keys())[0]
        if rightType == 'ColumnRef':
            leftField = aexprDict.get('A_Expr').get('rexpr').get('ColumnRef').get('fields')
            left = columnRefParse(leftField, aliasDict, selectTbl, schema)
            if operator == '=':
                mapDict[left] = right
            else:
                newRight = (right[0], operator + ' ' + str(right[1]))
                mapDict[left] = mapDict.get(left, [])
                if isinstance(mapDict[left], list):
                    mapDict[left].append(newRight)
                else:
                    print('This %r column has a value or a equality in foregoing conditions' % str(left))
        elif rightType == 'A_Const' or rightType == 'IN_List' or rightType == 'BETWEEN_List':
            print("Subquery in expression has no column ref to right: %r." % rightType)
        else:
            raise ValueError("Unexpected subquery expression arg right type %r" % rightType)
    # case 5: left expr is another expression
    elif list(aexprDict.get('A_Expr').get('lexpr').keys())[0] == 'A_Expr':
        lexpr = aexprDict.get('A_Expr').get('lexpr')
        leftTuList = lrExprParse(lexpr, aliasDict, selectTbl, schema, passAnchor, opsList)
        exprRight = ('Type_3', '?')
        for tu in leftTuList:
            mapDict[tu] = mapDict.get(tu, [])
            if isinstance(mapDict[tu], list):
                mapDict[tu].append(exprRight)
            else:
                print('This %r column has a value or a equality in foregoing expressions' % str(tu))
        rexpr = aexprDict.get('A_Expr').get('rexpr')
        rexprType = list(rexpr.keys())[0]
        if rexprType == 'ColumnRef':
            rexprFields = rexpr.get('ColumnRef').get('fields')
            left = columnRefParse(rexprFields, aliasDict, selectTbl, schema)
            right = ('Type_3', '?')
            if operator == '=':
                mapDict[left] = right
            else:
                newRight = (right[0], operator + ' ' + str(right[1]))
                mapDict[left] = mapDict.get(left, [])
                if isinstance(mapDict[left], list):
                    mapDict[left].append(newRight)
                else:
                    print('This %r column has a value or an equality in foregoing expressions' % str(left))
        elif rexprType == 'A_Const':
            print('This A_Const has no column ref in the qual expression.')
        elif rexprType == 'SubLink':
            passAnchor += 1
            subselectTbl = []
            subselectTbl.append(selectTbl[0])
            subaliasDict = aliasDict.copy()
            exprSubselectStmtDict = aexprDict.get('A_Expr').get('rexpr').get('SubLink').get('subselect')
            exprSubselectList = selectParse(exprSubselectStmtDict, subaliasDict, subselectTbl, passAnchor, schema)
            opsList.extend(exprSubselectList)
        elif rexprType == 'A_Expr':
            exprRight = ('Type_3', '?')
            lexpr = aexprDict.get('A_Expr').get('rexpr').get('A_Expr').get('lexpr')
            rexpr = aexprDict.get('A_Expr').get('rexpr').get('A_Expr').get('rexpr')
            lExprTuList = lrExprParse(lexpr, aliasDict, selectTbl, schema, passAnchor, opsList)
            rExprTuList = lrExprParse(rexpr, aliasDict, selectTbl, schema, passAnchor, opsList)
            for tu in rExprTuList:
                if tu not in lExprTuList:
                    lExprTuList.append(tu)
            for tu in lExprTuList:
                mapDict[tu] = mapDict.get(tu, [])
                if isinstance(mapDict[tu], list):
                    mapDict[tu].append(exprRight)
                else:
                    print('This %r column has a value or a equality in foregoing expressions' % str(tu))
        elif rexprType == 'FuncCall':
            funRight = ('Type_3', '?')
            rightFunctionDict = aexprDict.get('A_Expr').get('rexpr')
            funcTupleList = funcParse(rightFunctionDict, aliasDict, selectTbl, schema, passAnchor, opsList)
            for tu in funcTupleList:
                mapDict[tu] = mapDict.get(tu, [])
                if isinstance(mapDict[tu], list):
                    mapDict[tu].append(funRight)
                else:
                    print('This %r column has a value or a equality in foregoing expressions' % str(tu))
    else:
        raise ValueError(
            "Unexpected type of expression left expr %r" % list(aexprDict.get('A_Expr').get('lexpr').keys())[0])
    return mapDict

# input: an expression dict:  {exprType: {}}, selectTbl, aliasDict
# return: (table, column) tuple list
# used to get only columnref in a sub expression
def lrExprParse(exprDict: {}, aliasDict: {}, selectTbl: [], schema: DbSchema, passAnchor: int, opsList: []):
    tupleList = []
    if isinstance(exprDict, list):
        print('This list has no column ref.')
        return tupleList
    # print(exprDict)
    eType = list(exprDict.keys())[0]
    if eType == 'ColumnRef':
        eFields = exprDict.get('ColumnRef').get('fields')
        tbl_col = columnRefParse(eFields, aliasDict, selectTbl, schema)
        tupleList.append(tbl_col)
    elif eType == 'A_Const':
        print('This A_Const has no column ref.')
        # val = list(list(exprDict.get('A_Const').get('val').values())[0].values())[0]
        # tbl_col = ('NULL', val)
        # tupleList.append(tbl_col)
    elif eType == 'FuncCall':
        funcDict = exprDict
        funcTupleList = funcParse(funcDict, aliasDict, selectTbl, schema, passAnchor, opsList)
        for tu in funcTupleList:
            if tu not in tupleList:
                tupleList.append(tu)
    elif eType == 'A_Expr':
        lexpr = exprDict.get('A_Expr').get('lexpr')
        rexpr = exprDict.get('A_Expr').get('rexpr')
        lExprTuList = lrExprParse(lexpr, aliasDict, selectTbl, schema, passAnchor, opsList)
        rExprTuList = lrExprParse(rexpr, aliasDict, selectTbl, schema, passAnchor, opsList)
        for tu in lExprTuList:
            if tu not in tupleList:
                tupleList.append(tu)
        for tu in rExprTuList:
            if tu not in tupleList:
                tupleList.append(tu)
    elif eType == 'SubLink':
        passAnchor += 1
        subquery = exprDict.get('SubLink').get('subselect')
        subselectTbl = selectTbl[:]
        subaliasDict = aliasDict.copy()
        subselectReadList = selectParse(subquery, subaliasDict, subselectTbl, passAnchor, schema)
        opsList.extend(subselectReadList)
    elif eType == 'TypeCast':
        castArg = exprDict.get('TypeCast').get('arg')
        if list(castArg.keys())[0] == 'FuncCall':
            nestFuncDict = castArg
            nestTupleList = funcParse(nestFuncDict, aliasDict, selectTbl, schema, passAnchor, opsList)
            for tu in nestTupleList:
                if tu not in tupleList:
                    tupleList.append(tu)
        elif list(castArg.keys())[0] == 'ColumnRef':
            castFields = castArg.get('ColumnRef').get('fields')
            tbl_col = columnRefParse(castFields, aliasDict, selectTbl, schema)
            tupleList.append(tbl_col)
        elif list(castArg.keys())[0] == 'A_Const':
            print('This Typecast has no column ref.')
        elif list(castArg.keys())[0] == 'SubLink':
            passAnchor += 1
            typeCastSubquery = castArg.get('SubLink').get('subselect')
            subselectTbl = selectTbl[:]
            subaliasDict = aliasDict.copy()
            subselectReadList = selectParse(typeCastSubquery, subaliasDict, subselectTbl, passAnchor, schema)
            opsList.extend(subselectReadList)
        else:
            raise ValueError("Unexpected arg type for Typecast function: %r" % list(castArg.keys())[0])
    else:
        raise ValueError("Unexpected lrexpr type: %r" % eType)
    return tupleList

# input: {'FuncCall': {}}
# return: a tuple list
# problem: what if the columns in function will not be read simultaneously?
def funcParse(funcDict: {}, aliasDict: {}, selectTbl: [], schema: DbSchema, passAnchor: int, opsList: []):
    tupleList = []  # a list of tuple elements
    funcName = funcDict.get('FuncCall').get('funcname')[0].get('String').get('str')
    if funcDict.get('FuncCall').get('args') is None:
        print("No arg in function: %r" % funcName)
        return []
    else:
        funcArgsList = funcDict.get('FuncCall').get('args')  # a list
        for arg in funcArgsList:
            argType = list(arg.keys())[0]
            if argType == 'TypeCast':
                castArg = arg.get('TypeCast').get('arg')
                if list(castArg.keys())[0] == 'FuncCall':
                    nestFuncDict = castArg
                    nestTupleList = funcParse(nestFuncDict, aliasDict, selectTbl, schema, passAnchor, opsList)
                    for tu in nestTupleList:
                        if tu not in tupleList:
                            tupleList.append(tu)
                elif list(castArg.keys())[0] == 'ColumnRef':
                    castFields = castArg.get('ColumnRef').get('fields')
                    tbl_col = columnRefParse(castFields, aliasDict, selectTbl, schema)
                    if tbl_col not in tupleList:
                        tupleList.append(tbl_col)
                elif list(castArg.keys())[0] == 'A_Const':
                    print('This Typecast has no column ref.')
                elif list(castArg.keys())[0] == 'SubLink':
                    passAnchor += 1
                    typeCastSubquery = castArg.get('SubLink').get('subselect')
                    subselectTbl = selectTbl[:]
                    subaliasDict = aliasDict.copy()
                    subselectReadList = selectParse(typeCastSubquery, subaliasDict, subselectTbl, passAnchor, schema)
                    opsList.extend(subselectReadList)
                else:
                    raise ValueError("Unexpected arg type for Typecast function: %r" % list(castArg.keys())[0])
            elif argType == 'ColumnRef':
                colFields = arg.get('ColumnRef').get('fields')
                tbl_col = columnRefParse(colFields, aliasDict, selectTbl, schema)
                if tbl_col not in tupleList and tbl_col != ():
                    tupleList.append(tbl_col)
            elif argType == 'A_Const':
                continue
            elif argType == 'NullTest':
                nullFields = arg.get('NullTest').get('arg').get('ColumnRef').get('fields')
                tbl_col = columnRefParse(nullFields, aliasDict, selectTbl, schema)
                if tbl_col not in tupleList:
                    tupleList.append(tbl_col)
            elif argType == 'FuncCall':
                nestFuncDict = arg
                nestTupleList = funcParse(nestFuncDict, aliasDict, selectTbl, schema, passAnchor, opsList)
                for tu in nestTupleList:
                    if tu not in tupleList:
                        tupleList.append(tu)
            elif argType == 'SubLink':
                passAnchor += 1
                funSubquery = arg.get('SubLink').get('subselect')
                subselectTbl = selectTbl[:]
                subaliasDict = aliasDict.copy()
                subselectReadList = selectParse(funSubquery, subaliasDict, subselectTbl, passAnchor, schema)
                opsList.extend(subselectReadList)
            elif argType == 'A_Expr':
                lexpr = arg.get('A_Expr').get('lexpr')
                rexpr = arg.get('A_Expr').get('rexpr')
                lExprTuList = lrExprParse(lexpr, aliasDict, selectTbl, schema, passAnchor, opsList)
                rExprTuList = lrExprParse(rexpr, aliasDict, selectTbl, schema, passAnchor, opsList)
                for tu in lExprTuList:
                    if tu not in tupleList:
                        tupleList.append(tu)
                for tu in rExprTuList:
                    if tu not in tupleList:
                        tupleList.append(tu)
            else:
                raise ValueError("Unexpected arg type for function %r : %r" % funcName, argType)

    return tupleList

# input: {'JoinExpr': {larg: {RangeVar}, rarg: {RangeVar}, quals}}
# def joinParse(joinExpr: {}, aliasDict: {}, selectTbl: [], passAnchor: int, schema: DbSchema):
#     leftJoinOnList = []
#     # get the left join table
#     # outerDict = joinExpr.get('JoinExpr')
#     larg = joinExpr.get('JoinExpr').get('larg')
#     leftTbl = larg.get('RangeVar').get('relname')
#     # left table as the first element in selectTbl ("from leftTbl")
#     selectTbl.append(leftTbl)
#     if larg.get('RangeVar').get('alias') is not None:
#         aliasName = larg.get('RangeVar').get('alias').get('Alias').get('aliasname')
#         aliasDict[aliasName] = leftTbl
#     # get the right join table
#     rarg = joinExpr.get('JoinExpr').get('rarg')
#     rightTbl = rarg.get('RangeVar').get('relname')
#     # add join table to select table lists
#     selectTbl.append(rightTbl)
#     if rarg.get('RangeVar').get('alias') is not None:
#         aliasName = rarg.get('RangeVar').get('alias').get('Alias').get('aliasname')
#         aliasDict[aliasName] = rightTbl
#     # get the on condition (first join on clause)
#     onclause = joinExpr.get('JoinExpr').get('quals')
#     if onclause is None:
#         print("This is a join w/o on clause: Join Type %r" % joinExpr.get('JoinExpr').get('jointype'))
#     elif next(iter(onclause)) == 'A_Expr':
#         operator = onclause.get('A_Expr').get('name')[0].get('String').get('str')
#         # case 1: left expr is a const: left and right reversed
#         if list(onclause.get('A_Expr').get('lexpr').keys())[0] == 'A_Const':
#             value = list(list(onclause.get('A_Expr').get('lexpr').get('A_Const').get('val').values())[0].values())[
#                 0]
#             right = ('Type_1', value)
#             # right expr should be a column ref
#             rexpr = onclause.get('A_Expr').get('rexpr').get('ColumnRef').get('fields')
#             left = columnRefParse(rexpr, aliasDict, selectTbl, schema)
#         # case 2: left expr is a column ref
#         elif list(onclause.get('A_Expr').get('lexpr').keys())[0] == 'ColumnRef':
#             lexpr = onclause.get('A_Expr').get('lexpr').get('ColumnRef').get('fields')
#             left = columnRefParse(lexpr, aliasDict, selectTbl, schema)
#             rexprType = next(iter(onclause.get('A_Expr').get('rexpr')))
#             if rexprType == 'ColumnRef':
#                 rexpr = onclause.get('A_Expr').get('rexpr').get('ColumnRef').get('fields')
#                 right = columnRefParse(rexpr, aliasDict, selectTbl, schema)
#             elif rexprType == 'A_Const':
#                 value = \
#                     list(list(onclause.get('A_Expr').get('rexpr').get('A_Const').get('val').values())[0].values())[
#                         0]
#                 right = ('Type_1', value)
#             else:
#                 raise ValueError("Unexpected type of on clause right expr: %r" % rexprType)
#         else:
#             raise ValueError(
#                 "Unexpected type of on clause left expr %r" % list(onclause.get('A_Expr').get('lexpr').keys())[0])
#     elif next(iter(onclause)) == 'BoolExpr':
#         # multiple on conditions
#         argsList = onclause.get('BoolExpr').get('args')
#         for arg in argsList:
#             argType = list(arg.keys())[0]
#             if argType == 'A_Expr':
#                 operator = arg.get('A_Expr').get('name')[0].get('String').get('str')

# input: onclause as {'A_Expr/BoolExpr/NullTest': {}}
# output: the read operation list in the onclause
def joinOnRecur(onclause: {}, aliasDict: {}, selectTbl: [], passAnchor: int,
                schema: DbSchema, opsList: []):
    anchor = passAnchor
    joinOnList = []
    if next(iter(onclause)) == 'A_Expr':
        mapDict = qualExprParse(onclause, aliasDict, selectTbl, schema,
                                passAnchor, opsList)
        # print(mapDict)
        for left, right in mapDict.items():
            if isinstance(right, list):
                for tu in right:
                    if tu[0] == 'Type_4':
                        right = tu[1]
                        newRight = ('Type_3', '?')
                        listelement = [
                            (passAnchor, 'JOIN', left[0], left[1], newRight),
                            (passAnchor, 'JOIN', right[0], right[1], newRight)]
                    elif tu[0] == 'Type_3' or 'Type_1':
                        listelement = [
                            (passAnchor, 'JOIN', left[0], left[1], tu)]
                    else:
                        raise ValueError(
                            "Error in join on parseing: %r" % tu[0])
                    joinOnList.extend(listelement)
            else:
                if right[0] == 'Type_4':
                    secondCol = right[1]
                    newRight = ('Type_3', '?')
                    listelement = [
                        (passAnchor, 'JOIN', left[0], left[1], newRight),
                        (passAnchor, 'JOIN', secondCol[0], secondCol[1],
                         newRight)]
                elif right[0] == 'Type_3' or 'Type_1':
                    listelement = [
                        (passAnchor, 'JOIN', left[0], left[1], right)]
                else:
                    raise ValueError("Error in join on parseing: %r" % right)
                joinOnList.extend(listelement)
    elif next(iter(onclause)) == 'BoolExpr':
        argsList = onclause.get('BoolExpr').get('args')
        for arg in argsList:
            nestedOnList = joinOnRecur(arg, aliasDict, selectTbl, passAnchor,
                                       schema, opsList)
            joinOnList.extend(nestedOnList[:])
            nestedOnList.clear()
    elif next(iter(onclause)) == 'NullTest':
        operator = 'NullTest'
        nullFields = onclause.get('NullTest').get('arg').get('ColumnRef').get(
            'fields')
        left = columnRefParse(nullFields, aliasDict, selectTbl, schema)
        right = ('Type_3', '?')
        # construct join read operations for NullTest
        element = [(passAnchor, 'JOIN', left[0], left[1], right)]
        joinOnList.extend(element)
    elif next(iter(onclause)) == 'FuncCall':
        # onclause only has a function: ISNULL/NOT ISNULL
        funcTupleList = funcParse(onclause, aliasDict, selectTbl, schema,
                                  passAnchor, opsList)
        right = ('Type_3', '?')
        for tu in funcTupleList:
            element = [(passAnchor, 'JOIN', tu[0], tu[1], right)]
            joinOnList.extend(element)
    else:
        raise ValueError("Unexpected on clause type %r" % next(iter(onclause)))
    return joinOnList


# input: {'JoinExpr': {}}
def joinRecur(fromClauseDict: {}, aliasDict: {}, selectTbl: [], passAnchor: int,
              schema: DbSchema, opsList: []):
    anchor = passAnchor
    joinOnList = []
    larg = fromClauseDict.get('JoinExpr').get('larg')
    if next(iter(larg)) == 'JoinExpr':
        # get nested(left one) join reads from left until the first join
        leftJoinsOnList = joinRecur(larg, aliasDict, selectTbl, passAnchor,
                                    schema, opsList)
        # get current join reads
        rightJoinOnList = []
        rarg = fromClauseDict.get('JoinExpr').get('rarg')
        rightTbl = rarg.get('RangeVar').get('relname')
        # add current join table to select table list
        selectTbl.append(rightTbl)

        if rarg.get('RangeVar').get('alias') is not None:
            rightAlias = rarg.get('RangeVar').get('alias').get('Alias').get(
                'aliasname')
            aliasDict[rightAlias] = rightTbl
        # get the on condition
        onclause = fromClauseDict.get('JoinExpr').get('quals')
        if onclause is None:
            print(
                "This is a join w/o on clause: Join Type %r" % fromClauseDict.get(
                    'JoinExpr').get('jointype'))
        else:
            joinOnList = joinOnRecur(onclause, aliasDict, selectTbl, passAnchor,
                                     schema, opsList)
            rightJoinOnList.extend(joinOnList)
        leftJoinsOnList.extend(rightJoinOnList)
        return leftJoinsOnList
    # do not yet change the table alias in element to true table name in following code
    # get the first join
    elif next(iter(larg)) == 'RangeVar':
        leftJoinOnList = []
        # get the left most table
        outerDict = fromClauseDict.get('JoinExpr')
        leftTbl = larg.get('RangeVar').get('relname')
        rightTbl = ''
        # left most table as the first element in selectTbl ("from leftTbl")
        selectTbl.append(leftTbl)
        if larg.get('RangeVar').get('alias') is not None:
            aliasName = larg.get('RangeVar').get('alias').get('Alias').get(
                'aliasname')
            aliasDict[aliasName] = leftTbl
        # get the table next to left most one (first join table)
        if list(outerDict.get('rarg').keys())[0] == 'RangeVar':
            # join table exists
            rightTbl = outerDict.get('rarg').get('RangeVar').get('relname')
            # add join table to select table lists
            selectTbl.append(rightTbl)
            if outerDict.get('rarg').get('RangeVar').get('alias') is not None:
                aliasName = outerDict.get('rarg').get('RangeVar').get(
                    'alias').get('Alias').get('aliasname')
                aliasDict[aliasName] = rightTbl
        elif list(outerDict.get('rarg').keys())[0] == 'RangeSubselect':
            # join a subquery
            joinSubSelectStmtDict = outerDict.get('rarg').get(
                'RangeSubselect').get('subquery')
            rightTbl = \
            outerDict.get('rarg').get('RangeSubselect').get('subquery').get(
                'SelectStmt').get('fromClause')[0].get('RangeVar').get(
                'relname')
            anchor += 1
            # subselectTbl = selectTbl[:]
            subaliasDict = aliasDict.copy()
            joinSubSelectReadList = selectParse(joinSubSelectStmtDict,
                                                subaliasDict, selectTbl, anchor,
                                                schema)
            opsList.extend(joinSubSelectReadList)
            if outerDict.get('rarg').get('RangeSubselect').get(
                    'alias') is not None:
                aliasName = outerDict.get('rarg').get('RangeSubselect').get(
                    'alias').get('Alias').get('aliasname')
                aliasDict[aliasName] = rightTbl
                # print(aliasDict)
        else:
            raise ValueError("Unexpected join table type %r" %
                             list(outerDict.get('rarg').keys())[0])
        # get the on condition (first join on clause)
        onclause = outerDict.get('quals')
        if onclause is None:
            print(
                "This is a join w/o on clause: Join Type %r" % fromClauseDict.get(
                    'JoinExpr').get('jointype'))
        else:
            joinOnList = joinOnRecur(onclause, aliasDict, selectTbl, passAnchor,
                                     schema, opsList)
            leftJoinOnList.extend(joinOnList)
        return leftJoinOnList
    else:
        print('Problem: ' + str(next(iter(larg))))
        raise ValueError("Problem: %r " % str(next(iter(larg))))

# deal with nested bool expression
# input: {'BoolExpr': {}}
def whereBoolExprParse(boolExpr: {}, aliasDict: {}, selectTbl: [], whereDict: {}, selectReadList: [], passAnchor: int, schema: DbSchema):
    anchor = passAnchor
    # multiple where conditions
    argsList = boolExpr.get('BoolExpr').get('args')
    for arg in argsList:
        if list(arg.keys())[0] == 'A_Expr':
            oneExpr = arg
            whereOneExprParse(oneExpr, aliasDict, selectTbl, whereDict, selectReadList, passAnchor, schema)
        # bool expression in bool expression
        elif list(arg.keys())[0] == 'BoolExpr':
            nestedBoolExpr = arg
            whereBoolExprParse(nestedBoolExpr, aliasDict, selectTbl, whereDict, selectReadList, passAnchor, schema)
        elif list(arg.keys())[0] == 'NullTest':
            # is null test
            nullFields = arg.get('NullTest').get('arg').get('ColumnRef').get('fields')
            left = columnRefParse(nullFields, aliasDict, selectTbl, schema)
            right = ('Type_3', '?')
            whereDict[left] = whereDict.get(left, [])
            if isinstance(whereDict[left], list):
                existRights = whereDict[left]
                if right not in existRights:
                    whereDict[left].append(right)
            else:
                print('Null test column has a value in foregoing where conditions')
        elif list(arg.keys())[0] == 'SubLink':
            # EXISTS/NOT EXISTS subqueries: cannot decide the value
            passAnchor += 1
            subselectTbl = []
            subselectTbl.append(selectTbl[0])
            subaliasDict = aliasDict.copy()

            whereSubselectStmtDict = arg.get('SubLink').get('subselect')
            whereSubselectList = selectParse(whereSubselectStmtDict, subaliasDict, subselectTbl, passAnchor, schema)
            selectReadList.extend(whereSubselectList)

            # construct a virtual column refer to the subquery: no where column if subquery is an exists/not exists
            tbl = '?' + str(passAnchor)
            col = tbl
            left = (tbl, col)
            right = ('Type_2', passAnchor)
            whereDict[left] = whereDict.get(left, [])
            if isinstance(whereDict[left], list):
                existRights = whereDict[left]
                if right not in existRights:
                    whereDict[left].append(right)
            else:
                print('Only column in where has a value in foregoing where conditions')
        elif list(arg.keys())[0] == 'ColumnRef':
            # Where only has a column ref, no value
            colFields = arg.get('ColumnRef').get('fields')
            left = columnRefParse(colFields, aliasDict, selectTbl, schema)
            right = ('Type_3', '?')
            whereDict[left] = whereDict.get(left, [])
            if isinstance(whereDict[left], list):
                existRights = whereDict[left]
                if right not in existRights:
                    whereDict[left].append(right)
            else:
                print('Only column in where has a value in foregoing where conditions')
        elif list(arg.keys())[0] == 'FuncCall':
            # Where only has a function: ISNULL
            funcTupleList = funcParse(arg, aliasDict, selectTbl, schema, passAnchor, selectReadList)
            right = ('Type_3', '?')
            for tu in funcTupleList:
                whereDict[tu] = whereDict.get(tu, [])
                if isinstance(whereDict[tu], list):
                    existRights = whereDict[tu]
                    if right not in existRights:
                        whereDict[tu].append(right)
                else:
                    print('Only column in where has a value in foregoing where conditions')
        else:
            raise ValueError("Unexpected where arg type %r" % list(arg.keys())[0])

# deal with one where expression
# input: {'A_Expr': {}}
def whereOneExprParse(oneExpr: {}, aliasDict: {}, selectTbl: [], whereDict: {}, selectReadList: [], passAnchor: int, schema: DbSchema):
    anchor = passAnchor
    qualExpr = oneExpr
    mapDict = qualExprParse(qualExpr, aliasDict, selectTbl, schema, passAnchor, selectReadList)
    for key, value in mapDict.items():
        if value[0] == 'Type_4':
            val = (value[0], value[1][0] + '.' + value[1][1])
        else:
            val = value
        whereDict[key] = val

# The select statement parser: SELECT columns FROM table1 JOIN table2 ON ... where ... [ UNION SELECT ...]
# input: {'SelectStmt': {} }
def selectParse(selectStmtDict: {}, aliasDict: {}, selectTbl: [], passAnchor: int, schema: DbSchema):
    selectReadList = []  # a list of list: [[(),()], [(),()], ...], nested select operation is one list in the outer list
    selectStmt = selectStmtDict.get('SelectStmt')
    anchor = passAnchor
    if selectStmt.get('op') == 1:
        # multiple selects with UNION combiner attributes: op, larg, rarg
        leftSelectDict = selectStmt.get('larg')
        leftSelectTbl = []
        leftAliasDict = {}
        leftselectReadList = selectParse(leftSelectDict, leftAliasDict, leftSelectTbl, passAnchor, schema)
        rightSelectDict = selectStmt.get('rarg')
        rightSelectTbl = []
        rightAliasDict = {}
        rightselectReadList = selectParse(rightSelectDict, rightAliasDict, rightSelectTbl, passAnchor, schema)
        # combine read operation lists: one select is one list
        selectReadList.extend(leftselectReadList)
        selectReadList.extend(rightselectReadList)
    elif selectStmt.get('op') == 0:
        # only-one select attributes: fromClause, op, targetList, whereClause
        selectDict = {}
        whereDict = {}
        joinReadList = []
        temp = []
        targetList = selectStmt.get('targetList')  # a list
        whereClause = selectStmt.get('whereClause')  # a dict
        fromClause = selectStmt.get('fromClause')  # a list

        # 1. use the fromClause to get aliasDict and JOIN read operations
        # Do not have a from
        if fromClause is None:
            print("This query does not have a from clause.\n")
        else:
            # in case fromClause has more than one table
            for fromTbl in fromClause:
                # from contains a table
                if list(fromTbl.keys())[0] == 'RangeVar':
                    tableName = fromTbl.get('RangeVar').get('relname')
                    if tableName not in selectTbl:
                        # main table of this query always be inserted to head
                        selectTbl.insert(0, tableName)
                    # selectTbl.append(tableName)
                    # check if table has alias name, one table may have multiple aliases
                    if fromTbl.get('RangeVar').get('alias') is not None:
                        # table has alias
                        aliasName = fromTbl.get('RangeVar').get('alias').get('Alias').get('aliasname')
                        aliasDict[aliasName] = tableName
                # from contains a join
                elif list(fromTbl.keys())[0] == 'JoinExpr':
                    joinExpr = fromTbl
                    joinReadList = joinRecur(joinExpr, aliasDict, selectTbl, passAnchor, schema, selectReadList)
                    newJoinList = []
                    # change alias names to table names
                    for item in joinReadList:
                        l1 = list(item)
                        table = aliasDict.get(item[2])
                        # print(table)
                        if table is not None:
                            l1[2] = table
                            t1 = tuple(l1)
                            newJoinList.append(t1)
                        else:
                            newJoinList.append(item)
                    joinReadList = newJoinList
                    joinReadList.sort()
                    # temp.extend(joinReadList)
                # from contains a subquery(subselect): it is a temporary table, other queries cannot see it
                elif list(fromTbl.keys())[0] == 'RangeSubselect':
                    nestSelectStmtDict = fromTbl.get('RangeSubselect').get('subquery')
                    anchor += 1
                    subselectTbl = selectTbl[:]
                    subaliasDict = aliasDict.copy()
                    nestSelectReadList = selectParse(nestSelectStmtDict, subaliasDict, subselectTbl, anchor, schema)
                    selectReadList.extend(nestSelectReadList)
                    # construct a virtual table refer to the subquery: no main table if subquery in from
                    tableName = '?' + str(anchor)
                    selectTbl.append(tableName)
                    # return selectReadList
                else:
                    raise ValueError("Unexpected from type %r" % list(fromTbl.keys())[0])
        # print("After from: %r" % selectTbl)
        # 2. get targetList read operations
        for target in targetList:
            TableName = ""
            targetType = next(iter(target.get('ResTarget').get('val')))
            if targetType == 'ColumnRef':
                fields = target.get('ResTarget').get('val').get('ColumnRef').get('fields')
                if len(fields) == 1:
                    firstKey = next(iter(fields[0]))
                    # case 1: only has the column name
                    if firstKey == 'String':
                        selectCol = fields[0].get('String').get('str')
                        # verify which table this column is in
                        if len(selectTbl) == 1:
                            colList = schema.tables.get(selectTbl[0], [])
                            if selectCol in colList:
                                tableName = selectTbl[0]
                        else:
                            # multiple from tables (including join tables)
                            for table in selectTbl:
                                colList = schema.tables.get(table, [])
                                if selectCol in colList:
                                    tableName = table
                        if tableName == "":
                            # no table has this column
                            continue
                        elif tableName.startswith('?'):
                            # construct a virtual target column
                            col = tableName
                            selectDict[tableName] = selectDict.get(tableName,
                                                                   [])
                            selectDict[tableName].append(col)
                        else:
                            selectDict[tableName] = selectDict.get(tableName,
                                                                   [])
                            selectDict[tableName].append(selectCol)
                    # case 2: target column is a star(*)
                    if firstKey == 'A_Star':
                        if len(selectTbl) == 0:
                            raise ValueError("Unexpected main table: %r" % selectTbl)
                        else:
                            tbl = selectTbl[0]
                            # main table is a virtual table
                            if tbl.startswith('?'):
                                # construct a virtual target column
                                col = tbl
                                selectDict[tbl] = selectDict.get(tbl, [])
                                selectDict[tbl].append(col)
                            else:
                                cols = dbSchema.tables[tbl]
                                for col in cols:
                                    selectDict[tbl] = selectDict.get(tbl, [])
                                    selectDict[tbl].append(col)
                else:
                    tbl = fields[0].get('String').get('str')
                    tablename = aliasDict.get(tbl) if aliasDict.get(tbl) is not None else tbl
                    if tablename not in selectTbl:
                        # a temporary table
                        col = '?' + str(anchor)
                        selectDict[selectTbl[0]] = selectDict.get(selectTbl[0], [])
                        selectDict[selectTbl[0]].append(col)
                    else:
                        firstKey = next(iter(fields[1]))
                        # case 1: has the column name
                        if firstKey == 'String':
                            selectCol = fields[1].get('String').get('str')
                            # check if column is in the table
                            colList = schema.tables.get(tablename, [])
                            if selectCol in colList:
                                selectDict[tablename] = selectDict.get(
                                    tablename, [])
                                selectDict[tablename].append(selectCol)
                            else:
                                continue
                        # case 2: column is a star(*)
                        if firstKey == 'A_Star':
                            cols = dbSchema.tables[tablename]
                            for col in cols:
                                selectDict[tablename] = selectDict.get(tablename, [])
                                selectDict[tablename].append(col)
            elif targetType == 'FuncCall':
                # contain function in select target list
                # function contains star(*)
                if target.get('ResTarget').get('val').get('FuncCall').get('agg_star', False):
                    # a star in function
                    tbl = selectTbl[0]
                    cols = dbSchema.tables[tbl]
                    for col in cols:
                        selectDict[tbl] = selectDict.get(tbl, [])
                        selectDict[tbl].append(col)
                # function contains column ref/expr/nestfunction
                else:
                    funcDict = target.get('ResTarget').get('val')
                    tableName = selectTbl[0]
                    if tableName.startswith('?'):
                        # construct a virtual target column
                        col = tableName
                        selectDict[tableName] = selectDict.get(tableName, [])
                        selectDict[tableName].append(col)
                    else:
                        funcTupleList = funcParse(funcDict, aliasDict, selectTbl, schema, passAnchor, selectReadList)
                        for tu in funcTupleList:
                            selectCol = tu[1]
                            tableName = tu[0]
                            selectDict[tableName] = selectDict.get(tableName, [])
                            if selectCol not in selectDict[tableName]:
                                selectDict[tableName].append(selectCol)
            # tpcc case
            elif targetType == 'CoalesceExpr':
                coalesceArgsList = target.get('ResTarget').get('val').get('CoalesceExpr').get('args')
                for item in coalesceArgsList:
                    if list(item.keys())[0] == 'A_Const':
                        continue
                    # arg is a column ref
                    elif list(item.keys())[0] == 'ColumnRef':
                        fields = item.get('ColumnRef').get('fields')
                        # column ref only have column name
                        tbl_col = columnRefParse(fields, aliasDict, selectTbl, schema)
                        table = tbl_col[0]
                        col = tbl_col[1]
                        selectDict[table] = selectDict.get(table, [])
                        selectDict[table].append(col)
                    # arg is a another function call
                    elif list(item.keys())[0] == 'FuncCall':
                        funcDict = item
                        funcTupleList = funcParse(funcDict, aliasDict, selectTbl, schema, passAnchor, selectReadList)
                        for tu in funcTupleList:
                            selectCol = tu[1]
                            tableName = tu[0]
                            selectDict[tableName] = selectDict.get(tableName, [])
                            if selectCol not in selectDict[tableName]:
                                selectDict[tableName].append(selectCol)
                    else:
                        raise ValueError("Unexpected CoalesceExpr arg type")
            elif targetType == 'A_Const':
                # select (1) as ... from ...
                col = '1'
                table = selectTbl[0]
                selectDict[table] = selectDict.get(table, [])
                selectDict[table].append(col)
            elif targetType == 'SubLink':
                # select (subquery) as ... from ...
                targetNestSelectStmtDict = target.get('ResTarget').get('val').get('SubLink').get('subselect')
                anchor += 1
                subselectTbl = selectTbl[:]
                subaliasDict = aliasDict.copy()
                targetNestSelectReadList = selectParse(targetNestSelectStmtDict, subaliasDict, subselectTbl, anchor, schema)
                selectReadList.extend(targetNestSelectReadList)
                # construct a virtual column refer to the subquery: no target column if subquery in a target col
                col = '?' + str(anchor)
                if selectTbl == []:
                    tbl = col
                    selectTbl.append(tbl)
                selectDict[selectTbl[0]] = selectDict.get(selectTbl[0], [])
                selectDict[selectTbl[0]].append(col)
            # fa case:
            elif targetType == 'TypeCast':
                castArg = target.get('ResTarget').get('val').get('TypeCast').get('arg')
                if list(castArg.keys())[0] == 'FuncCall':
                    funcDict = castArg
                    funcTupleList = funcParse(funcDict, aliasDict, selectTbl, schema, passAnchor, selectReadList)
                    for tu in funcTupleList:
                        selectCol = tu[1]
                        tableName = tu[0]
                        selectDict[tableName] = selectDict.get(tableName, [])
                        if selectCol not in selectDict[tableName]:
                            selectDict[tableName].append(selectCol)
                elif list(castArg.keys())[0] == 'ColumnRef':
                    castFields = castArg.get('ColumnRef').get('fields')
                    if len(castFields) == 1:
                        selectCol = castFields[0].get('String').get('str')
                        selectDict[selectTbl[0]] = selectDict.get(selectTbl[0], [])
                        selectDict[selectTbl[0]].append(selectCol)
                    else:
                        tbl = castFields[0].get('String').get('str')
                        col = castFields[1].get('String').get('str')
                        table = aliasDict.get(tbl) if aliasDict.get(tbl) is not None else tbl
                        selectDict[table] = selectDict.get(table, [])
                        selectDict[table].append(col)
                else:
                    raise ValueError("Unexpected select target typecast arg type %r" % list(castArg.keys())[0])
            # broadleaf case:
            elif targetType == 'CaseExpr':
                # Not process CaseExpr yet
                continue
            elif targetType == 'BoolExpr':
                # Not process BoolExpr yet
                continue
            elif targetType == 'A_Expr':
                exprOp = target.get('ResTarget').get('val').get('A_Expr').get('name')[0].get('String').get('str')
                lexprTarget = target.get('ResTarget').get('val').get('A_Expr').get('lexpr')
                rexprTarget = target.get('ResTarget').get('val').get('A_Expr').get('rexpr')
                lExprTuList = lrExprParse(lexprTarget, aliasDict, selectTbl, schema, passAnchor, selectReadList)
                rExprTuList = lrExprParse(rexprTarget, aliasDict, selectTbl, schema, passAnchor, selectReadList)
                for tu in rExprTuList:
                    if tu not in lExprTuList:
                        lExprTuList.append(tu)
                for tu in lExprTuList:
                    selectCol = tu[1]
                    tableName = tu[0]
                    selectDict[tableName] = selectDict.get(tableName, [])
                    if selectCol not in selectDict[tableName]:
                        selectDict[tableName].append(selectCol)
                print("This is an expression in target:")
            else:
                raise ValueError("Unexpected select target type %r" % targetType)

        # 3. get the where read operations if any: update whereDict and selectReadList
        if whereClause is not None:
            whereType = list(whereClause.keys())[0]
            if whereType == 'BoolExpr':
                # multiple where conditions combined with AND/OR: recursive where parser
                whereBoolExprParse(whereClause, aliasDict, selectTbl, whereDict, selectReadList, anchor, schema)
            elif whereType == 'A_Expr':
                # one where condition
                whereOneExprParse(whereClause, aliasDict, selectTbl, whereDict, selectReadList, anchor, schema)
            elif whereType == 'NullTest':
                # IS NULL test
                nullFields = whereClause.get('NullTest').get('arg').get('ColumnRef').get('fields')
                if len(nullFields) == 1:
                    table = selectTbl[0]
                    left = (table, nullFields[0].get('String').get('str'))
                else:
                    tbl = nullFields[0].get('String').get('str')
                    col = nullFields[1].get('String').get('str')
                    table = aliasDict.get(tbl) if aliasDict.get(tbl) is not None else tbl
                    left = (table, col)
                right = ('Type_3', '?')
                whereDict[left] = whereDict.get(left, [])
                if isinstance(whereDict[left], list):
                    existRights = whereDict[left]
                    if right not in existRights:
                        whereDict[left].append(right)
                else:
                    print('Null test column has a value in foregoing where conditions')
            elif whereType == 'ColumnRef':
                # Where only has a column ref, no value
                colFields = whereClause.get('ColumnRef').get('fields')
                if len(colFields) == 1:
                    table = selectTbl[0]
                    left = (table, colFields[0].get('String').get('str'))
                else:
                    tbl = colFields[0].get('String').get('str')
                    col = colFields[1].get('String').get('str')
                    table = aliasDict.get(tbl) if aliasDict.get(tbl) is not None else tbl
                    left = (table, col)
                right = ('Type_3', '?')
                whereDict[left] = whereDict.get(left, [])
                if isinstance(whereDict[left], list):
                    existRights = whereDict[left]
                    if right not in existRights:
                        whereDict[left].append(right)
                else:
                    print('Only column in where has a value in foregoing where conditions')
            elif whereType == 'A_Const':
                print("No read operations!\n")
            else:
                # print(whereType)
                raise ValueError("Unexpected where clause type: %r" % whereType)

        # add JOIN operations
        if len(joinReadList) != 0:
            temp.extend(joinReadList)
        # add WHERE operations
        # print(whereDict)
        if len(whereDict) != 0:
            left_tbl = []
            left_dict = {}
            col_tuples = list(whereDict.keys())
            for tp in col_tuples:
                left_dict[tp[0]] = left_dict.get(tp[0], [])
                left_dict[tp[0]].append(tp[1])
            left_tbl = list(left_dict.keys())
            left_tbl.sort()
            for tbl in left_tbl:
                left_col = left_dict.get(tbl)
                left_col.sort()
                for col in left_col:
                    colvalue = whereDict[(tbl, col)]
                    if isinstance(colvalue, tuple):
                        element = (passAnchor, 'WHERE', tbl, col, colvalue)
                        temp.append(element)
                    elif isinstance(colvalue, list):
                        # multiple conditions on same column
                        for val in colvalue:
                            # element = ('READ', tbl, col, 'WHERE', val)
                            element = (passAnchor, 'WHERE', tbl, col, val)
                            temp.append(element)
                    else:
                        element = (passAnchor, 'WHERE', tbl, col, colvalue)
                        temp.append(element)
        # add SELECT read operations
        if len(selectDict) != 0:
            target_tbl = list(selectDict.keys())
            target_tbl.sort()
            for tbl in target_tbl:
                target_cols = selectDict.get(tbl)
                target_cols.sort()
                for col in target_cols:
                    if col == '1':
                        # element = (passAnchor, 'SELECT', tbl, col, ('Type_5', '?'))
                        for key, value in schema.colKeys.items():
                            if key[0] == tbl and schema.colKeys[
                                key] == KeyType.pri:
                                col = key[1]
                                element = (
                                passAnchor, 'SELECT', tbl, col, ('Type_3', '?'))
                                if element not in temp:
                                    temp.append(element)
                    else:
                        if col.startswith('?'):
                            number = int(col[1:])
                            element = (
                            passAnchor, 'SELECT', tbl, col, ('Type_2', number))
                        else:
                            element = (
                            passAnchor, 'SELECT', tbl, col, ('Type_3', '?'))
                        if element not in temp:
                            temp.append(element)
        selectReadList.extend(temp)
    else:
        print(selectStmt.get('op'))
        raise ValueError("Unexpected select op type %r" % selectStmt.get('op'))
    return selectReadList

# transform transaction list to operation tuple list
def generateOpsList(txnList, schema: DbSchema):
    tempOpsList = []
    opsList = []
    tempSqlList = []
    sqlList = []
    count = 0
    for stmt in txnList:
        # print(stmt)
        # rxy: 5/18
        #tempSqlList.append(stmt)
        queryType = getQueryType(stmt)
        if queryType == QueryType.startTxn or queryType == QueryType.startTxnCond:
            beginList = [('Begin Transaction',)]
            tempOpsList.append(beginList)
        elif queryType == QueryType.endTxn or queryType == QueryType.endTxnCond:
            endList = [('Commit',)]
            if len(tempOpsList) > 0:
                tempOpsList.append(endList)
                opsList.append(tempOpsList[:])
                # rxy: 5/18
                #opsList.append(tempSqlList[:])
                tempOpsList.clear()
                #tempSqlList.clear()
            else:
                continue
            # may have problems in tpcc
        else:
            if queryType == QueryType.select:
                anchor = 0
                root = Node(parse_sql(stmt))
                selectStmtDict = root[0].parse_tree.get('stmt')
                selectTbl = []
                aliasDict = {}  # alias name --> table name
                selectReadOps = selectParse(selectStmtDict, aliasDict, selectTbl, anchor, schema)
                # print("After top select: %r" % selectTbl)
                tempOpsList.append(selectReadOps)
                # rxy: 0518
                tempOpsList.append(stmt)
            elif queryType == QueryType.insert:
                anchor = 0
                insertCols = []
                insertVals = []
                temp = []
                root = Node(parse_sql(stmt))
                insertDtmtDict = root[0].parse_tree.get('stmt')
                # 1. get insert table
                insertTbl = insertDtmtDict.get('InsertStmt').get('relation').get('RangeVar').get('relname')
                # 2. extract insert columns or get from schema file
                if 'cols' in insertDtmtDict.get('InsertStmt'):
                    colList = insertDtmtDict.get('InsertStmt').get('cols')
                    for colDict in colList:
                        col = colDict.get('ResTarget').get('name')
                        insertCols.append(col)
                # No columns in insert, get them from schema file
                else:
                    cols = dbSchema.tables[insertTbl]
                    for col in cols:
                        insertCols.append(col)
                # 3. get insert values
                valList = insertDtmtDict.get('InsertStmt').get('selectStmt').get('SelectStmt').get('valuesLists')[0]
                for valDict in valList:
                    # print(valDict)
                    insertValType = list(valDict.keys())[0]
                    if insertValType == 'A_Const':
                        if list(valDict.get('A_Const').get('val').keys())[0] == 'Null':
                            val = 'NULL'
                        else:
                            val = list(list(valDict.get('A_Const').get('val').values())[0].values())[0]
                        insertVals.append(val)
                    elif insertValType == 'FuncCall':
                        val = '?'
                        insertVals.append(val)
                    elif insertValType == 'SQLValueFunction':
                        val = '?'
                        insertVals.append(val)
                    else:
                        raise ValueError("Unexpected insert value type %r" % insertValType)
                # 4. construct insert operation tuples
                col_val = {}
                for idx in range(0, len(insertCols)):
                    col_val[insertCols[idx]] = insertVals[idx]
                insertCols.sort()
                for item in insertCols:
                    # element = ('WRITE', insertTbl, item, 'INSERT', col_val[item])
                    newRight = ('Type_1', col_val[item])
                    element = (anchor, 'INSERT', insertTbl, item, newRight)
                    temp.append(element)
                # last function has insert start transaction and commit stmt to add corresponding tuple of begin and comit
                tempOpsList.append(temp)
                # rxy: 0518
                tempOpsList.append(stmt)
            elif queryType == QueryType.update:
                anchor = 0
                passAnchor = anchor
                updateTbl = []
                targetList = []
                updateDict = {}
                aliasDict = {}
                whereDict = {}
                updateRead = []
                root = Node(parse_sql(stmt))
                updateStmtDict = root[0].parse_tree.get('stmt')
                # 1. get update table
                updateTbl.append(updateStmtDict.get('UpdateStmt').get('relation').get('RangeVar').get('relname'))
                # 2. get update columns and values
                targetList = updateStmtDict.get('UpdateStmt').get('targetList')
                for setDict in targetList:
                    if setDict.get('ResTarget').get('indirection') is not None:
                        col = setDict.get('ResTarget').get('indirection')[0].get('String').get('str')
                    else:
                        col = setDict.get('ResTarget').get('name')
                    valType = list(setDict.get('ResTarget').get('val').keys())[0]
                    if valType == 'A_Const':
                        if list(setDict.get('ResTarget').get('val').get('A_Const').get('val').keys())[0] == 'Null':
                            val = 'NULL'
                        else:
                            val = list(list(setDict.get('ResTarget').get('val').get('A_Const').get('val').values())[0].values())[0]
                        updateDict[col] = ('Type_1', val)
                    elif valType == 'A_Expr':
                        leftExpr = setDict.get('ResTarget').get('val').get('A_Expr').get('lexpr')
                        operator = setDict.get('ResTarget').get('val').get('A_Expr').get('name')[0].get('String').get(
                            'str')
                        rightExpr = setDict.get('ResTarget').get('val').get('A_Expr').get('rexpr')
                        if next(iter(leftExpr)) == 'ColumnRef':
                            leftOp = leftExpr.get('ColumnRef').get('fields')[0].get('String').get('str')
                        else:
                            leftOp = list(list(leftExpr.get('A_Const').get('val').values())[0].values())[0]
                        if next(iter(rightExpr)) == 'ColumnRef':
                            rightOp = rightExpr.get('ColumnRef').get('fields')[0].get('String').get('str')
                        else:
                            rightOp = list(list(rightExpr.get('A_Const').get('val').values())[0].values())[0]
                        expr = str(leftOp) + operator + str(rightOp)
                        updateDict[col] = ('Type_1', expr)
                    elif valType == 'SubLink':
                        subselectDict = setDict.get('ResTarget').get('val').get('SubLink').get('subselect')
                        subselectTbl = []
                        subaliasDict = {}
                        passAnchor += 1
                        subselectReadList = selectParse(subselectDict, subaliasDict, subselectTbl, passAnchor, schema)
                        updateRead.extend(subselectReadList)
                        updateDict[col] = ('Type_2', passAnchor)
                    elif valType == 'FuncCall':
                        # to do: apply function parse
                        val = '?'
                        updateDict[col] = ('Type_1', val)
                    else:
                        raise ValueError("Unexpected update set value type %r" % valType)
                # 3. get UPDATE where conditions
                whereClause = updateStmtDict.get('UpdateStmt').get('whereClause')  # a dict
                whereType = list(whereClause.keys())[0]
                if whereType == 'BoolExpr':
                    whereBoolExprParse(whereClause, aliasDict, updateTbl, whereDict, updateRead, passAnchor, schema)
                elif whereType == 'A_Expr':
                    whereOneExprParse(whereClause, aliasDict, updateTbl, whereDict, updateRead, passAnchor, schema)
                else:
                    raise ValueError("Unexpected where type %r" % whereType)
                # 4. add update where operations
                # print(whereDict)
                if len(whereDict) != 0:
                    left_tbl = []
                    left_dict = {}
                    col_tuples = list(whereDict.keys())
                    for tp in col_tuples:
                        left_dict[tp[0]] = left_dict.get(tp[0], [])
                        left_dict[tp[0]].append(tp[1])
                    left_tbl = list(left_dict.keys())
                    left_tbl.sort()
                    for tbl in left_tbl:
                        left_col = left_dict.get(tbl)
                        left_col.sort()
                        for col in left_col:
                            colvalue = whereDict[(tbl, col)]
                            if isinstance(colvalue, tuple):
                                element = (anchor, 'WHERE', tbl, col, colvalue)
                                updateRead.append(element)
                            elif isinstance(colvalue, list):
                                # multiple conditions on same column
                                for val in colvalue:
                                    element = (anchor, 'WHERE', tbl, col, val)
                                    updateRead.append(element)
                            else:
                                element = (anchor, 'WHERE', tbl, col, colvalue)
                                updateRead.append(element)
                # 5. add UPDATE write operations
                updateCols = list(updateDict.keys())
                updateCols.sort()
                for col in updateCols:
                    element = (anchor, 'UPDATE', updateTbl[0], col, updateDict[col])
                    updateRead.append(element)
                tempOpsList.append(updateRead)
                # rxy: 0518
                tempOpsList.append(stmt)
            elif queryType == QueryType.delete:
                anchor = 0
                passAnchor = anchor
                deleteTbl = []
                deleteCols = []
                aliasDict = {}
                whereDict = {}
                whereRead = []
                root = Node(parse_sql(stmt))
                deleteStmtDict = root[0].parse_tree.get('stmt')
                # 1. get delete table
                deleteTbl.append(deleteStmtDict.get('DeleteStmt').get('relation').get('RangeVar').get('relname'))
                # 2. get delete where conditions
                whereClause = deleteStmtDict.get('DeleteStmt').get('whereClause')  # a dict
                whereType = list(whereClause.keys())[0]
                if whereType == 'BoolExpr':
                    whereBoolExprParse(whereClause, aliasDict, deleteTbl, whereDict, whereRead, passAnchor, schema)
                elif whereType == 'A_Expr':
                    whereOneExprParse(whereClause, aliasDict, deleteTbl, whereDict, whereRead, passAnchor, schema)
                else:
                    raise ValueError("Unexpected where type %r" % whereType)
                # 3. add DELETE where operations
                if len(whereDict) != 0:
                    left_tbl = []
                    left_dict = {}
                    col_tuples = list(whereDict.keys())
                    for tp in col_tuples:
                        left_dict[tp[0]] = left_dict.get(tp[0], [])
                        left_dict[tp[0]].append(tp[1])
                    left_tbl = list(left_dict.keys())
                    left_tbl.sort()
                    for tbl in left_tbl:
                        left_col = left_dict.get(tbl)
                        left_col.sort()
                        for col in left_col:
                            colvalue = whereDict[(tbl, col)]
                            if isinstance(colvalue, tuple):
                                element = (anchor, 'WHERE', tbl, col, colvalue)
                                whereRead.append(element)
                            elif isinstance(colvalue, list):
                                # multiple conditions on same column
                                for val in colvalue:
                                    element = (anchor, 'WHERE', tbl, col, val)
                                    whereRead.append(element)
                            else:
                                element = (anchor, 'WHERE', tbl, col, colvalue)
                                whereRead.append(element)
                # 4. add DELETE write operations
                deleteCols = dbSchema.tables[deleteTbl[0]]
                deleteCols.sort()
                for col in deleteCols:
                    element = (anchor, 'DELETE',  deleteTbl[0], col, ('Type_3', '?'))
                    whereRead.append(element)
                tempOpsList.append(whereRead)
                # rxy: 0518
                tempOpsList.append(stmt)
    return opsList


def stmtRewrite(statement, application_name):
    originalStmt = statement
    skippableWords = [' ALL', 'DISTINCT', 'DISTINCTION', 'HIGH_PRIORITY',
                      'SQL_SMALL_STATEMENT', 'SQL_BIG_RESULT',
                      'SQL_BUFFER_RESULT', 'SQL_CACHE', 'SQL_NO_CACHE',
                      'SQL_CALC_FOUND_ROWS']
    originalStmt = re.sub('"', "'", originalStmt)
    originalStmt = re.sub('""', "''", originalStmt)
    # originalStmt = originalStmt.replace('\\', '')
    originalStmt = re.sub('BINARY', '', originalStmt)
    originalStmt = re.sub("_latin1", '', originalStmt)
    originalStmt = re.sub(r"ON DUPLICATE KEY UPDATE(.*)", '', originalStmt)
    originalStmt = re.sub(r"REGEXP\s.*", '', originalStmt)
    originalStmt = re.sub(r'LIMIT\s\d\,\d+', '', originalStmt)
    originalStmt = re.sub(r'LIMIT\s\d\,\s\d+', '', originalStmt)
    originalStmt = re.sub('`from`', '"from"', originalStmt)
    originalStmt = re.sub('`to`', '"to"', originalStmt)
    originalStmt = re.sub(' user,', ' "user", ', originalStmt)
    originalStmt = originalStmt.replace('user.', '"user".')
    originalStmt = originalStmt.replace(' user ', ' "user" ')
    # need the end replacement for fa
    if application_name == 'fa':
        originalStmt = re.sub(' end ', ' "end" ',
                              originalStmt)  # comment on broadleaf
    originalStmt = re.sub(' zone ', ' "zone" ', originalStmt)
    originalStmt = re.sub('`limit`', '"limit"', originalStmt)
    originalStmt = re.sub('`default`', '"default"', originalStmt)
    originalStmt = re.sub(r'(INTERVAL\s\d+\s\w+)', r"'\1'", originalStmt)
    originalStmt = re.sub(r"\\'v6\\'", 'v6', originalStmt)
    originalStmt = re.sub(r"\\'", '', originalStmt)
    originalStmt = re.sub('STRAIGHT_JOIN', 'JOIN', originalStmt)
    originalStmt = re.sub('`', '', originalStmt)
    # originalStmt = re.sub(r"(\\\')", '', originalStmt)
    # originalStmt = re.sub(r"LIMIT(.*)", '', originalStmt)

    # skip words
    for token in skippableWords:
        if token in originalStmt:
            originalStmt = re.sub(token, '', originalStmt)

    splitList = originalStmt.split()
    # deal with order by within DELETE/UPDATE statements
    if len(splitList) != 0 and (splitList[0] == 'DELETE' or splitList[0] == 'UPDATE'):
        originalStmt = re.sub(r"ORDER BY(.*)", '', originalStmt)

    # deal with INSERT INTO ... SET ... statements
    if len(splitList) != 0 and (splitList[0] == 'INSERT' and splitList[3] == 'SET'):
        rewrite = ' '
        acc = ''
        setMapping = {}
        setString = splitList[4:]
        for token in setString:
            acc = acc + str(token) + ' '
        setpair = splitUnquoted(acc, ',')
        for subString in setpair:
            eqIdx = subString.index('=')
            col = subString[0:eqIdx].split('.')[-1].strip()
            val = subString[eqIdx + 1:].strip()
            setMapping[col] = val
        rewrite = rewrite.join(splitList[:3])
        keystr = ', '.join(setMapping.keys())
        valstr = ', '.join(setMapping.values())
        rewrite = rewrite + ' (' + keystr + ') VALUES (' + valstr + ')'
        originalStmt = rewrite
    return originalStmt


def preProcess(logs, application_name):
    logs = getSqlStmts(logs)
    logs = filterUninterestingLogs(logs)
    logs = filterEmptyTxns(logs)
    sqlStrList = []

    for stmt in logs:
        if 'AGAINST' in stmt:
            continue
        else:
            formatSQL = stmtRewrite(stmt, application_name)
        sqlStrList.append(formatSQL)
    # count = len(sqlStrList)
    txnlist = generateTxnList(sqlStrList, application_name)

    return txnlist

# -----------------------------------------------------------------
# | Beginning of top level of script                              |
# -----------------------------------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument("file_name", help="Name of the file to read")
parser.add_argument("db_schema_file",
                    help="Name of the csv file containing the db_schema in the form table_name, col_name, col_key")
parser.add_argument("output_dir", help="Directory to store processing output")
args = parser.parse_args()

print("Reading from file " + args.file_name)

before_schema_time = time.time()
dbSchema = readDbSchemaFile(args.db_schema_file)
db_filename = str(args.db_schema_file).split("/")
application = db_filename[-1].split("_")
application_name = application[0]
print(application)
output_folder = args.output_dir

before_read_time = time.time()
logs = readRawLogs(args.file_name)
post_read_time = time.time()
count = len(logs)

txnlist = preProcess(logs, application_name)

post_preprocess_time = time.time()

parseResult = generateOpsList(txnlist, dbSchema)

f = open(output_folder+'/pglast_' + application[0] + '.txt', 'w+')
for element in parseResult:
    for item in element:
        s = str(item)
        f.write(s + '\n')
f.close()

post_parse_time = time.time()

print('')
print('')
print("PRINTING LOG STATS")
print('Num Logs: ' + str(count))
print('')
print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
print('Total Time: ' + str(post_parse_time - before_schema_time))
print('Read Data: ' + str(post_read_time - before_read_time))
print('Reorganize to txn Time: ' + str(post_preprocess_time - post_read_time))
print('Parse Time: ' + str(post_parse_time - post_preprocess_time))
