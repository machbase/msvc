/******************************************************************************
 * Copyright of this product 2013-2023,
 * InfiniFlux Corporation(or Inc.) or its subsidiaries.
 * All Rights reserved.
 ******************************************************************************/

/* $Id:$ */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <windows.h>
#include <machbase_sqlcli.h>

#define MACHBASE_PORT_NO		5656

#define RC_SUCCESS          0
#define RC_FAILURE          -1

#define CHECK_STMT_RESULT(aRC, aSTMT, aMsg) \
    if( sRC != SQL_SUCCESS )                \
    {                                       \
        printError(gEnv, gCon, aSTMT, aMsg);\
        goto error;                         \
    }                                        


SQLHENV 	gEnv;
SQLHDBC 	gCon;

void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg);
int connectDB();
void disconnectDB();

void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( aMsg != NULL )
    {
        printf("%s\n", aMsg);
    }

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) == SQL_SUCCESS )
    {
        printf("SQLSTATE-[%s], Machbase-[%ld][%s]\n", sSqlState, sNativeError, sErrorMsg);
    }
}

int connectDB()
{
    char sConnStr[1024];

    if( SQLAllocEnv(&gEnv) != SQL_SUCCESS ) 
    {
        printf("SQLAllocEnv error\n");
        return RC_FAILURE;
    }

    if( SQLAllocConnect(gEnv, &gCon) != SQL_SUCCESS ) 
    {
        printf("SQLAllocConnect error\n");

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    sprintf_s(sConnStr, sizeof(sConnStr), "DSN=127.0.0.1;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=%d", MACHBASE_PORT_NO);

    if( SQLDriverConnect( gCon, NULL,
                          (SQLCHAR *)sConnStr,
                          SQL_NTS,
                          NULL, 0, NULL,
                          SQL_DRIVER_NOPROMPT ) != SQL_SUCCESS
      )
    {

        printError(gEnv, gCon, NULL, "SQLDriverConnect error");

        SQLFreeConnect(gCon);
        gCon = SQL_NULL_HDBC;

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void disconnectDB()
{
    if( SQLDisconnect(gCon) != SQL_SUCCESS ) 
    {
        printError(gEnv, gCon, NULL, "SQLDisconnect error");
    }

    SQLFreeConnect(gCon);
    gCon = SQL_NULL_HDBC;

    SQLFreeEnv(gEnv);
    gEnv = SQL_NULL_HENV;
}

int executeDirectSQL(const char *aSQL, int aErrIgnore)
{
    SQLHSTMT sStmt = SQL_NULL_HSTMT;

    if( SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS )
    {
        if( aErrIgnore == 0 ) 
        {
            printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
            return RC_FAILURE;
        }
    }

    if( SQLExecDirect(sStmt, (SQLCHAR *)aSQL, SQL_NTS) != SQL_SUCCESS )
    {

        if( aErrIgnore == 0 )
        {
            printError(gEnv, gCon, sStmt, "SQLExecDirect Error");

            SQLFreeStmt(sStmt,SQL_DROP);
            sStmt = SQL_NULL_HSTMT;
            return RC_FAILURE;
        }
    }

    if( SQLFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS )
    {
        if (aErrIgnore == 0) 
        {
            printError(gEnv, gCon, sStmt, "SQLFreeStmt Error");
            sStmt = SQL_NULL_HSTMT;
            return RC_FAILURE;
        }
    }
    sStmt = SQL_NULL_HSTMT;

    return RC_SUCCESS;
}

int createTable()
{
    int sRC;

    sRC = executeDirectSQL("DROP TABLE CLI_SAMPLE", 1);
    if( sRC != RC_SUCCESS )
    {
        return RC_FAILURE;
    }

    sRC = executeDirectSQL("CREATE TABLE CLI_SAMPLE(seq short, score integer, total long, percentage float, ratio double, id varchar(10), srcip ipv4, dstip ipv6, reg_date datetime, textlog text, image binary)", 0);
    if( sRC != RC_SUCCESS )
    {
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}


int main()
{
    const char      *sTableName  = "CLI_SAMPLE";

    SQLHSTMT         sStmt = SQL_NULL_HSTMT;
    SQLRETURN        sRC   = SQL_ERROR;

    SQLCHAR          sColName[32];
    SQLLEN           sColNameLen;
    SQLSMALLINT      sColType;
    SQLLEN           sColTypeLen;
    SQLCHAR          sColTypeName[16];
    SQLLEN           sColTypeNameLen;
    SQLINTEGER       sColSize;
    SQLLEN           sColSizeLen;

    if( connectDB() == RC_SUCCESS )
    {
        printf("connectDB success.\n");
    }
    else
    {
        printf("connectDB failure.\n");
        goto error;
    }

    if( createTable() == RC_SUCCESS )
    {
        printf("createTable success.\n");
    }
    else
    {
        printf("createTable failure.\n");
        goto error;
    }

    if( SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS ) 
    {
        printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
        goto error;
    }

    if( SQLColumns( sStmt, 
                    NULL, 0,                         //CataglogName, length
                    NULL, 0,                         //SchemaName,length
                    (SQLCHAR *)sTableName, SQL_NTS,  //TableName, length
                    NULL, 0                          //ColumnName, length 
                  ) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLColumns Error");
        goto error;
    }

    sRC = SQLBindCol(sStmt, 4, SQL_C_CHAR, sColName, sizeof(sColName), &sColNameLen);
    CHECK_STMT_RESULT(sRC, sStmt, "SQLBindCol 4 Error");

    sRC = SQLBindCol(sStmt, 5, SQL_C_SSHORT, &sColType, 0, &sColTypeLen);
    CHECK_STMT_RESULT(sRC, sStmt, "SQLBindCol 5 Error");

    sRC = SQLBindCol(sStmt, 6, SQL_C_CHAR, sColTypeName, sizeof(sColTypeName), &sColTypeNameLen);
    CHECK_STMT_RESULT(sRC, sStmt, "SQLBindCol 6 Error");

    sRC = SQLBindCol(sStmt, 7, SQL_C_SLONG, &sColSize, 0, &sColSizeLen);
    CHECK_STMT_RESULT(sRC, sStmt, "SQLBindCol 7 Error");

    printf("--------------------------------------------------------------------------------\n");
    printf("%32s%16s%16s%10s\n","Name","Type","TypeName","Length");
    printf("--------------------------------------------------------------------------------\n");

    while( SQLFetch(sStmt) == SQL_SUCCESS )
    {
        printf("%32s%16d%16s%10d\n", sColName, sColType, sColTypeName, (int)sColSize);
    }
    printf("--------------------------------------------------------------------------------\n");


    if( SQLFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLFreeStmt Error");
        goto error;
    }
    sStmt = SQL_NULL_HSTMT;


    disconnectDB();

    return RC_SUCCESS;

error:
    if( sStmt != SQL_NULL_HSTMT )
    {
        SQLFreeStmt(sStmt, SQL_DROP);
        sStmt = SQL_NULL_HSTMT;
    }

    if( gCon != SQL_NULL_HDBC )
    {
        disconnectDB();
    }
    
    return RC_FAILURE;
}
