#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <winsock2.h>
#include <machbase_sqlcli.h>

#define MACHBASE_PORT_NO		5656
#define ERROR_CHECK_COUNT	100

#define RC_SUCCESS          0
#define RC_FAILURE          -1

#define UNUSED(aVar) do { (void)(aVar); } while(0)

#define CHECK_APPEND_RESULT(aRC, aEnv, aCon, aSTMT)             \
    if( !SQL_SUCCEEDED(aRC) )                                   \
    {                                                           \
        if( checkAppendError(aEnv, aCon, aSTMT) == RC_FAILURE ) \
        {                                                       \
            return RC_FAILURE;                                  \
        }                                                       \
    } 


SQLHENV 	gEnv;
SQLHDBC 	gCon;

void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg);
time_t getTimeStamp();
int connectDB();
void disconnectDB();
int executeDirectSQL(const char *aSQL, int aErrIgnore);
int createTable();
int appendOpen(SQLHSTMT aStmt);
int appendData(SQLHSTMT aStmt);
int appendClose(SQLHSTMT aStmt);

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

int checkAppendError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) != SQL_SUCCESS )
    {
        return RC_FAILURE;
    }

    printf("SQLSTATE-[%s], Machbase-[%ld][%s]\n", sSqlState, sNativeError, sErrorMsg);

    if( sNativeError != 9604 &&
        sNativeError != 9605 &&
        sNativeError != 9606 )
    {
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void appendDumpError(SQLHSTMT    aStmt,
                 SQLINTEGER  aErrorCode,
                 SQLPOINTER  aErrorMessage,
                 SQLLEN      aErrorBufLen,
                 SQLPOINTER  aRowBuf,
                 SQLLEN      aRowBufLen)
{
    char       sErrMsg[1024] = {0, };
    char       sRowMsg[32 * 1024] = {0, };

    UNUSED(aStmt);

    if (aErrorMessage != NULL)
    {
        strncpy_s(sErrMsg, strlen(sErrMsg), (char *)aErrorMessage, aErrorBufLen);
    }

    if (aRowBuf != NULL)
    {
        strncpy_s(sRowMsg, strlen(sRowMsg), (char *)aRowBuf, aRowBufLen);
    }

    fprintf(stdout, "Append Error : [%ld][%s]\n[%s]\n\n", aErrorCode, sErrMsg, sRowMsg);
}

time_t getTimeStamp()
{
#if _WIN32 || _WIN64
#if defined(_MSC_VER) || defined(_MSC_EXTENSIONS)
#define DELTA_EPOCH_IN_MICROSECS  11644473600000000Ui64
#else
#define DELTA_EPOCH_IN_MICROSECS  11644473600000000ULL
#endif
    FILETIME         sFT;
    unsigned __int64 sTempResult = 0;
	
    GetSystemTimeAsFileTime(&sFT);
    sTempResult |= sFT.dwHighDateTime;
    sTempResult <<= 32;
    sTempResult |= sFT.dwLowDateTime;

    sTempResult -= DELTA_EPOCH_IN_MICROSECS;
	sTempResult /= 10;
	
    return sTempResult;
#else
    struct timeval sTimeVal;
    int            sRet;
	
    sRet = gettimeofday(&sTimeVal, NULL);

    if (sRet == 0)
    {
        return (time_t)(sTimeVal.tv_sec * 1000000 + sTimeVal.tv_usec);
    }
    else
    {
        return 0;
    }
#endif	
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

int appendOpen(SQLHSTMT aStmt)
{
    const char *sTableName = "CLI_SAMPLE";

    if( SQLAppendOpen(aStmt, (SQLCHAR *)sTableName, ERROR_CHECK_COUNT) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendOpen Error");
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

int appendData(SQLHSTMT aStmt)
{
    SQL_APPEND_PARAM sParam[11];
    SQLRETURN        sRC;

    char             sVarchar[100] = {0, };
    char             sText[100]   = {0, };
    char             sBinary[100] = {0, };

    memset(sParam, 0, sizeof(sParam));

    /* NULL FOR ALL*/
    sParam[0].mShort            = SQL_APPEND_SHORT_NULL;
    sParam[1].mInteger          = SQL_APPEND_INTEGER_NULL;
    sParam[2].mLong             = SQL_APPEND_LONG_NULL;
    sParam[3].mFloat            = SQL_APPEND_FLOAT_NULL;
    sParam[4].mDouble           = SQL_APPEND_DOUBLE_NULL;
    /* varchar */
    sParam[5].mVarchar.mLength  = SQL_APPEND_VARCHAR_NULL;
    /* ipv4 */
    sParam[6].mIP.mLength       = SQL_APPEND_IP_NULL;
    /* ipv6 */
    sParam[7].mIP.mLength       = SQL_APPEND_IP_NULL;
	/* datetime */
    sParam[8].mDateTime.mTime   = SQL_APPEND_DATETIME_NULL;
    /* text */
    sParam[9].mText.mLength     = SQL_APPEND_TEXT_NULL;
    /* binary */
    sParam[10].mBinary.mLength  = SQL_APPEND_BINARY_NULL;
    sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /* FIXED COLUMN Value */
    sParam[0].mShort    = 2;
    sParam[1].mInteger  = 4;
    sParam[2].mLong     = 6;
    sParam[3].mFloat    = (float)8.4;
    sParam[4].mDouble   = 10.9;
    sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /*  VARCHAR : string */
    strcpy_s(sVarchar, sizeof(sVarchar), "MY VARCHAR");
    sParam[5].mVar.mLength = (unsigned int)strlen(sVarchar);
    sParam[5].mVar.mData   = sVarchar;
    sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);


    /* IPv4 : ipv4 from binary bytes */
    sParam[6].mIP.mLength  = SQL_APPEND_IP_IPV4;
    sParam[6].mIP.mAddr[0] = 127;
    sParam[6].mIP.mAddr[1] = 0;
    sParam[6].mIP.mAddr[2] = 0;
    sParam[6].mIP.mAddr[3] = 1;
    sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /* IPv4 : ipv4 from binary */
	/*
    sParam[6].mIP.mLength  = SQL_APPEND_IP_IPV4;
    *(in_addr_t *)(sParam[7].mIP.mAddr) = inet_addr("192.168.0.1");
    sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);
   */

    /* IPv4 : ipv4 from string */
    sParam[6].mIP.mLength     = SQL_APPEND_IP_STRING;
    sParam[6].mIP.mAddrString = "203.212.222.111";
    sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);


    /* IPv6 : ipv6 from binary bytes */
    sParam[7].mIP.mLength  = SQL_APPEND_IP_IPV6;
    sParam[7].mIP.mAddr[0]  = 127;
    sParam[7].mIP.mAddr[1]  = 127;
    sParam[7].mIP.mAddr[2]  = 127;
    sParam[7].mIP.mAddr[3]  = 127;
    sParam[7].mIP.mAddr[4]  = 127;
    sParam[7].mIP.mAddr[5]  = 127;
    sParam[7].mIP.mAddr[6]  = 127;
    sParam[7].mIP.mAddr[7]  = 127;
    sParam[7].mIP.mAddr[8]  = 127;
    sParam[7].mIP.mAddr[9]  = 127;
    sParam[7].mIP.mAddr[10] = 127;
    sParam[7].mIP.mAddr[11] = 127;
    sParam[7].mIP.mAddr[12] = 127;
    sParam[7].mIP.mAddr[13] = 127;
    sParam[7].mIP.mAddr[14] = 127;
    sParam[7].mIP.mAddr[15] = 127;
    sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    sParam[7].mIP.mLength       = SQL_APPEND_IP_NULL; /* recover */
	
    /*  DATETIME : absolute value */
    sParam[8].mDateTime.mTime      = IFLUX_UINT64_LITERAL(1000000000);
    sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /*  DATETIME : current  */
    sParam[8].mDateTime.mTime      = SQL_APPEND_DATETIME_NOW;
    sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /*  DATETIME : string format*/
    sParam[8].mDateTime.mTime      = SQL_APPEND_DATETIME_STRING;
    sParam[8].mDateTime.mDateStr   = "23/May/2014:17:41:28";
    sParam[8].mDateTime.mFormatStr = "DD/MON/YYYY:HH24:MI:SS";
    sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /*  DATETIME : struct tm format*/
    sParam[8].mDateTime.mTime      = SQL_APPEND_DATETIME_STRUCT_TM;
    sParam[8].mDateTime.mTM.tm_year = 2000 - 1900;
    sParam[8].mDateTime.mTM.tm_mon  =  11;
    sParam[8].mDateTime.mTM.tm_mday  = 31;
    sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /*  TEXT : string */
    memset(sText, 'X', sizeof(sText));
    sParam[9].mVar.mLength = 100;
    sParam[9].mVar.mData   = sText;
    sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);


    /*  BINARY : datas */
    memset(sBinary, 0xFA, sizeof(sBinary));
    sParam[10].mVar.mLength = 100;
    sParam[10].mVar.mData   = sBinary;
    sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);


    return RC_SUCCESS;
}

int appendClose(SQLHSTMT aStmt)
{
    long sSuccessCount = 0;
    long sFailureCount = 0;

    if( SQLAppendClose(aStmt, &sSuccessCount, &sFailureCount) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendClose Error");
        return RC_FAILURE;
    }

    printf("success : %ld, failure : %ld\n", sSuccessCount, sFailureCount);

    return sSuccessCount;
}

int main()
{
    SQLHSTMT    sStmt = SQL_NULL_HSTMT;

    int         sCount=0;
    time_t      sStartTime, sEndTime;

    if( connectDB() == RC_SUCCESS )
    {
        printf("connectDB success\n");
    }
    else
    {
        printf("connectDB failure\n");
        goto error;
    }

    if( createTable() == RC_SUCCESS )
    {
        printf("createTable success\n");
    }
    else
    {
        printf("createTable failure\n");
        goto error;
    }

    if( SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS ) 
    {
        printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
        goto error;
    }

    if( appendOpen(sStmt) == RC_SUCCESS )
    {
        printf("appendOpen success\n");
    }
    else
    {
        printf("appendOpen failure\n");
        goto error;
    }

    if( SQLAppendSetErrorCallback(sStmt, appendDumpError) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLAppendSetErrorCallback Error");
        goto error;
    }

    sStartTime = getTimeStamp();
    appendData(sStmt);
    sEndTime = getTimeStamp();

    sCount = appendClose(sStmt);
    if( sCount >= 0 )
    {
        printf("appendClose success\n");
        printf("timegap = %ld microseconds for %d records\n", (long)(sEndTime - sStartTime), sCount);
        printf("%.2f records/second\n",  ((double)sCount/(double)(sEndTime - sStartTime))*1000000);
    }
    else
    {
        printf("appendClose failure\n");
    }


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
