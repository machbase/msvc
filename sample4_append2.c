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

#define CHECK_STMT_RESULT(aRC, aSTMT, aMsg)     \
    if( sRC != SQL_SUCCESS )                    \
    {                                           \
        printError(gEnv, gCon, aSTMT, aMsg);    \
        goto error;                             \
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

    sRC = executeDirectSQL("CREATE TABLE CLI_SAMPLE(seq short, score integer, total long, percentage float, ratio double, id varchar(100), srcip ipv4, dstip ipv6, reg_date datetime, textlog text, image binary)", 0);
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
    const char      *sFileName = "data.txt";
    FILE            *sFp       = NULL;
    char             sBuf[1024];
    char            *sToken;
	char			*sContext;
    unsigned int     sCount=0;
    int              j;
	errno_t			 sErr;

    SQL_APPEND_PARAM sParam[11];

    sErr = fopen_s(&sFp, sFileName, "r");
    if( sErr != 0  )
    {
        printf("file open error-%s\n", sFileName);
        return RC_FAILURE;
    }

    printf("append data start\n");

	memset(sParam, 0, sizeof(sParam));
	memset(sBuf, 0, sizeof(sBuf));
		
    while( fgets(sBuf, 1024, sFp ) != NULL )
    {
        if( strlen(sBuf) < 1)
        {
            break;
        }

        j=0;
        sToken = strtok_s(sBuf,",", &sContext);
		
        while( sToken != NULL )
        {
            switch(j){
                case 0 : sParam[j].mShort = atoi(sToken); break;   //short
                case 1 : sParam[j].mInteger = atoi(sToken); break; //int
                case 2 : sParam[j].mLong = atoll(sToken); break;    //long
                case 3 : sParam[j].mFloat = (float)atof(sToken); break;   //float
                case 4 : sParam[j].mDouble = atof(sToken); break;  //double
                case 5 :  //string
                case 9 :  //text
                case 10 : //binary
                    sParam[j].mVar.mLength = (unsigned int)strlen(sToken);
                    sParam[j].mVar.mData = sToken;
                    break;
                case 6 : //ipv4
                case 7 : //ipv6
                    sParam[j].mIP.mLength = SQL_APPEND_IP_STRING;
                    sParam[j].mIP.mAddrString = sToken;
                    break;
                case 8 : //datetime
                    sParam[j].mDateTime.mTime = SQL_APPEND_DATETIME_STRING;
                    sParam[j].mDateTime.mDateStr = sToken;
                    sParam[j].mDateTime.mFormatStr = "DD/MON/YYYY:HH24:MI:SS";
                    break;
				default :
					break;
            }

            sToken = strtok_s(NULL, ",", &sContext);

            j++;
        }

        if( SQLAppendDataV2(aStmt, sParam) != SQL_SUCCESS )
        {
            printError(gEnv, gCon, aStmt, "SQLAppendData Error");
            return RC_FAILURE;
        }

        if ( ((sCount++) % 10000) == 0)
        {
            fprintf(stdout, ".");
            fflush(stdout);
        }

        if( ((sCount) % 100) == 0 )
        {
            if( SQLAppendFlush( aStmt ) != SQL_SUCCESS )
            {
                printError(gEnv, gCon, aStmt, "SQLAppendFlush Error");
            }
        }
    }

    printf("\nappend data end\n");

    fclose(sFp);

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
