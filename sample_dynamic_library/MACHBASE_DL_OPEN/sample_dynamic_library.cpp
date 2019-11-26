/******************************************************************************
 * Copyright of this product 2013-2023,
 * MACHBASE Corporation(or Inc.) or its subsidiaries.
 * All Rights reserved.
 ******************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <machbase_sqlcli.h>
#pragma comment(lib, "ws2_32.lib")

#define MACHBASE_PORT_NO    5656
#define ERROR_CHECK_COUNT   100

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
    }                                                           \

typedef struct MachbaseApi
{
    SQLRETURN (__stdcall *mAllocEnv)(SQLHENV*);
    SQLRETURN (__stdcall *mAllocConnect)(SQLHENV,
                                         SQLHDBC*);
    SQLRETURN (__stdcall *mAllocStmt)(SQLHDBC,
                                      SQLHSTMT*);
    SQLRETURN (__stdcall *mFreeConnect)(SQLHDBC);
    SQLRETURN (__stdcall *mFreeEnv)(SQLHENV);
    SQLRETURN (__stdcall *mFreeStmt)(SQLHSTMT,
                                     SQLUSMALLINT);
    SQLRETURN (__stdcall *mDriverConnect)(SQLHDBC,
                                          SQLHWND,
                                          SQLCHAR*,
                                          SQLSMALLINT,
                                          SQLCHAR*,
                                          SQLSMALLINT,
                                          SQLSMALLINT*,
                                          SQLUSMALLINT);
    SQLRETURN (__stdcall *mExecDirect)(SQLHSTMT,
                                       SQLCHAR*,
                                       SQLINTEGER);
    SQLRETURN (__stdcall *mAppendOpen)(SQLHSTMT,
                                       SQLCHAR*,
                                       SQLINTEGER);
    SQLRETURN (__stdcall *mAppendSetErrorCallback)(SQLHSTMT,
                                                   SQLAppendErrorCallback);
    SQLRETURN (__stdcall *mAppendDataV2)(SQLHSTMT,
                                         SQL_APPEND_PARAM*);
    SQLRETURN (__stdcall *mAppendClose)(SQLHSTMT,
                                        SQLBIGINT*,
                                        SQLBIGINT*);
    SQLRETURN (__stdcall *mDisconnect)(SQLHDBC);
} MachbaseApi;

MachbaseApi gMachApi;
HMODULE gModule;

void initMachFunc()
{
    FARPROC sFuncPtr;
    
    gModule = LoadLibrary("machbasecli_dll.dll");

    if(gModule != NULL)
    {
        sFuncPtr = GetProcAddress(gModule, "SQLAllocEnv");
        gMachApi.mAllocEnv = (SQLRETURN (__stdcall*)(SQLHENV*))sFuncPtr;
        
        sFuncPtr = GetProcAddress(gModule, "SQLAllocConnect");
        gMachApi.mAllocConnect = (SQLRETURN (__stdcall*)(SQLHENV, SQLHDBC*))sFuncPtr;
        
        sFuncPtr = GetProcAddress(gModule, "SQLAllocStmt");
        gMachApi.mAllocStmt = (SQLRETURN (__stdcall*)(SQLHDBC, SQLHSTMT*))sFuncPtr;
        
        sFuncPtr = GetProcAddress(gModule, "SQLFreeEnv");
        gMachApi.mFreeEnv = (SQLRETURN (__stdcall*)(SQLHENV))sFuncPtr;

        sFuncPtr = GetProcAddress(gModule, "SQLFreeConnect");
        gMachApi.mFreeConnect = (SQLRETURN (__stdcall*)(SQLHDBC))sFuncPtr;
        
        sFuncPtr = GetProcAddress(gModule, "SQLFreeStmt");
        gMachApi.mFreeStmt = (SQLRETURN (__stdcall*)(SQLHSTMT, SQLUSMALLINT))sFuncPtr;
        
        sFuncPtr = GetProcAddress(gModule, "SQLDriverConnect");
        gMachApi.mDriverConnect = (SQLRETURN (__stdcall*)(SQLHDBC,
                                                          SQLHWND,
                                                          SQLCHAR*,
                                                          SQLSMALLINT,
                                                          SQLCHAR*,
                                                          SQLSMALLINT,
                                                          SQLSMALLINT*,
                                                          SQLUSMALLINT))sFuncPtr;
        
        sFuncPtr = GetProcAddress(gModule, "SQLExecDirect");
        gMachApi.mExecDirect = (SQLRETURN (__stdcall*)(SQLHSTMT, SQLCHAR*, SQLINTEGER))sFuncPtr;
        
        sFuncPtr = GetProcAddress(gModule, "SQLAppendOpen");
        gMachApi.mAppendOpen = (SQLRETURN (__stdcall*)(SQLHSTMT, SQLCHAR*, SQLINTEGER))sFuncPtr;
        
        sFuncPtr = GetProcAddress(gModule, "SQLAppendSetErrorCallback");
        gMachApi.mAppendSetErrorCallback = (SQLRETURN (__stdcall*)(SQLHSTMT, SQLAppendErrorCallback))sFuncPtr;
        
        sFuncPtr = GetProcAddress(gModule, "SQLAppendDataV2");
        gMachApi.mAppendDataV2 = (SQLRETURN (__stdcall*)(SQLHSTMT, SQL_APPEND_PARAM*))sFuncPtr;
        
        sFuncPtr = GetProcAddress(gModule, "SQLAppendClose");
        gMachApi.mAppendClose = (SQLRETURN (__stdcall*)(SQLHSTMT, SQLBIGINT*, SQLBIGINT*))sFuncPtr;
        
        sFuncPtr = GetProcAddress(gModule, "SQLDisconnect");
        gMachApi.mDisconnect = (SQLRETURN (__stdcall*)(SQLHDBC))sFuncPtr;
    }
    else
    {
        printf(".dll open error!");
        exit(1);
    }
}

SQLHENV     gEnv;
SQLHDBC     gCon;

void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char* aMsg);
int connectDB();
void disconnectDB();
int executeDirectSQL(const char* aSQL, int aErrIgnore);
int createTable();
int appendOpen(SQLHSTMT aStmt);
int appendData(SQLHSTMT aStmt);
SQLBIGINT appendClose(SQLHSTMT aStmt);


void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char* aMsg)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if (aMsg != NULL)
    {
        printf("%s\n", aMsg);
    }

    if (SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) == SQL_SUCCESS)
    {
        printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, sNativeError, sErrorMsg);
    }
}

int checkAppendError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if (SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) != SQL_SUCCESS)
    {
        return RC_FAILURE;
    }

    printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, sNativeError, sErrorMsg);

    if (sNativeError != 9604 &&
        sNativeError != 9605 &&
        sNativeError != 9606)
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
    char       sErrMsg[1024] = { 0, };
    char       sRowMsg[32 * 1024] = { 0, };

    UNUSED(aStmt);

    if (aErrorMessage != NULL)
    {
        strncpy(sErrMsg, (char*)aErrorMessage, aErrorBufLen);
    }

    if (aRowBuf != NULL)
    {
        strncpy(sRowMsg, (char*)aRowBuf, aRowBufLen);
    }

    fprintf(stdout, "Append Error : [%d][%s]\n[%s]\n\n", aErrorCode, sErrMsg, sRowMsg);
}

int gettimeofday(struct timeval* tp, int* tz)
{
    LARGE_INTEGER tickNow;
    static LARGE_INTEGER tickFrequency;
    static BOOL tickFrequencySet = FALSE;
    if (tickFrequencySet == FALSE) {
        QueryPerformanceFrequency(&tickFrequency);
        tickFrequencySet = TRUE;
    }
    QueryPerformanceCounter(&tickNow);
    tp->tv_sec = (long)(tickNow.QuadPart / tickFrequency.QuadPart);
    tp->tv_usec = (long)(((tickNow.QuadPart % tickFrequency.QuadPart) * 1000000L) / tickFrequency.QuadPart);

    return 0;
}

time_t getTimeStamp()
{
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
}

int connectDB()
{
    char sConnStr[1024];

    if (gMachApi.mAllocEnv(&gEnv) != SQL_SUCCESS)
    {
        printf("SQLAllocEnv error\n");
        return RC_FAILURE;
    }

    if (gMachApi.mAllocConnect(gEnv, &gCon) != SQL_SUCCESS)
    {
        printf("SQLAllocConnect error\n");

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    sprintf(sConnStr, "SERVER=127.0.0.1;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=%d", MACHBASE_PORT_NO);

    if (gMachApi.mDriverConnect(gCon, NULL,
        (SQLCHAR*)sConnStr,
        SQL_NTS,
        NULL, 0, NULL,
        SQL_DRIVER_NOPROMPT) != SQL_SUCCESS
        )
    {

        printError(gEnv, gCon, NULL, "SQLDriverConnect error");

        gMachApi.mFreeConnect(gCon);
        gCon = SQL_NULL_HDBC;

        gMachApi.mFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void disconnectDB()
{
    if (gMachApi.mDisconnect(gCon) != SQL_SUCCESS)
    {
        printError(gEnv, gCon, NULL, "SQLDisconnect error");
    }

    gMachApi.mFreeConnect(gCon);
    gCon = SQL_NULL_HDBC;

    gMachApi.mFreeEnv(gEnv);
    gEnv = SQL_NULL_HENV;
}

int executeDirectSQL(const char* aSQL, int aErrIgnore)
{
    SQLHSTMT sStmt = SQL_NULL_HSTMT;

    if (gMachApi.mAllocStmt(gCon, &sStmt) != SQL_SUCCESS)
    {
        if (aErrIgnore == 0)
        {
            printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
            return RC_FAILURE;
        }
    }

    if (gMachApi.mExecDirect(sStmt, (SQLCHAR*)aSQL, SQL_NTS) != SQL_SUCCESS)
    {

        if (aErrIgnore == 0)
        {
            printError(gEnv, gCon, sStmt, "SQLExecDirect Error");

            gMachApi.mFreeStmt(sStmt, SQL_DROP);
            sStmt = SQL_NULL_HSTMT;
            return RC_FAILURE;
        }
    }

    if (gMachApi.mFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS)
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
    if (sRC != RC_SUCCESS)
    {
        return RC_FAILURE;
    }

    sRC = executeDirectSQL("CREATE TABLE CLI_SAMPLE(seq short, score integer, total long, percentage float, ratio double, reg_date datetime, id varchar(10), srcip ipv4, dstip ipv6, textlog text, image binary)", 0);
    if (sRC != RC_SUCCESS)
    {
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

int appendOpen(SQLHSTMT aStmt)
{
    const char* sTableName = "CLI_SAMPLE";

    if (gMachApi.mAppendOpen(aStmt, (SQLCHAR*)sTableName, ERROR_CHECK_COUNT) != SQL_SUCCESS)
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

    char             sVarchar[20] = { 0, };
    char             sText[100] = { 0, };
    char             sBinary[100] = { 0, };

    memset(sParam, 0, sizeof(sParam));

    /* NULL FOR ALL*/
    sParam[0].mShort = SQL_APPEND_SHORT_NULL;
    sParam[1].mInteger = SQL_APPEND_INTEGER_NULL;
    sParam[2].mLong = SQL_APPEND_LONG_NULL;
    sParam[3].mFloat = SQL_APPEND_FLOAT_NULL;
    sParam[4].mDouble = SQL_APPEND_DOUBLE_NULL;
    /* datetime */
    sParam[5].mDateTime.mTime = SQL_APPEND_DATETIME_NULL;
    /* varchar */
    sParam[6].mVarchar.mLength = SQL_APPEND_VARCHAR_NULL;
    /* ipv4 */
    sParam[7].mIP.mLength = SQL_APPEND_IP_NULL;
    /* ipv6 */
    sParam[8].mIP.mLength = SQL_APPEND_IP_NULL;
    /* text */
    sParam[9].mText.mLength = SQL_APPEND_TEXT_NULL;
    /* binary */
    sParam[10].mBinary.mLength = SQL_APPEND_BINARY_NULL;
    sRC = gMachApi.mAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /* FIXED COLUMN Value */
    sParam[0].mShort = 2;
    sParam[1].mInteger = 4;
    sParam[2].mLong = 6;
    sParam[3].mFloat = (float)8.4;
    sParam[4].mDouble = 10.9;
    sRC = gMachApi.mAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /*  DATETIME : absolute value */
    sParam[5].mDateTime.mTime = MACHBASE_UINT64_LITERAL(1000000000);
    sRC = gMachApi.mAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /*  DATETIME : current  */
    sParam[5].mDateTime.mTime = SQL_APPEND_DATETIME_NOW;
    sRC = gMachApi.mAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /*  DATETIME : string format*/
    sParam[5].mDateTime.mTime = SQL_APPEND_DATETIME_STRING;
    sParam[5].mDateTime.mDateStr = "23/May/2014:17:41:28";
    sParam[5].mDateTime.mFormatStr = "DD/MON/YYYY:HH24:MI:SS";
    sRC = gMachApi.mAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /*  DATETIME : struct tm format*/
    sParam[5].mDateTime.mTime = SQL_APPEND_DATETIME_STRUCT_TM;
    sParam[5].mDateTime.mTM.tm_year = 2000 - 1900;
    sParam[5].mDateTime.mTM.tm_mon = 11;
    sParam[5].mDateTime.mTM.tm_mday = 31;
    sRC = gMachApi.mAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /*  VARCHAR : string */
    strcpy(sVarchar, "MY VARCHAR");
    sParam[6].mVar.mLength = strlen(sVarchar);
    sParam[6].mVar.mData = sVarchar;
    sRC = gMachApi.mAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /* IPv4 : ipv4 from binary bytes */
    sParam[7].mIP.mLength = SQL_APPEND_IP_IPV4;
    sParam[7].mIP.mAddr[0] = 127;
    sParam[7].mIP.mAddr[1] = 0;
    sParam[7].mIP.mAddr[2] = 0;
    sParam[7].mIP.mAddr[3] = 1;
    sRC = gMachApi.mAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /* IPv4 : ipv4 from binary */
    sParam[7].mIP.mLength = SQL_APPEND_IP_IPV4;
    *(unsigned int*)(sParam[7].mIP.mAddr) = inet_addr("192.168.0.1");
    sRC = gMachApi.mAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /* IPv4 : ipv4 from string */
    sParam[7].mIP.mLength = SQL_APPEND_IP_STRING;
    sParam[7].mIP.mAddrString = "203.212.222.111";
    sRC = gMachApi.mAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /* IPv6 : ipv6 from binary bytes */
    sParam[8].mIP.mLength = SQL_APPEND_IP_IPV6;
    sParam[8].mIP.mAddr[0] = 127;
    sParam[8].mIP.mAddr[1] = 127;
    sParam[8].mIP.mAddr[2] = 127;
    sParam[8].mIP.mAddr[3] = 127;
    sParam[8].mIP.mAddr[4] = 127;
    sParam[8].mIP.mAddr[5] = 127;
    sParam[8].mIP.mAddr[6] = 127;
    sParam[8].mIP.mAddr[7] = 127;
    sParam[8].mIP.mAddr[8] = 127;
    sParam[8].mIP.mAddr[9] = 127;
    sParam[8].mIP.mAddr[10] = 127;
    sParam[8].mIP.mAddr[11] = 127;
    sParam[8].mIP.mAddr[12] = 127;
    sParam[8].mIP.mAddr[13] = 127;
    sParam[8].mIP.mAddr[14] = 127;
    sParam[8].mIP.mAddr[15] = 127;
    sRC = gMachApi.mAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    sParam[8].mIP.mLength = SQL_APPEND_IP_NULL; /* recover */

    /*  TEXT : string */
    memset(sText, 'X', sizeof(sText));
    sParam[9].mVar.mLength = 100;
    sParam[9].mVar.mData = sText;
    sRC = gMachApi.mAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    /*  BINARY : datas */
    memset(sBinary, 0xFB, sizeof(sBinary));
    sParam[10].mVar.mLength = 100;
    sParam[10].mVar.mData = sBinary;
    sRC = gMachApi.mAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

    return RC_SUCCESS;
}

SQLBIGINT appendClose(SQLHSTMT aStmt)
{
    SQLBIGINT sSuccessCount = 0;
    SQLBIGINT sFailureCount = 0;

    if (gMachApi.mAppendClose(aStmt, &sSuccessCount, &sFailureCount) != SQL_SUCCESS)
    {
        printError(gEnv, gCon, aStmt, "SQLAppendClose Error");
        return RC_FAILURE;
    }

    printf("success : %lld, failure : %lld\n", sSuccessCount, sFailureCount);

    return sSuccessCount;
}

int main()
{
    SQLHSTMT    sStmt = SQL_NULL_HSTMT;
    SQLBIGINT   sCount = 0;

    time_t      sStartTime, sEndTime;

    initMachFunc();
    
    if (connectDB() == RC_SUCCESS)
    {
        printf("connectDB success\n");
    }
    else
    {
        printf("connectDB failure\n");
        goto error;
    }

    if (createTable() == RC_SUCCESS)
    {
        printf("createTable success\n");
    }
    else
    {
        printf("createTable failure\n");
        goto error;
    }

    if (gMachApi.mAllocStmt(gCon, &sStmt) != SQL_SUCCESS)
    {
        printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
        goto error;
    }

    if (appendOpen(sStmt) == RC_SUCCESS)
    {
        printf("appendOpen success\n");
    }
    else
    {
        printf("appendOpen failure\n");
        goto error;
    }

    if (gMachApi.mAppendSetErrorCallback(sStmt, appendDumpError) != SQL_SUCCESS)
    {
        printError(gEnv, gCon, sStmt, "SQLAppendSetErrorCallback Error");
        goto error;
    }

    sStartTime = getTimeStamp();
    appendData(sStmt);
    sEndTime = getTimeStamp();

    sCount = appendClose(sStmt);
    if (sCount >= 0)
    {
        printf("appendClose success\n");
        printf("timegap = %lld microseconds for %lld records\n", sEndTime - sStartTime, sCount);
        printf("%.2f records/second\n", ((double)sCount / (double)(sEndTime - sStartTime)) * 1000000);
    }
    else
    {
        printf("appendClose failure\n");
    }

    if (gMachApi.mFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS)
    {
        printError(gEnv, gCon, sStmt, (char*)"SQLFreeStmt Error");
        goto error;
    }
    sStmt = SQL_NULL_HSTMT;

    disconnectDB();

    FreeLibrary(gModule);

    return RC_SUCCESS;

error:
    if (sStmt != SQL_NULL_HSTMT)
    {
        gMachApi.mFreeStmt(sStmt, SQL_DROP);
        sStmt = SQL_NULL_HSTMT;
    }

    if (gCon != SQL_NULL_HDBC)
    {
        disconnectDB();
    }

    FreeLibrary(gModule);

    return RC_FAILURE;
}
