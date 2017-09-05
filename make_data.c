#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_DATA_COUNT	1000000

int main(int argc, char **argv)
{
    int        sMaxData=0;
    int        i;
    FILE      *sFp;
	errno_t    sErr;
    
    if( argc == 2 )
    {
        sMaxData = atoi(argv[1]);
    }
    else
    {
        sMaxData = MAX_DATA_COUNT;
    }

    sErr = fopen_s(&sFp, "data.txt", "w");
    if( sErr != 0 )
    {
        printf("file open error\n");
        exit(0);
    }

    for( i=1; i<=sMaxData; i++ )
    {
        fprintf(sFp, "%d,%d,%ld,%lf,%lf,char-%d,192.168.9.%d,2001:0DB8:0000:0000:0000:0000:1428:%04d,18/May/2015:15:26:%02d,text log-%d,binary image-%d\n", i, i+i, (long)(i+i+i)*10000, (float)(i+2)/(i+i+i)*10000, (double)(i+1)/(i+i+i)*10000,i, i%256, i%9999,i%61, i, i);
    }

    fclose(sFp);
    return 0;
}
