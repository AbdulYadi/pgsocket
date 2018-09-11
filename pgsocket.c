#include <postgres.h>
#include <fmgr.h>
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif
#include <funcapi.h>
#include <utils/builtins.h>

#include <netinet/in.h>
#include <resolv.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>

#define PGSOCKETSEND_ARGCOUNT 4
#define ARG_ADDRESS 0
#define ARG_PORT 1
#define ARG_TIMEOUTSEC 2
#define ARG_DATA 3

#define INVALID_SOCKET -1

PG_FUNCTION_INFO_V1(pgsocketsend);
Datum pgsocketsend(PG_FUNCTION_ARGS);

Datum
pgsocketsend(PG_FUNCTION_ARGS)
{
	int32 iPort;
	char* sAddress;
	bytea* data;
	char* buf;
	int32 len;
	
	struct timeval tv;		
	int32 hsock;
	int32* p_int;
	struct sockaddr_in my_addr;
	int32 err;
	int32 bytecount;

	if(PG_NARGS()!=PGSOCKETSEND_ARGCOUNT){
		elog(ERROR, "argument count must be %d", PGSOCKETSEND_ARGCOUNT);
	}
	
	if(PG_ARGISNULL(ARG_ADDRESS)){
		elog(ERROR, "address must be defined");
	}
	if(PG_ARGISNULL(ARG_PORT)){
		elog(ERROR, "port must be defined");
	}
	if(PG_ARGISNULL(ARG_TIMEOUTSEC)){
		elog(ERROR, "timeout must be defined");
	}
	if(PG_ARGISNULL(ARG_DATA)){
		elog(ERROR, "data must be defined");
	}
	
	sAddress = TextDatumGetCString(PG_GETARG_DATUM(ARG_ADDRESS));
	iPort = PG_GETARG_INT32(ARG_PORT);
	tv.tv_sec = PG_GETARG_INT32(ARG_TIMEOUTSEC);
	tv.tv_usec = 0;
	data = PG_GETARG_BYTEA_PP(ARG_DATA);
	buf = VARDATA_ANY(data);
	len = VARSIZE_ANY_EXHDR(data);	

	hsock = socket(AF_INET, SOCK_STREAM, 0);
	if(hsock==INVALID_SOCKET){
		elog(ERROR,"Error initializing socket %d",errno);
	}
	
	/*if(setsockopt( hsock, SOL_SOCKET, SO_RCVTIMEO, (char*)&tv,  sizeof tv)){
		close(hsock);
		elog(ERROR,"can not set socket receive timeout %d", errno);
	}*/
	
	if(setsockopt( hsock, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv,  sizeof tv)){
		close(hsock);
		elog(ERROR,"can not set socket send timeout %d", errno);
	}

	p_int = (int32*)palloc(sizeof(int32));
	*p_int = 1;		
	if( 	(setsockopt(hsock, SOL_SOCKET, SO_REUSEADDR, (char*)p_int, sizeof(int32)) == -1 )
			|| (setsockopt(hsock, SOL_SOCKET, SO_KEEPALIVE, (char*)p_int, sizeof(int32)) == -1 ) 
	){
		pfree(p_int);
		close(hsock);
		elog(ERROR,"Error setting options %d",errno);
	}
	pfree(p_int);
	
	my_addr.sin_family = AF_INET;
	my_addr.sin_port = htons(iPort);
	
	memset(&(my_addr.sin_zero), 0, 8);
	inet_pton(AF_INET, sAddress/*IP Address or Name*/, &my_addr.sin_addr.s_addr);
	
	if( connect( hsock, (struct sockaddr*)&my_addr, sizeof(my_addr)) == -1 ){
		if( (err = errno) != EINPROGRESS ){
			close(hsock);
			elog(ERROR, "Error connecting socket %d", errno);
		}
	}

	if( (bytecount=send(hsock, buf, len, 0))==-1 ){
		close(hsock);
		elog(ERROR, "Error sending data %d", errno);
	}
	
	close(hsock);
	
	PG_RETURN_VOID();
}
