#include <postgres.h>
#include <fmgr.h>
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif
#include <funcapi.h>
#include <utils/builtins.h>
#include <lib/stringinfo.h> 

#include <netinet/in.h>
#include <resolv.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>

#define PGSOCKETSEND_ARGCOUNT 4
#define PGSOCKETSEND_ARGADDRESS 0
#define PGSOCKETSEND_ARGPORT 1
#define PGSOCKETSEND_ARGTIMEOUTSEC 2
#define PGSOCKETSEND_ARGDATA 3

#define PGSOCKETRCVSTXETX_ARGCOUNT 5
#define PGSOCKETRCVSTXETX_ARGADDRESS 0
#define PGSOCKETRCVSTXETX_ARGPORT 1
#define PGSOCKETRCVSTXETX_ARGSENDTIMEOUTSEC 2
#define PGSOCKETRCVSTXETX_ARGRCVTIMEOUTSEC 3
#define PGSOCKETRCVSTXETX_ARGSENDDATA 4

#define INVALID_SOCKET -1

#define STX 0x02
#define ETX 0x03
#define RECVBUFFSIZE 100

PG_FUNCTION_INFO_V1(pgsocketsend);
Datum pgsocketsend(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pgsocketsendrcvstxetx);
Datum pgsocketsendrcvstxetx(PG_FUNCTION_ARGS);

static int32 pgsocketconfig(const char* address, int32 port, int32 sendtimeout, int32 recvtimeout);

Datum
pgsocketsend(PG_FUNCTION_ARGS)
{
	bytea* data;
	char* buf;
	int32 len;	
	int32 hsock;
	int32 bytecount;
	if(PG_NARGS()!=PGSOCKETSEND_ARGCOUNT){
		elog(ERROR, "argument count must be %d", PGSOCKETSEND_ARGCOUNT);
	}	
	if(PG_ARGISNULL(PGSOCKETSEND_ARGADDRESS)){
		elog(ERROR, "address must be defined");
	}
	if(PG_ARGISNULL(PGSOCKETSEND_ARGPORT)){
		elog(ERROR, "port must be defined");
	}
	if(PG_ARGISNULL(PGSOCKETSEND_ARGTIMEOUTSEC)){
		elog(ERROR, "timeout must be defined");
	}
	if(PG_ARGISNULL(PGSOCKETSEND_ARGDATA)){
		elog(ERROR, "data must be defined");
	}
	hsock = pgsocketconfig(
		TextDatumGetCString(PG_GETARG_DATUM(PGSOCKETSEND_ARGADDRESS)), 
		PG_GETARG_INT32(PGSOCKETSEND_ARGPORT), 
		PG_GETARG_INT32(PGSOCKETSEND_ARGTIMEOUTSEC), 
		-1);
	data = PG_GETARG_BYTEA_PP(PGSOCKETSEND_ARGDATA);
	buf = VARDATA_ANY(data);
	len = VARSIZE_ANY_EXHDR(data);	
	if( (bytecount=send(hsock, buf, len, 0))==-1 ){
		close(hsock);
		elog(ERROR, "Error sending data %d", errno);
	}	
	close(hsock);
	PG_RETURN_VOID();
}

Datum
pgsocketsendrcvstxetx(PG_FUNCTION_ARGS)
{
	bytea* data;
	char* sendbuf;
	int32 len;	
	int32 hsock;
	int32 bytecount;
	unsigned char* pstart;
	unsigned char* pstop;
	unsigned char recvbuf[RECVBUFFSIZE];
	bytea* byteout;
	StringInfoData bytedata;
	int32 stxfound;
	int32 etxfound;
	
	if(PG_NARGS()!=PGSOCKETRCVSTXETX_ARGCOUNT){
		elog(ERROR, "argument count must be %d", PGSOCKETRCVSTXETX_ARGCOUNT);
	}	
	if(PG_ARGISNULL(PGSOCKETRCVSTXETX_ARGADDRESS)){
		elog(ERROR, "address must be defined");
	}
	if(PG_ARGISNULL(PGSOCKETRCVSTXETX_ARGPORT)){
		elog(ERROR, "port must be defined");
	}
	if(PG_ARGISNULL(PGSOCKETRCVSTXETX_ARGSENDTIMEOUTSEC)){
		elog(ERROR, "send timeout must be defined");
	}
	if(PG_ARGISNULL(PGSOCKETRCVSTXETX_ARGRCVTIMEOUTSEC)){
		elog(ERROR, "receive timeout must be defined");
	}
	if(PG_ARGISNULL(PGSOCKETRCVSTXETX_ARGSENDDATA)){
		elog(ERROR, "send data must be defined");
	}		
	hsock = pgsocketconfig(
		TextDatumGetCString(PG_GETARG_DATUM(PGSOCKETRCVSTXETX_ARGADDRESS)), 
		PG_GETARG_INT32(PGSOCKETRCVSTXETX_ARGPORT), 
		PG_GETARG_INT32(PGSOCKETRCVSTXETX_ARGSENDTIMEOUTSEC), 
		PG_GETARG_INT32(PGSOCKETRCVSTXETX_ARGRCVTIMEOUTSEC));
	data = PG_GETARG_BYTEA_PP(PGSOCKETRCVSTXETX_ARGSENDDATA);
	sendbuf = VARDATA_ANY(data);
	len = VARSIZE_ANY_EXHDR(data);	
	if( (bytecount=send(hsock, sendbuf, len, 0))==-1 ){
		close(hsock);
		elog(ERROR, "Error sending data %d", errno);
	}	
	
	stxfound=0;
	etxfound=0;
	initStringInfo(&bytedata);	
	while(1) {
		if( (bytecount=read(hsock, recvbuf, RECVBUFFSIZE))==-1 ){
			close(hsock);
			pfree(bytedata.data);
			elog(ERROR, "Error receiving data %d", errno);
		}	
		if(bytecount<1) {
			break;
		}
		
		if(!stxfound) {
			pstart = (unsigned char*)memchr(recvbuf, STX, bytecount); 
			if(pstart!=NULL) {
				stxfound=1;
				pstart++;				
				bytecount -= (pstart - recvbuf);
			}			
		} else {
			pstart = recvbuf;
		}
		
		if(stxfound && bytecount>0) {
			pstop = (unsigned char*)memchr(pstart, ETX, bytecount); 
			if(pstop!=NULL) {
				pstop--;
				etxfound = 1;
				bytecount = 1 + (pstop - pstart);
			}	
			if(bytecount>0) {
				appendBinaryStringInfo(&bytedata, (const char*)pstart, bytecount);			
			}						
			if(etxfound) {
				break;
			}
		}
	}
	close(hsock);
	if(!etxfound) {		
		pfree(bytedata.data);
		elog(ERROR, "ETX not found");
	}
	
	byteout=(bytea*)palloc(bytedata.len+VARHDRSZ);
	SET_VARSIZE(byteout, bytedata.len+VARHDRSZ);
	memcpy(VARDATA(byteout), bytedata.data, bytedata.len);
	pfree(bytedata.data);	
	PG_RETURN_BYTEA_P(byteout);
}

static int32 pgsocketconfig(const char* address, int32 port, int32 sendtimeout, int32 recvtimeout) {
	int32 hsock;
	struct sockaddr_in my_addr;
	int32* p_int;
	struct timeval tv;
	int32 err;
	
	hsock = socket(AF_INET, SOCK_STREAM, 0);
	if(hsock==INVALID_SOCKET){
		elog(ERROR,"Error initializing socket %d",errno);
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
	
	if(sendtimeout>-1) {
		tv.tv_sec = sendtimeout;
		tv.tv_usec = 0;		
		if(setsockopt( hsock, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv,  sizeof tv)){
			close(hsock);
			elog(ERROR,"can not set socket send timeout %d", errno);
		}		
	}
	
	if(recvtimeout>-1) {
		tv.tv_sec = recvtimeout;
		tv.tv_usec = 0;	
		if(setsockopt( hsock, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv,  sizeof tv)){
			close(hsock);
			elog(ERROR,"can not set socket receive timeout %d", errno);
		}		
	}
	
	my_addr.sin_family = AF_INET;
	my_addr.sin_port = htons(port);	
	memset(&(my_addr.sin_zero), 0, 8);
	inet_pton(AF_INET, address/*IP Address or Name*/, &my_addr.sin_addr.s_addr);

	if( connect( hsock, (struct sockaddr*)&my_addr, sizeof(my_addr)) == -1 ){
		if( (err = errno) != EINPROGRESS ){
			close(hsock);
			elog(ERROR, "Error connecting socket %d", errno);
		}
	}	
	
	return hsock;
}