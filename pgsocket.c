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

#define PGSOCKETGETIMAGE_ARGCOUNT 6
#define PGSOCKETGETIMAGE_ARGADDRESS 0
#define PGSOCKETGETIMAGE_ARGPORT 1
#define PGSOCKETGETIMAGE_ARGSENDTIMEOUTSEC 2
#define PGSOCKETGETIMAGE_ARGRCVTIMEOUTSEC 3
#define PGSOCKETGETIMAGE_ARGCOMMAND 4
#define PGSOCKETGETIMAGE_ARGIMAGEID 5

#define INVALID_SOCKET -1

#define STX 0x02
#define ETX 0x03
//#define RECVBUFFSIZE 100
#define RECVBUFFSIZE 102400

PG_FUNCTION_INFO_V1(pgsocketsend);
Datum pgsocketsend(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pgsocketsendrcvstxetx);
Datum pgsocketsendrcvstxetx(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pgsocketgetimage);
Datum pgsocketgetimage(PG_FUNCTION_ARGS);

static int32 pgsocketconfig(const char* address, int32 port, int32 sendtimeout, int32 recvtimeout);
static void sendBytes(int32 hsock, char* buf, int32 len);

Datum
pgsocketsend(PG_FUNCTION_ARGS)
{
	bytea* data;
	char* buf;
	int32 len, hsock;
	if(PG_NARGS()!=PGSOCKETSEND_ARGCOUNT){
		elog(ERROR, "/*argument count must be %d*/", PGSOCKETSEND_ARGCOUNT);
	}	
	if(PG_ARGISNULL(PGSOCKETSEND_ARGADDRESS)){
		elog(ERROR, "/*address must be defined*/");
	}
	if(PG_ARGISNULL(PGSOCKETSEND_ARGPORT)){
		elog(ERROR, "/*port must be defined*/");
	}
	if(PG_ARGISNULL(PGSOCKETSEND_ARGTIMEOUTSEC)){
		elog(ERROR, "/*timeout must be defined*/");
	}
	if(PG_ARGISNULL(PGSOCKETSEND_ARGDATA)){
		elog(ERROR, "/*data must be defined*/");
	}
	hsock = pgsocketconfig(
		TextDatumGetCString(PG_GETARG_DATUM(PGSOCKETSEND_ARGADDRESS)), 
		PG_GETARG_INT32(PGSOCKETSEND_ARGPORT), 
		PG_GETARG_INT32(PGSOCKETSEND_ARGTIMEOUTSEC), 
		-1);
	data = PG_GETARG_BYTEA_PP(PGSOCKETSEND_ARGDATA);
	buf = VARDATA_ANY(data);
	len = VARSIZE_ANY_EXHDR(data);	
	sendBytes(hsock, buf, len);
	
	close(hsock);
	PG_RETURN_VOID();
}

Datum
pgsocketsendrcvstxetx(PG_FUNCTION_ARGS)
{
	bytea* data;
	char* sendbuf;
	int32 len, hsock, bytecount;
	unsigned char* pstart;
	unsigned char* pstop;
	unsigned char recvbuf[RECVBUFFSIZE];
	bytea* byteout;
	StringInfoData bytedata;
	int32 stxfound, etxfound, err;
	
	if(PG_NARGS()!=PGSOCKETRCVSTXETX_ARGCOUNT){
		elog(ERROR, "/*argument count must be %d*/", PGSOCKETRCVSTXETX_ARGCOUNT);
	}	
	if(PG_ARGISNULL(PGSOCKETRCVSTXETX_ARGADDRESS)){
		elog(ERROR, "/*address must be defined*/");
	}
	if(PG_ARGISNULL(PGSOCKETRCVSTXETX_ARGPORT)){
		elog(ERROR, "/*port must be defined*/");
	}
	if(PG_ARGISNULL(PGSOCKETRCVSTXETX_ARGSENDTIMEOUTSEC)){
		elog(ERROR, "/*send timeout must be defined*/");
	}
	if(PG_ARGISNULL(PGSOCKETRCVSTXETX_ARGRCVTIMEOUTSEC)){
		elog(ERROR, "/*receive timeout must be defined*/");
	}
	if(PG_ARGISNULL(PGSOCKETRCVSTXETX_ARGSENDDATA)){
		elog(ERROR, "/*send data must be defined*/");
	}		
	hsock = pgsocketconfig(
		TextDatumGetCString(PG_GETARG_DATUM(PGSOCKETRCVSTXETX_ARGADDRESS)), 
		PG_GETARG_INT32(PGSOCKETRCVSTXETX_ARGPORT), 
		PG_GETARG_INT32(PGSOCKETRCVSTXETX_ARGSENDTIMEOUTSEC), 
		PG_GETARG_INT32(PGSOCKETRCVSTXETX_ARGRCVTIMEOUTSEC));
	data = PG_GETARG_BYTEA_PP(PGSOCKETRCVSTXETX_ARGSENDDATA);
	sendbuf = VARDATA_ANY(data);
	len = VARSIZE_ANY_EXHDR(data);	
	sendBytes(hsock, sendbuf, len);
	
	stxfound=0;
	etxfound=0;
	initStringInfo(&bytedata);	
	while(1) {
		if( (bytecount=read(hsock, recvbuf, RECVBUFFSIZE))==-1 ){
			err = errno;
			close(hsock);
			pfree(bytedata.data);
			elog(ERROR, "/*error receiving data %d*/", err);
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
		elog(ERROR, "/*ETX not found*/");
	}
	
	byteout=(bytea*)palloc(bytedata.len+VARHDRSZ);
	SET_VARSIZE(byteout, bytedata.len+VARHDRSZ);
	memcpy(VARDATA(byteout), bytedata.data, bytedata.len);
	pfree(bytedata.data);	
	PG_RETURN_BYTEA_P(byteout);
}

//(IN t_address text, IN i_port integer, IN i_sendtimeoutsec integer, IN i_recvtimeoutsec integer, IN t_command text, IN i_imageid integer)
Datum
pgsocketgetimage(PG_FUNCTION_ARGS)
{
	int32 hsock, bytecount, totalReadLen, packageLen, err;
	bytea* byteout;
	StringInfoData bytedata;
	char* command;
	uint32_t imageId;
	unsigned char recvbuf[RECVBUFFSIZE];
	
	if(PG_NARGS()!=PGSOCKETGETIMAGE_ARGCOUNT){
		elog(ERROR, "/*argument count must be %d*/", PGSOCKETGETIMAGE_ARGCOUNT);
	}	
	if(PG_ARGISNULL(PGSOCKETGETIMAGE_ARGADDRESS)){
		elog(ERROR, "/*address must be defined*/");
	}
	if(PG_ARGISNULL(PGSOCKETGETIMAGE_ARGPORT)){
		elog(ERROR, "/*port must be defined*/");
	}
	if(PG_ARGISNULL(PGSOCKETGETIMAGE_ARGSENDTIMEOUTSEC)){
		elog(ERROR, "/*send timeout must be defined*/");
	}
	if(PG_ARGISNULL(PGSOCKETGETIMAGE_ARGRCVTIMEOUTSEC)){
		elog(ERROR, "/*receive timeout must be defined*/");
	}
	if(PG_ARGISNULL(PGSOCKETGETIMAGE_ARGCOMMAND)){
		elog(ERROR, "/*command text must be defined*/");
	}
	if(PG_ARGISNULL(PGSOCKETGETIMAGE_ARGIMAGEID)){
		elog(ERROR, "/*image ID must be defined*/");
	}			

	hsock = pgsocketconfig(
		TextDatumGetCString(PG_GETARG_DATUM(PGSOCKETGETIMAGE_ARGADDRESS)), 
		PG_GETARG_INT32(PGSOCKETGETIMAGE_ARGPORT), 
		PG_GETARG_INT32(PGSOCKETGETIMAGE_ARGSENDTIMEOUTSEC), 
		PG_GETARG_INT32(PGSOCKETGETIMAGE_ARGRCVTIMEOUTSEC));
	
	command = TextDatumGetCString(PG_GETARG_DATUM(PGSOCKETGETIMAGE_ARGCOMMAND));
	imageId = (uint32_t)PG_GETARG_INT32(PGSOCKETGETIMAGE_ARGIMAGEID);
	
	initStringInfo(&bytedata);
	packageLen = 4/*packagelen*/ + strlen(command) + 4/*imageId*/;
	elog(NOTICE, "/*imageId %d packageLen %d*/", imageId, packageLen);
	appendBinaryStringInfo(&bytedata, (const char*)&packageLen, 4);
	appendBinaryStringInfo(&bytedata, (const char*)command, strlen(command));
	appendBinaryStringInfo(&bytedata, (const char*)&imageId, 4);
	sendBytes(hsock, bytedata.data, bytedata.len);
	
	resetStringInfo(&bytedata);
	packageLen=0;
	totalReadLen=0;
	while(1) {
		bytecount=read(hsock, recvbuf, RECVBUFFSIZE);		
		if(bytecount==-1) {
			err = errno;
			if (err == EINTR || err == EAGAIN || err == EWOULDBLOCK)
				continue;
			else {
				if(err)
					elog(ERROR, "/*read socket failed %d:%s*/", err, strerror(err));
				break;
			}
		} else if(bytecount==0) {
			elog(ERROR, "/*socket disconnected*/");
			break;//server disconnected
		} else if(bytecount>0) {
			totalReadLen += bytecount;
			appendBinaryStringInfo(&bytedata, (const char*)recvbuf, bytecount);			
			if(packageLen==0 && totalReadLen>=4) {
				packageLen = *((int32*)bytedata.data);
			}
			if(packageLen>0 && totalReadLen>=packageLen)
				break;
		}
	}
	close(hsock);	
	
	byteout=(bytea*)palloc((bytedata.len-4)+VARHDRSZ);
	SET_VARSIZE(byteout, (bytedata.len-4)+VARHDRSZ);
	memcpy(VARDATA(byteout), bytedata.data+4, (bytedata.len-4));
	pfree(bytedata.data);	
	PG_RETURN_BYTEA_P(byteout);	
}

static int32 pgsocketconfig(const char* address, int32 port, int32 sendtimeout, int32 recvtimeout) {
	int32 hsock;
	struct sockaddr_in my_addr;
	int32 err, *p_int;
	struct timeval tv;
	
	hsock = socket(AF_INET, SOCK_STREAM, 0);
	if(hsock==INVALID_SOCKET){
		err = errno;
		elog(ERROR,"/*error initializing socket %d*/",err);
	}	
	
	p_int = (int32*)palloc(sizeof(int32));
	*p_int = 1;		
	if( 	(setsockopt(hsock, SOL_SOCKET, SO_REUSEADDR, (char*)p_int, sizeof(int32)) == -1 )
		|| (setsockopt(hsock, SOL_SOCKET, SO_KEEPALIVE, (char*)p_int, sizeof(int32)) == -1 ) 
	){
		err = errno;
		pfree(p_int);
		close(hsock);
		elog(ERROR,"/*error setting options %d*/",err);
	}
	pfree(p_int);	
	
	if(sendtimeout>-1) {
		tv.tv_sec = sendtimeout;
		tv.tv_usec = 0;		
		if(setsockopt( hsock, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv,  sizeof tv)){
			err = errno;
			close(hsock);
			elog(ERROR,"/*can not set socket send timeout %d*/", err);
		}		
	}
	
	if(recvtimeout>-1) {
		tv.tv_sec = recvtimeout;
		tv.tv_usec = 0;	
		if(setsockopt( hsock, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv,  sizeof tv)){
			err = errno;
			close(hsock);
			elog(ERROR,"/*can not set socket receive timeout %d*/", err);
		}		
	}
	
	my_addr.sin_family = AF_INET;
	my_addr.sin_port = htons(port);	
	memset(&(my_addr.sin_zero), 0, 8);
	inet_pton(AF_INET, address/*IP Address or Name*/, &my_addr.sin_addr.s_addr);

	if( connect( hsock, (struct sockaddr*)&my_addr, sizeof(my_addr)) == -1 ){
		if( (err = errno) != EINPROGRESS ){
			close(hsock);
			elog(ERROR, "/*error connecting socket %d*/", err);
		}
	}	
	
	return hsock;
}

static void sendBytes(int32 hsock, char* buf, int32 len) {
	int32 bytecount, e;
	while(len) {
		bytecount = write(hsock, buf, len);
		if(bytecount==-1) {
			if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
				continue;
			else {
				e = errno;
				elog(ERROR, "/*error sending data %d:%s*/", e, strerror(e));
				break;
			}
		} else if(bytecount==0) {
			elog(ERROR, "/*socket disconnected*/");
			break;//client disconnected
		} else if(bytecount>0)
			len -= bytecount;			
	}
}
