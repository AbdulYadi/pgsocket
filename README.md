# pgsocket
This project is compiled in Linux against PostgreSQL version 10.<br />
$ make clean<br />
$ make<br />
$ make install<br />

On successful compilation, install this extension in PostgreSQL environment<br />
$ create extension pgsocket<br />

Let us send bytes to --for example-- host with IP address nnn.nnn.nnn.nnn, port 9090, send time out 30 seconds, messages "Hello"<br />
$ select pgsocketsend('nnn.nnn.nnn.nnn', 9090, 30, (E'\\\\x' || encode('Hello', 'hex'))::bytea);<br />

Or using address host name instead of IP address<br />
$ select pgsocketsend('thesocketserver', 9090, 30, (E'\\\\x' || encode('Hello', 'hex'))::bytea);<br />

To send message to and read response (started with STX and ended with ETX only) from host, send time out 30 seconds, read time out 40 seconds<br />
$ select pgsocketsendrcvstxetx('thesocketserver', 9090, 30, 40, (E'\\\\x' || encode('Hello', 'hex'))::bytea);<br />
The returned message is in bytea.

