# pgsocket
This project is compiled in Linux against PostgreSQL version 10.
$ make clean
$ make
$ make install

On successful compilation, install this extension in PostgreSQL environment
$ create extension pgsocket

Let us send bytes to --for example-- host with IP address ddd.ddd.ddd.ddd, port 9090, send time out 30 seconds, messages "Hello"
$ select pgsocketsend('ddd.ddd.ddd.ddd', 9090, 30, (E'\\x' || encode('Hello', 'hex'))::bytea);

Or using address host name instead of IP address
$ select pgsocketsend('thesocketserver', 9090, 30, (E'\\x' || encode('Hello', 'hex'))::bytea);
