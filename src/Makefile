#
# "make all" to build necessary executables.
#

CC=		gcc

LINK=		g++

MY_HOME=	/usr/local/onesql5739

SSL_LIB=	/usr/local/lib64/libssl.a /usr/local/lib64/libcrypto.a

LIBS=		-L${MY_HOME}/lib ${MY_HOME}/lib/libmysqlclient.a ${SSL_LIB} /usr/local/lib/libjemalloc.a -lm -lrt -ldl -lpthread -Wl,-rpath,'$$ORIGIN/lib'

INC=		-I. -I${MY_HOME}/include

#DEFS=		-DDEBUG

CFLAGS=		-w -O3 -g 

TRANSACTIONS=	neword.o payment.o ordstat.o delivery.o slev.o
OBJS=		main.o spt_proc.o driver.o support.o sequence.o rthist.o sb_percentile.o $(TRANSACTIONS)

.SUFFIXES:
.SUFFIXES: .o .c

.c.o:
	$(CC) $(CFLAGS) $(INC) $(DEFS) -c $*.c

all: ../tpcc_load ../tpcc_start

../tpcc_load : load.o support.o
	$(LINK) -static-libgcc -static-libstdc++ load.o support.o $(LIBS) -o ../tpcc_load

../tpcc_start : $(OBJS)
	$(LINK) -static-libgcc -static-libstdc++ $(OBJS) $(LIBS) -o ../tpcc_start

clean :
	rm -f *.o
