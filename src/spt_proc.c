/*
 * spt_proc.pc
 * support routines for the proc tpcc implementation
 */

#include <mysql.h>

#include <stdio.h>

/*
 * report error
 */
int error(
    MYSQL        *mysql,
      int	 sqllen,
     char	 *sqlbuf
)
{
	if (sqllen > 0) {
		fprintf(stderr, "%.*s\n", sqllen, sqlbuf);
	}
	if(mysql){
	    fprintf(stderr, "%d, %s, %s\n", mysql_errno(mysql), mysql_sqlstate(mysql), mysql_error(mysql) );
	}
	fflush(stderr);
	return (0);
}


