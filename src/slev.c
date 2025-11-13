/*
 * -*-C-*-
 * slev.pc 
 * corresponds to A.5 in appendix A
 */

#include <string.h>
#include <stdio.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"

extern MYSQL **ctx;

/*
 * the stock level transaction
 */
int slev( int t_num,
	  int w_id_arg,		/* warehouse id */
	  int d_id_arg,		/* district id */
	  int level_arg		/* stock level */
)
{
	int            w_id = w_id_arg;
	int            d_id = d_id_arg;
	int            level = level_arg;
	int            d_next_o_id = 0;
	int            i_count, row_cnt;

        int     sqllen;
        char    sqlbuf[SQLBUF_SIZE + 1];
	MYSQL_RES*    myres;
	MYSQL_ROW     myrow;
	unsigned long *mylens;

	/*EXEC SQL WHENEVER NOT FOUND GOTO sqlerr;*/
	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	/* find the next order id */
#ifdef DEBUG
	printf("select 1\n");
#endif
	/*EXEC_SQL SELECT d_next_o_id
	                INTO :d_next_o_id
	                FROM district
	                WHERE d_id = :d_id
			AND d_w_id = :w_id;*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                          "SELECT /* tpcc_stocklevel_01 */ d_next_o_id"
                          "  FROM onesql_district WHERE d_id = %d and d_w_id = %d",
			  d_id, w_id);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
	if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
	if(!(myrow = mysql_fetch_row(myres))) {
		mysql_free_result(myres);
		goto sqlerr;
	}
	d_next_o_id = strtol(myrow[0], NULL, 10);	
	mysql_free_result(myres);
		
	DEBUGTPCCINFO("stocklevel -- query onesql_district", 1);

	/* find the most recent 20 orders for this district */
	/*EXEC_SQL SELECT count(*) AS low_stock
                   FROM stock
                   WHERE s_w_id = ? AND s_i_id IN
                              (SELECT ol_i_id FROM order_line
                               WHERE ol_d_id = ? AND ol_w_id = ? AND ol_o_id >= ? - 20 AND ol_o_id < ?)
                         AND s_quantity < ?

	EXEC SQL WHENEVER NOT FOUND GOTO done;*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                          "SELECT /* tpcc_stocklevel_02 */ count(*) AS low_stock "
			  "FROM onesql_stock "
			  "WHERE s_w_id = %d AND s_i_id IN "
			  "    (SELECT ol_i_id FROM onesql_order_line "
			  "     WHERE ol_d_id = %d AND ol_w_id = %d AND ol_o_id >= %d AND ol_o_id < %d) "
			  "    AND s_quantity < %d",
                          w_id, d_id, w_id, d_next_o_id - 20, d_next_o_id, level);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
        if(!(myrow = mysql_fetch_row(myres))) {
                mysql_free_result(myres);
                goto sqlerr;
        }
        d_next_o_id = strtol(myrow[0], NULL, 10);
        mysql_free_result(myres);

	DEBUGTPCCINFO("stocklevel -- query low onesql_stock count", 1);

done:
	/*EXEC_SQL CLOSE ord_line;*/
	/*EXEC_SQL COMMIT WORK;*/
	if( mysql_commit(ctx[t_num]) ) goto sqlerr;

	return (1);

sqlerr:
        fprintf(stderr,"slev\n");
	error(ctx[t_num], sqllen, sqlbuf);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);
	return (0);
}
