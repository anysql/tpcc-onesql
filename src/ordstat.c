/*
 * -*-C-*- 
 * ordstat.pc 
 * corresponds to A.3 in appendix A
 */

#include <string.h>
#include <stdio.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"

extern MYSQL **ctx;

/*
 * the order status transaction
 */
int ordstat( int t_num,
	     int w_id_arg,		/* warehouse id */
	     int d_id_arg,		/* district id */
	     int byname,		/* select by c_id or c_last? */
	     int c_id_arg,		/* customer id */
	     char c_last_arg[]	        /* customer last name, format? */
)
{
	int            w_id = w_id_arg;
	int            d_id = d_id_arg;
	int            c_id = c_id_arg;
	int            c_d_id = d_id;
	int            c_w_id = w_id;
	char            c_first[17];
	char            c_middle[3];
	char            c_last[17];
	float           c_balance;
	int            o_id;
	char            o_entry_d[25];
	int            o_carrier_id;
	int            ol_i_id;
	int            ol_supply_w_id;
	int            ol_quantity;
	float           ol_amount;
	char            ol_delivery_d[25];
	int            namecnt;

	int             n, row_cnt;
	int             proceed = 0;

        int     sqllen;
        char    sqlbuf[SQLBUF_SIZE + 1];
        MYSQL_RES*    myres;
        MYSQL_ROW     myrow;
	unsigned long *mylens;

	/*EXEC SQL WHENEVER NOT FOUND GOTO sqlerr;*/
	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	if (byname) {
		c_last[16] = 0;
		strcpy(c_last, c_last_arg);
		proceed = 1;
		/*EXEC_SQL SELECT count(c_id)
			INTO :namecnt
		        FROM customer
			WHERE c_w_id = :c_w_id
			AND c_d_id = :c_d_id
		        AND c_last = :c_last;*/

	        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
        	                  "SELECT /* tpcc_ordstat_01 */ count(c_id) "
                                  " FROM onesql_customer "
                                  " WHERE c_w_id = %d AND c_d_id = %d AND c_last = '%s'",
                	          c_w_id, c_d_id, c_last);

	        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        	if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
	        if(!(myrow = mysql_fetch_row(myres))) {
        	        mysql_free_result(myres);
                	goto sqlerr;
	        }
        	namecnt = strtol(myrow[0], NULL, 10);
	        mysql_free_result(myres);

		DEBUGTPCCINFO("ordstat -- query byname onesql_coustimer count", 1);

		proceed = 2;
		/*EXEC_SQL DECLARE c_byname_o CURSOR FOR
		        SELECT c_balance, c_first, c_middle, c_last
		        FROM customer
		        WHERE c_w_id = :c_w_id
			AND c_d_id = :c_d_id
			AND c_last = :c_last
			ORDER BY c_first;
		proceed = 3;
		EXEC_SQL OPEN c_byname_o;*/

                sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                                  "SELECT /* tpcc_ordstat_02 */ c_balance, c_first, c_middle, c_last"
				  " FROM onesql_customer WHERE c_w_id = %d AND c_d_id = %d AND c_last = '%s'"
				  " ORDER BY c_first",
                                  c_w_id, c_d_id, c_last);

                if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
                if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;

		if (namecnt % 2)
			namecnt++;	/* Locate midpoint customer; */

		for (n = 0; n < namecnt / 2; n++) {
			proceed = 4;
			/*EXEC_SQL FETCH c_byname_o
			  INTO :c_balance, :c_first, :c_middle, :c_last;*/
			if(!(myrow = mysql_fetch_row(myres))) {
				mysql_free_result(myres);
				goto sqlerr;
			}
			mylens = mysql_fetch_lengths(myres);
			c_balance = strtod(myrow[0], NULL);
			strncpy(c_first, myrow[1], mylens[1] + 1);
			strncpy(c_middle, myrow[2], mylens[2] + 1);
			strncpy(c_last, myrow[3], mylens[3] + 1);
		}
		proceed = 5;
		/*EXEC_SQL CLOSE  c_byname_o;*/
		mysql_free_result(myres);

		DEBUGTPCCINFO("ordstat -- query byname onesql_coustimer middle", namecnt);

	} else {		/* by number */
		proceed = 6;
		/*EXEC_SQL SELECT c_balance, c_first, c_middle, c_last
			INTO :c_balance, :c_first, :c_middle, :c_last
		        FROM customer
		        WHERE c_w_id = :c_w_id
			AND c_d_id = :c_d_id
			AND c_id = :c_id;*/

                sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                                  "SELECT /* tpcc_ordstat_03 */ c_balance, c_first, c_middle, c_last"
                                  " FROM onesql_customer WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d"
                                  " ORDER BY c_first",
                                  c_w_id, c_d_id, c_id);

                if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
                if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
                if(!(myrow = mysql_fetch_row(myres))) {
                        mysql_free_result(myres);
                        goto sqlerr;
                }
		mylens = mysql_fetch_lengths(myres);
		c_balance = strtod(myrow[0], NULL);
		strncpy(c_first, myrow[1], mylens[1] + 1);
		strncpy(c_middle, myrow[2], mylens[2] + 1);
		strncpy(c_last, myrow[3], mylens[3] + 1);
                mysql_free_result(myres);

		DEBUGTPCCINFO("ordstat -- query onesql_coustimer", 1);

	}

	/* find the most recent order for this customer */

	proceed = 7;
	/*EXEC_SQL SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0)
		INTO :o_id, :o_entry_d, :o_carrier_id
	        FROM orders
	        WHERE o_w_id = :c_w_id
		AND o_d_id = :c_d_id
		AND o_c_id = :c_id
		AND o_id = (SELECT MAX(o_id)
		    	    FROM orders
		    	    WHERE o_w_id = :c_w_id
		  	    AND o_d_id = :c_d_id
		    	    AND o_c_id = :c_id);*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                          "SELECT /* tpcc_ordstat_04 */ o_id, o_entry_d, COALESCE(o_carrier_id, 0) "
			  "FROM onesql_orders WHERE o_w_id = %d AND o_d_id = %d AND o_c_id = %d "
			  " AND o_id = (SELECT MAX(o_id) FROM onesql_orders "
			  "    WHERE o_w_id = %d AND o_d_id = %d AND o_c_id = %d)",
                          c_w_id, c_d_id, c_id, c_w_id, c_d_id, c_id);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
        if(!(myrow = mysql_fetch_row(myres))) {
                mysql_free_result(myres);
                goto sqlerr;
        }
	mylens = mysql_fetch_lengths(myres);
        o_id = strtol(myrow[0], NULL, 10);
	strncpy(o_entry_d, myrow[1], mylens[1] + 1);
	o_carrier_id = strtol(myrow[2], NULL, 10);
        mysql_free_result(myres);

	DEBUGTPCCINFO("ordstat -- query onesql_orders latest", 1);

	/* find all the items in this order */
	proceed = 8;
	/*EXEC_SQL DECLARE c_items CURSOR FOR
		SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount,
                       ol_delivery_d
		FROM order_line
	        WHERE ol_w_id = :c_w_id
		AND ol_d_id = :c_d_id
		AND ol_o_id = :o_id;*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                          "SELECT /* tpcc_ordstat_05 */ ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d"
			  " FROM onesql_order_line WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id = %d",
                          c_w_id, c_d_id, o_id);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
	/*proceed = 9;
	EXEC_SQL OPEN c_items;

	EXEC SQL WHENEVER NOT FOUND GOTO done;*/

	row_cnt = 0;
	for (;;) {
		proceed = 10;
		/*EXEC_SQL FETCH c_items
			INTO :ol_i_id, :ol_supply_w_id, :ol_quantity,
			:ol_amount, :ol_delivery_d;*/
	        if(!(myrow = mysql_fetch_row(myres))) break;
		row_cnt++;
	        ol_i_id = strtol(myrow[0], NULL, 10);
        	ol_supply_w_id = strtol(myrow[1], NULL, 10);
	        ol_quantity = strtol(myrow[2], NULL, 10);
        	ol_amount = strtod(myrow[3], NULL);
	}
	mysql_free_result(myres);

done:
	DEBUGTPCCINFO("ordstat -- query onesql_order_line latest", 1);

	/*EXEC_SQL CLOSE c_items;*/
        /*EXEC_SQL COMMIT WORK;*/
	if( mysql_commit(ctx[t_num]) ) goto sqlerr;

	return (1);

sqlerr:
        fprintf(stderr, "ordstat %d:%d\n",t_num,proceed);
	error(ctx[t_num], sqllen, sqlbuf);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);
sqlerrerr:
	return (0);
}

