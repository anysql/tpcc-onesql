/*
 * -*-C-*-
 * delivery.pc
 * corresponds to A.4 in appendix A
 */

#include <stdio.h>
#include <string.h>
#include <time.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"

extern MYSQL **ctx;

#define NNULL ((void *)0)

struct Delivery_Record {
	int	w_id;
	int	d_id;
	int	o_id;
	int	c_id;
	float	ol_total;
};

int delivery( int t_num,
	      int w_id_arg,
	      int o_carrier_id_arg
)
{
	int            w_id = w_id_arg;
	int            o_carrier_id = o_carrier_id_arg;
        int		d_id;
        int		o_id;
        int		c_id;
        float		ol_total;
	char           datetime[81];

	int tmp, swp, d_count = 0, row_cnt=0;
	struct Delivery_Record d_recs[DIST_PER_WARE];

	int proceed = 0;

        int     sqllen;
        char    sqlbuf[SQLBUF_SIZE + 1];
        MYSQL_RES*    myres;
        MYSQL_ROW     myrow;
	unsigned long *mylens;

	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

        gettimestamp(datetime, STRFTIME_FORMAT, TIMESTAMP_LEN);

	/* For each district in warehouse */
	/* printf("W: %d\n", w_id); */

	for(tmp = 0; tmp < DIST_PER_WARE; tmp++) {
		d_recs[tmp].w_id = INVALID_W_ID;
		d_recs[tmp].d_id = INVALID_D_ID;
		d_recs[tmp].o_id = INVALID_O_ID;
		d_recs[tmp].c_id = INVALID_C_ID;
		d_recs[tmp].ol_total = 0.0f;
	}

        proceed = 1;
	/*EXEC_SQL SELECT no_w_id, no_d_id, no_o_id
          FROM new_orders JOIN ( SELECT ? c1, 1 c2, COALESCE(MIN(no_o_id), 0) c3 FROM new_orders WHERE no_w_id = ? AND no_d_id = 1 UNION ALL
                                 SELECT ? c1, 2 c2, COALESCE(MIN(no_o_id), 0) c3 FROM new_orders WHERE no_w_id = ? AND no_d_id = 2 UNION ALL
				 SELECT ? c1, 3 c2, COALESCE(MIN(no_o_id), 0) c3 FROM new_orders WHERE no_w_id = ? AND no_d_id = 3 UNION ALL
				 SELECT ? c1, 4 c2, COALESCE(MIN(no_o_id), 0) c3 FROM new_orders WHERE no_w_id = ? AND no_d_id = 4 UNION ALL
				 SELECT ? c1, 5 c2, COALESCE(MIN(no_o_id), 0) c3 FROM new_orders WHERE no_w_id = ? AND no_d_id = 5 UNION ALL
				 SELECT ? c1, 6 c2, COALESCE(MIN(no_o_id), 0) c3 FROM new_orders WHERE no_w_id = ? AND no_d_id = 6 UNION ALL
				 SELECT ? c1, 7 c2, COALESCE(MIN(no_o_id), 0) c3 FROM new_orders WHERE no_w_id = ? AND no_d_id = 7 UNION ALL
				 SELECT ? c1, 8 c2, COALESCE(MIN(no_o_id), 0) c3 FROM new_orders WHERE no_w_id = ? AND no_d_id = 8 UNION ALL
				 SELECT ? c1, 9 c2, COALESCE(MIN(no_o_id), 0) c3 FROM new_orders WHERE no_w_id = ? AND no_d_id = 9 UNION ALL
				 SELECT ? c1, 10 c2, COALESCE(MIN(no_o_id), 0) c3 FROM new_orders WHERE no_w_id = ? AND no_d_id = 10) tmp
			ON no_w_id = c1 AND no_d_id = c2 AND no_o_id = c3 FOR UPDATE; */

        sqllen = 0;
        sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
                           "SELECT /* tpcc_delivery_01 */ no_w_id, no_d_id, no_o_id FROM onesql_new_orders JOIN ");
        sqlbuf[sqllen++] = '(';
        for(tmp = 0; tmp < DIST_PER_WARE; tmp++) {
                if (tmp > 0) sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, " UNION ALL ");
                sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
				   "SELECT %d c1, %d c2, COALESCE(MIN(no_o_id), 0) c3 FROM onesql_new_orders WHERE no_w_id = %d AND no_d_id = %d ",
                                   w_id, (tmp + 1), w_id, (tmp + 1));
        }
        sqlbuf[sqllen++] = ')';
	sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
			   " tmp ON no_w_id = c1 AND no_d_id = c2 AND no_o_id = c3 FOR UPDATE");

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
	d_count = 0;
        while((myrow = mysql_fetch_row(myres)) != NULL) {
		d_recs[d_count].w_id = strtol(myrow[0], NULL, 10);
		d_recs[d_count].d_id = strtol(myrow[1], NULL, 10);
		d_recs[d_count].o_id = strtol(myrow[2], NULL, 10);
		d_count++;
        }
        mysql_free_result(myres);

	DEBUGTPCCINFO("delivery -- query onesql_new_orders for update", d_count);

	if (d_count > 0) {
		proceed = 2;
		/*EXEC_SQL DELETE FROM new_orders
		  WHERE (no_w_id, no_d_id, no_o_id) in ((?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?),
							(?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)); */

		sqllen = 0;
	        sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
				   "DELETE /* tpcc_delivery_02 */ FROM onesql_new_orders WHERE (no_w_id, no_d_id, no_o_id) in ");
		sqlbuf[sqllen++] = '(';
		for(tmp = 0; tmp < d_count; tmp++) {
			if (tmp > 0) sqlbuf[sqllen++] = ',';
			if (tmp < d_count) {
				sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, "(%d, %d, %d)",
						   d_recs[tmp].w_id, d_recs[tmp].d_id, d_recs[tmp].o_id);
			} else {
				sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, "(%d, %d, %d)",
						   INVALID_W_ID, INVALID_D_ID, INVALID_O_ID);
			}
		}
		sqlbuf[sqllen++] = ')';

	        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

		DEBUGTPCCINFO("delivery -- delete onesql_new_orders", mysql_affected_rows(ctx[t_num]));

		proceed = 3;
		/*EXEC_SQL SELECT o_w_id, o_d_id, o_id, o_c_id, SUM(ol_amount) ol_amount
			  FROM orders JOIN order_line ON o_w_id = ol_w_id AND o_d_id = ol_d_id AND o_id = ol_o_id
			  WHERE (o_w_id, o_d_id, o_id) in ((?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?),
							   (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?))
			  GROUP BY o_w_id, o_d_id, o_id, o_c_id; */

	        sqllen = 0;
        	sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
                	           "SELECT /* tpcc_delivery_03 */ o_w_id, o_d_id, o_id, o_c_id, SUM(ol_amount) ol_amount");
		sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
				   "  FROM onesql_orders JOIN onesql_order_line ON o_w_id = ol_w_id AND o_d_id = ol_d_id AND o_id = ol_o_id");
		sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
				   "  WHERE (o_w_id, o_d_id, o_id) in ");
	        sqlbuf[sqllen++] = '(';
        	for(tmp = 0; tmp < d_count; tmp++) {
	                if (tmp > 0) sqlbuf[sqllen++] = ',';
			if (tmp < d_count) {
	        	        sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, " (%d, %d, %d)",
						   d_recs[tmp].w_id, d_recs[tmp].d_id, d_recs[tmp].o_id);
			} else {
				sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, " (%d, %d, %d)",
						   INVALID_W_ID, INVALID_D_ID, INVALID_O_ID);
			}
	        }
        	sqlbuf[sqllen++] = ')';
	        sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
        	                   "  GROUP BY o_w_id, o_d_id, o_id, o_c_id");

	        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        	if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
		row_cnt = 0;
	        while((myrow = mysql_fetch_row(myres)) != NULL) {
			row_cnt ++;
        	        w_id = strtol(myrow[0], NULL, 10);
                	d_id = strtol(myrow[1], NULL, 10);
	                o_id = strtol(myrow[2], NULL, 10);
			c_id = strtol(myrow[3], NULL, 10);
			ol_total = strtod(myrow[4], NULL);
                        for(swp = 0; swp < d_count; swp++) {
                                if (d_recs[swp].w_id == w_id &&
                                    d_recs[swp].d_id == d_id &&
                                    d_recs[swp].o_id == o_id) {
                                        d_recs[swp].c_id = c_id;
                                        d_recs[swp].ol_total = ol_total;
                                }
                        }
	        }
        	mysql_free_result(myres);

		DEBUGTPCCINFO("delivery -- query onesql_orders join onesql_order_line", row_cnt);

		proceed = 4;
		/*EXEC_SQL UPDATE (SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4) tmp JOIN orders
			ON o_w_id = c1 and o_d_id = c2 AND o_id = c3
			SET o_carrier_id = c4; */

                sqllen = 0;
                sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, "UPDATE /* tpcc_delivery_04 */ ");
                sqlbuf[sqllen++] = '(';
                for(tmp = 0; tmp < d_count; tmp++) {
                        if (tmp > 0) {
				sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, " UNION ALL ");
			}
			if (tmp < d_count) {
	                        sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
						   " SELECT %d c1, %d c2, %d c3 ",
						   d_recs[tmp].w_id, d_recs[tmp].d_id, d_recs[tmp].o_id);
			} else {
				sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
						   " SELECT %d c1, %d c2, %d c3 ",
						   INVALID_W_ID, INVALID_D_ID, INVALID_O_ID);
			}
                }
                sqlbuf[sqllen++] = ')';
		sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
				   " tmp JOIN onesql_orders "
				   " ON o_w_id = c1 and o_d_id = c2 AND o_id = c3 "
				   " SET o_carrier_id = %d", o_carrier_id);

                if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

		DEBUGTPCCINFO("delivery -- update onesql_orders", mysql_affected_rows(ctx[t_num]));

		proceed = 5;
		/*EXEC_SQL UPDATE order_line SET ol_delivery_d = ?
			WHERE (ol_w_id, ol_d_id, ol_o_id)
			   IN ((?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?),
			       (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)); */

                sqllen = 0;
                sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
                                   "UPDATE /* tpcc_delivery_05 */ onesql_order_line SET ol_delivery_d = '%s'"
				   "   WHERE (ol_w_id, ol_d_id, ol_o_id) in ", datetime);
                sqlbuf[sqllen++] = '(';
                for(tmp = 0; tmp < d_count; tmp++) {
                        if (tmp > 0) sqlbuf[sqllen++] = ',';
			if (tmp < d_count) {
	                        sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, " (%d, %d, %d)",
        	                                   d_recs[tmp].w_id, d_recs[tmp].d_id, d_recs[tmp].o_id);
			} else {
				sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, " (%d, %d, %d)",
						   INVALID_W_ID, INVALID_D_ID, INVALID_O_ID);
			}
                }
                sqlbuf[sqllen++] = ')';

		if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

		DEBUGTPCCINFO("delivery -- update onesql_order_line", mysql_affected_rows(ctx[t_num]));

		proceed = 7;
		/*EXEC_SQL UPDATE (SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4 UNION ALL
				   SELECT ? c1, ? c2, ? c3, ? c4) tmp JOIN customer
			ON c_w_id = c1 and c_d_id = c2 AND c_id = c3
			SET c_balance = c_balance + c4, c_delivery_cnt = c_delivery_cnt + 1; */

                sqllen = 0;
                sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, "UPDATE /* tpcc_delivery_06 */ ");
                sqlbuf[sqllen++] = '(';
                for(tmp = 0; tmp < d_count; tmp++) {
                        if (tmp > 0) {
                                sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, " UNION ALL ");
                        }
			if (tmp < d_count) {
	                        sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
        	                                   " SELECT %d c1, %d c2, %d c3, %.2f c4",
                	                           d_recs[tmp].w_id, d_recs[tmp].d_id, d_recs[tmp].c_id, d_recs[tmp].ol_total);
			} else {
				sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
						   " SELECT %d c1, %d c2, %d c3, %.2f c4",
						   INVALID_W_ID, INVALID_D_ID, INVALID_C_ID, 0.0f);
			}
                }
                sqlbuf[sqllen++] = ')';
                sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
                                   " tmp JOIN onesql_customer "
                                   " ON c_w_id = c1 and c_d_id = c2 AND c_id = c3 "
                                   " SET c_balance = c_balance + c4, c_delivery_cnt = c_delivery_cnt + 1");

                if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

		DEBUGTPCCINFO("delivery -- update onesql_customer", mysql_affected_rows(ctx[t_num]));
	}

	/*EXEC_SQL COMMIT WORK;*/
	if( mysql_commit(ctx[t_num]) ) goto sqlerr;
	return (1);

sqlerr:
        fprintf(stderr, "delivery %d:%d\n",t_num,proceed);
	error(ctx[t_num], sqllen, sqlbuf);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);
sqlerrerr:
	return (0);
}
