/*
 * -*-C-*- 
 * neword.pc 
 * corresponds to A.1 in appendix A
 */

#include <stdio.h>
#include <string.h>
#include <time.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"

extern MYSQL **ctx;

extern FILE *ftrx_file;

#define NNULL ((void *)0)

struct order_line_record {
        int o_id;
        int d_id;
        int w_id;
        int ol_number;
        int ol_i_id;
        int ol_supply_w_id;
        int ol_quantity;
        float ol_amount;
        char ol_dist_info[25];
};

struct stock_record {
        int            s_quantity;
        char            s_dist[25];
};

/*
 * the new order transaction
 */
int neword( int t_num,
	    int w_id_arg,		/* warehouse id */
	    int d_id_arg,		/* district id */
	    int c_id_arg,		/* customer id */
	    int o_ol_cnt_arg,	        /* number of items */
	    int o_all_local_arg,	/* are all order lines local */
	    int itemid[],		/* ids of items to be ordered */
	    int supware[],		/* warehouses supplying items */
	    int qty[]		        /* quantity of each item */
)
{

	int            w_id = w_id_arg;
	int            d_id = d_id_arg;
	int            c_id = c_id_arg;
	int            o_ol_cnt = o_ol_cnt_arg;
	int            o_all_local = o_all_local_arg;
	float           c_discount;
	char            c_last[17];
	char            c_credit[3];
	float           w_tax;
	int            d_next_o_id;
	float           d_tax;
	char            datetime[81];
	int            o_id;
	float           i_price;
	int            ol_i_id;
	int            s_quantity;
	int            ol_supply_w_id;
	float           ol_amount;
	int            ol_number;
	int            ol_quantity;

	float           price[MAX_NUM_ITEMS];
	int            stock[MAX_NUM_ITEMS];

	int            min_num;
	int            i,j,tmp,swp,row_cnt;
	int            ol_num_seq[MAX_NUM_ITEMS];

	struct order_line_record ol_rec[MAX_NUM_ITEMS];
	struct stock_record st_rec[MAX_NUM_ITEMS];

	int		notfound_i_id = INVALID_I_ID;
	int		notfound_w_id = INVALID_W_ID;
	int             proceed = 0;
 	struct timespec tbuf1,tbuf_start;
	clock_t clk1,clk_start;	


	int	sqllen;
	char	sqlbuf[SQLBUF_SIZE + 1];
        MYSQL_RES*    myres;
        MYSQL_ROW     myrow;
	unsigned long *mylens;

	/* EXEC SQL WHENEVER NOT FOUND GOTO sqlerr;*/
	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	/*EXEC SQL CONTEXT USE :ctx[t_num];*/

        gettimestamp(datetime, STRFTIME_FORMAT, TIMESTAMP_LEN);
	clk_start = clock_gettime(CLOCK_REALTIME, &tbuf_start );

        /* sort orders to avoid DeadLock */
        for (i = 0; i < o_ol_cnt; i++) {
                ol_num_seq[i]=i;
        }
        for (i = 0; i < (o_ol_cnt - 1); i++) {
                tmp = (MAXITEMS + 1) * supware[ol_num_seq[i]] + itemid[ol_num_seq[i]];
                min_num = i;
                for ( j = i+1; j < o_ol_cnt; j++) {
                  if ( (MAXITEMS + 1) * supware[ol_num_seq[j]] + itemid[ol_num_seq[j]] < tmp ){
                    tmp = (MAXITEMS + 1) * supware[ol_num_seq[j]] + itemid[ol_num_seq[j]];
                    min_num = j;
                  }
                }
                if ( min_num != i ){
                  swp = ol_num_seq[min_num];
                  ol_num_seq[min_num] = ol_num_seq[i];
                  ol_num_seq[i] = swp;
                }
        }

	proceed = 1;
	/*EXEC_SQL SELECT c_discount, c_last, c_credit, w_tax
		INTO :c_discount, :c_last, :c_credit, :w_tax
	        FROM customer, warehouse
	        WHERE w_id = :w_id 
		AND c_w_id = w_id 
		AND c_d_id = :d_id 
		AND c_id = :c_id;*/


        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                          "SELECT /* tpcc_neword_01 */ c_discount, c_last, c_credit, w_tax"
			  "  FROM onesql_customer JOIN onesql_warehouse ON w_id = c_w_id"
			  "  WHERE w_id = %d AND c_w_id = %d AND c_d_id = %d AND c_id = %d",
                          w_id, w_id, d_id, c_id);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
        if(!(myrow = mysql_fetch_row(myres))) {
                mysql_free_result(myres);
                goto sqlerr;
        }
	mylens = mysql_fetch_lengths(myres);
	c_discount = strtod(myrow[0], NULL);
	strncpy(c_last, myrow[1], mylens[1] + 1);
	strncpy(c_credit, myrow[2], mylens[2] + 1);
	w_tax = strtod(myrow[3], NULL);
        mysql_free_result(myres);

	DEBUGTPCCINFO("neword -- query onesql_customer join onesql_warehouse", 1);

#ifdef DEBUG
	printf("n %d\n",proceed);
#endif

	/* EXEC SQL WHENEVER NOT FOUND GOTO invaliditem; */
	proceed = 2;
	/*EXEC_SQL SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax
	        FROM district
	        WHERE d_id = :d_id
		AND d_w_id = :w_id
		FOR UPDATE;*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                          "SELECT /* tpcc_neword_02 */ d_next_o_id, d_tax"
                          "  FROM onesql_district"
                          "  WHERE d_id = %d AND d_w_id = %d FOR UPDATE",
                          d_id, w_id);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
        if(!(myrow = mysql_fetch_row(myres))) {
                mysql_free_result(myres);
                goto sqlerr;
        }
	d_next_o_id = strtol(myrow[0], NULL, 10);
	d_tax = strtod(myrow[1], NULL);
        mysql_free_result(myres);

	DEBUGTPCCINFO("neword -- query onesql_district for update", 1);

	proceed = 3;
	/*EXEC_SQL UPDATE district SET d_next_o_id = :d_next_o_id + 1
	        WHERE d_id = :d_id 
		AND d_w_id = :w_id;*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE - 1,
			 "UPDATE /* tpcc_neword_03 */ onesql_district SET d_next_o_id = %d WHERE d_id = %d AND d_w_id = %d",
			 d_next_o_id + 1, d_id, w_id);

	if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

	DEBUGTPCCINFO("neword -- update onesql_district", mysql_affected_rows(ctx[t_num]));

	o_id = d_next_o_id;

#ifdef DEBUG
	printf("n %d\n",proceed);
#endif

	proceed = 4;
	/*EXEC_SQL INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,
			             o_entry_d, o_ol_cnt, o_all_local)
		VALUES(:o_id, :d_id, :w_id, :c_id, 
		       :datetime, :o_ol_cnt, :o_all_local);*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE - 1,
                         "INSERT /* tpcc_neword_04 */ INTO onesql_orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)"
			 "  VALUES (%d, %d, %d, %d, '%s', %d, %d)",
                         o_id, d_id, w_id, c_id, datetime, o_ol_cnt, o_all_local);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

	DEBUGTPCCINFO("neword -- insert onesql_orders", mysql_affected_rows(ctx[t_num]));

#ifdef DEBUG
	printf("n %d\n",proceed);
#endif

	proceed = 5;
	/* EXEC_SQL INSERT INTO new_orders (no_o_id, no_d_id, no_w_id)
	   VALUES (:o_id,:d_id,:w_id); */

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE - 1,
                         "INSERT /* tpcc_neword_05 */ INTO onesql_new_orders (no_o_id, no_d_id, no_w_id) VALUES (%d, %d, %d)",
                         o_id, d_id, w_id);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

	DEBUGTPCCINFO("neword -- insert onesql_new_orders", mysql_affected_rows(ctx[t_num]));

        /* EXEC SQL WHENEVER NOT FOUND GOTO invaliditem; */
        proceed = 6;
        /*EXEC_SQL SELECT s_w_id, s_i_id, s_quantity, s_data,
			  s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
			  s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
		    INTO  :st_rec....
                FROM stock
                WHERE ((s_w_id, s_i_id) in ((?, ?), ...) FOR UPDATE; */

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                          "SELECT /* tpcc_neword_06 */ s_w_id, s_i_id, s_quantity, s_data, s_dist_%02d"
                          "  FROM onesql_stock WHERE (s_w_id, s_i_id) IN ", d_id);
	sqlbuf[sqllen++] = '(';
	for (ol_number = 0; ol_number < o_ol_cnt; ol_number++) {
		if (ol_number> 0) sqlbuf[sqllen++] = ',';
		if (ol_number < o_ol_cnt) {
			sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, " (%d, %d)",
					   supware[ol_num_seq[ol_number]], itemid[ol_num_seq[ol_number]]);
		} else {
			sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, " (%d, %d)",
					   INVALID_W_ID, INVALID_I_ID);
		}
	}
	sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, ") FOR UPDATE");

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
	row_cnt = 0;
        while((myrow = mysql_fetch_row(myres)) != NULL) {
		row_cnt++;
		mylens = mysql_fetch_lengths(myres);
	        ol_supply_w_id = strtol(myrow[0], NULL, 10);
		ol_i_id = strtol(myrow[1], NULL, 10);
		for(j = 0; j < o_ol_cnt; j++) {
			if (supware[j] == ol_supply_w_id && ol_i_id == itemid[j]) {
                                st_rec[j].s_quantity = strtol(myrow[2], NULL, 10);
				strncpy(st_rec[j].s_dist, myrow[4], mylens[4] + 1);
			}
		}
	}
        mysql_free_result(myres);

	DEBUGTPCCINFO("neword -- query onesql_stock for update", row_cnt);

        /* EXEC SQL WHENEVER NOT FOUND GOTO invaliditem; */
        proceed = 7;
        /*EXEC_SQL SELECT i_price, i_name, i_data
                INTO :i_price, :i_name, :i_data
                FROM item
                WHERE i_id in (:ol_i_id, ...);*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                          "SELECT /* tpcc_neword_07 */ i_id, i_price, i_name, i_data"
                          "  FROM onesql_item"
                          "  WHERE i_id in (");
        for(i = 0; i < o_ol_cnt; i++) {
                if (i > 0) sqlbuf[sqllen++] = ',';
                if (i < o_ol_cnt) {
                        sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, " %d", itemid[i]);
                } else {
                        sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, " %d", INVALID_I_ID);
                }
        }
        sqlbuf[sqllen++] = ')';

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
        swp = 0;
        row_cnt = 0;
        while((myrow = mysql_fetch_row(myres)) != NULL) {
                row_cnt ++;
                mylens = mysql_fetch_lengths(myres);
                ol_i_id = strtol(myrow[0], NULL, 10);
                for(j = 0; j < o_ol_cnt; j++) {
                        if (ol_i_id == itemid[j]) {
                                price[j] = strtod(myrow[1], NULL);;
                                swp++;
                        }
                }
        }
        mysql_free_result(myres);

        DEBUGTPCCINFO("neword -- query onesql_item", row_cnt);

        if (swp != o_ol_cnt) {
                goto invaliditem;
        }

	for (ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
		ol_supply_w_id = supware[ol_num_seq[ol_number - 1]];
		ol_i_id = itemid[ol_num_seq[ol_number - 1]];
		ol_quantity = qty[ol_num_seq[ol_number - 1]];
		i_price = price[ol_num_seq[ol_number - 1]];

		/* EXEC SQL WHENEVER NOT FOUND GOTO sqlerr; */

                s_quantity = st_rec[ol_num_seq[ol_number - 1]].s_quantity;

		stock[ol_num_seq[ol_number - 1]] = s_quantity;

		if (s_quantity > ol_quantity)
			s_quantity = s_quantity - ol_quantity;
		else
			s_quantity = s_quantity - ol_quantity + 91;

		st_rec[ol_num_seq[ol_number - 1]].s_quantity = s_quantity;

#ifdef DEBUG
		printf("n %d\n",proceed);
#endif

		ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);

#ifdef DEBUG
		printf("n %d\n",proceed);
#endif

		ol_rec[ol_number - 1].o_id = o_id;
		ol_rec[ol_number - 1].d_id = d_id;
		ol_rec[ol_number - 1].w_id = w_id;
		ol_rec[ol_number - 1].ol_number = ol_number;
		ol_rec[ol_number - 1].ol_i_id = ol_i_id;
		ol_rec[ol_number - 1].ol_supply_w_id = ol_supply_w_id;
		ol_rec[ol_number - 1].ol_quantity = ol_quantity;
		ol_rec[ol_number - 1].ol_amount = ol_amount;
		strncpy(ol_rec[ol_number - 1].ol_dist_info, st_rec[ol_num_seq[ol_number - 1]].s_dist, 25);
	}


        proceed = 8;
        /*EXEC_SQL UPDATE (select ? c1, ? c2, ? c3 UNION ALL
			   select ? c1, ? c2, ? c3 UNION ALL
			   ......
			   select ? c1, ? c2, ? c3) tmp
		JOIN stock ON s_w_id = c1 and s_i_id = c2
		SET s_quantity = c3 ;*/

	sqllen = 0;
        sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, "UPDATE /* tpcc_neword_08 */ (");
	for (ol_number = 0; ol_number < o_ol_cnt; ol_number++) {
		if (ol_number > 0) sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen, " UNION ALL ");
		if (ol_number < o_ol_cnt) {
			sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
					  " select %d c1, %d c2, %d c3 ",
					  supware[ol_num_seq[ol_number]],
					  itemid[ol_num_seq[ol_number]],
					  st_rec[ol_num_seq[ol_number]].s_quantity);
		} else {
			sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
					  " select %d c1, %d c2, %d c3 ",
					  INVALID_W_ID, INVALID_I_ID, 0);
		}
	}
	sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
 			   ") tmp JOIN onesql_stock ON s_w_id = c1 and s_i_id = c2 SET s_quantity = c3");

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

	DEBUGTPCCINFO("neword -- update onesql_stock", mysql_affected_rows(ctx[t_num]));

	proceed = 9;
	/*EXEC_SQL INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, 
					 ol_number, ol_i_id, 
					 ol_supply_w_id, ol_quantity, 
					 ol_amount, ol_dist_info)
		VALUES (:o_id, :d_id, :w_id, :ol_number, :ol_i_id,
			:ol_supply_w_id, :ol_quantity, :ol_amount,
			:ol_dist_info);*/

        sqllen = 0;
        sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
			   "INSERT /* tpcc_neword_09 */ INTO onesql_order_line (ol_o_id, ol_d_id, ol_w_id, ol_number,"
			   " ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES");

	for (ol_number = 0; ol_number < o_ol_cnt; ol_number++) {
		if (ol_number > 0) sqlbuf[sqllen++] = ',';
		sqlbuf[sqllen++] = ' ';
                sqllen += snprintf(sqlbuf + sqllen, SQLBUF_SIZE - sqllen,
				   "(%d, %d, %d, %d, %d, %d, %d, %.2f, '%s')",
                                   ol_rec[ol_number].o_id, ol_rec[ol_number].d_id, ol_rec[ol_number].w_id, ol_rec[ol_number].ol_number,
                                   ol_rec[ol_number].ol_i_id, ol_rec[ol_number].ol_supply_w_id, ol_rec[ol_number].ol_quantity,
                                   ol_rec[ol_number].ol_amount, ol_rec[ol_number].ol_dist_info);
	}

	if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

	DEBUGTPCCINFO("neword -- insert onesql_order_line", mysql_affected_rows(ctx[t_num]));

	/* End Order Lines */


#ifdef DEBUG
	printf("insert 3\n");
	fflush(stdout);
#endif

	/*EXEC_SQL COMMIT WORK;*/
	if( mysql_commit(ctx[t_num]) ) goto sqlerr;
	clk1 = clock_gettime(CLOCK_REALTIME, &tbuf1 );
	if (ftrx_file) {
		fprintf(ftrx_file,"t_num: %d finish: %lu %lu start: %lu %lu\n",t_num, tbuf1.tv_sec, tbuf1.tv_nsec,
			tbuf_start.tv_sec, tbuf_start.tv_nsec);
	}

	return (1);

invaliditem:
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);

	/* printf("Item number is not valid\n"); */
	return (1); /* OK? */

sqlerr:
	fprintf(stderr,"neword %d:%d\n",t_num,proceed);
      	error(ctx[t_num], sqllen, sqlbuf);
	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);
sqlerrerr:
	return (0);
}

