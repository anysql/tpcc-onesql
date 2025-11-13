/*
 * -*-C-*-  
 * payment.pc 
 * corresponds to A.2 in appendix A
 */

#include <string.h>
#include <stdio.h>
#include <time.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"

extern MYSQL **ctx;

#define NNULL ((void *)0)

/*
 * the payment transaction
 */
int payment( int t_num,
	     int w_id_arg,		/* warehouse id */
	     int d_id_arg,		/* district id */
	     int byname,		/* select by c_id or c_last? */
	     int c_w_id_arg,
	     int c_d_id_arg,
	     int c_id_arg,		/* customer id */
	     char c_last_arg[],	        /* customer last name */
	     float h_amount_arg	        /* payment amount */
)
{
	int            w_id = w_id_arg;
	int            d_id = d_id_arg;
	int            c_id = c_id_arg;
	char            w_name[11];
	char            w_street_1[21];
	char            w_street_2[21];
	char            w_city[21];
	char            w_state[3];
	char            w_zip[10];
	int            c_d_id = c_d_id_arg;
	int            c_w_id = c_w_id_arg;
	char            c_first[17];
	char            c_middle[3];
	char            c_last[17];
	char            c_street_1[21];
	char            c_street_2[21];
	char            c_city[21];
	char            c_state[3];
	char            c_zip[10];
	char            c_phone[17];
	char            c_since[20];
	char            c_credit[4];
	int            c_credit_lim;
	float           c_discount;
	float           c_balance;
	char            c_data[502];
	char            c_new_data[502];
	float           h_amount = h_amount_arg;
	char            h_data[26];
	char            d_name[11];
	char            d_street_1[21];
	char            d_street_2[21];
	char            d_city[21];
	char            d_state[3];
	char            d_zip[10];
	int            namecnt;
	char            datetime[81];

	int             n, row_cnt = 0;
	int             proceed = 0;

        int     sqllen;
        char    sqlbuf[SQLBUF_SIZE + 1];
        MYSQL_RES*    myres;
        MYSQL_ROW     myrow;
	unsigned long *mylens;

	/* EXEC SQL WHENEVER NOT FOUND GOTO sqlerr; */
	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr; */

	gettimestamp(datetime, STRFTIME_FORMAT, TIMESTAMP_LEN);

	proceed = 1;
	/*EXEC_SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip,
	                w_name
	                INTO :w_street_1, :w_street_2, :w_city, :w_state,
				:w_zip, :w_name
	                FROM warehouse
	                WHERE w_id = :w_id;*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                          "SELECT /* tpcc_payment_01 */ w_street_1, w_street_2, w_city, w_state, w_zip, w_name"
			  "  FROM onesql_warehouse WHERE w_id = %d", w_id);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
        if(!(myrow = mysql_fetch_row(myres))) {
                mysql_free_result(myres);
                goto sqlerr;
        }
	mylens = mysql_fetch_lengths(myres);
	strncpy(w_street_1, myrow[0], mylens[0] + 1);
	strncpy(w_street_2, myrow[1], mylens[1] + 1);
	strncpy(w_city, myrow[2], mylens[2] + 1);
	strncpy(w_state, myrow[3], mylens[3] + 1);
	strncpy(w_zip, myrow[4], mylens[4] + 1);
	strncpy(w_name, myrow[5], mylens[5] + 1);
        mysql_free_result(myres);

	DEBUGTPCCINFO("payment -- query onesql_warehouse", 1);

        proceed = 2;
        /*EXEC_SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip,
                        d_name
                        INTO :d_street_1, :d_street_2, :d_city, :d_state,
                                :d_zip, :d_name
                        FROM district
                        WHERE d_w_id = :w_id
                        AND d_id = :d_id;*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                          "SELECT /* tpcc_payment_02 */ d_street_1, d_street_2, d_city, d_state, d_zip, d_name"
                          "  FROM onesql_district WHERE d_w_id = %d AND d_id = %d", w_id, d_id);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
        if(!(myrow = mysql_fetch_row(myres))) {
                mysql_free_result(myres);
                goto sqlerr;
        }
	mylens = mysql_fetch_lengths(myres);
        strncpy(d_street_1, myrow[0], mylens[0] + 1);
        strncpy(d_street_2, myrow[1], mylens[1] + 1);
        strncpy(d_city, myrow[2], mylens[2] + 1);
        strncpy(d_state, myrow[3], mylens[3] + 1);
        strncpy(d_zip, myrow[4], mylens[4] + 1);
        strncpy(d_name, myrow[5], mylens[5] + 1);
        mysql_free_result(myres);

	DEBUGTPCCINFO("payment -- query onesql_district", 1);

	if (byname) {
		strncpy(c_last, c_last_arg, 17);

		proceed = 3;
		/*EXEC_SQL SELECT count(c_id) 
			INTO :namecnt
		        FROM customer
			WHERE c_w_id = :c_w_id
			AND c_d_id = :c_d_id
		        AND c_last = :c_last;*/

	        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
        	                  "SELECT /* tpcc_payment_03 */ count(c_id) FROM onesql_customer"
                	          "  WHERE c_w_id = %d AND c_d_id = %d AND c_last = '%s'",
				  c_w_id, c_d_id, c_last);

	        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        	if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
	        if(!(myrow = mysql_fetch_row(myres))) {
        	        mysql_free_result(myres);
                	goto sqlerr;
	        }
		namecnt = strtol(myrow[0], NULL, 10);
        	mysql_free_result(myres);

		DEBUGTPCCINFO("payment -- query byname onesql_coustimer count", 1);

		/*EXEC_SQL DECLARE c_byname_p CURSOR FOR
		        SELECT c_id
		        FROM customer
		        WHERE c_w_id = :c_w_id 
			AND c_d_id = :c_d_id 
			AND c_last = :c_last
			ORDER BY c_first;

			EXEC_SQL OPEN c_byname_p;*/

                sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                                  "SELECT /* tpcc_payment_04 */ c_id FROM onesql_customer"
                                  "  WHERE c_w_id = %d AND c_d_id = %d AND c_last = '%s'"
				  "  ORDER BY c_first",
                                  c_w_id, c_d_id, c_last);

                if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
                if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;

		if (namecnt % 2) 
			namecnt++;	/* Locate midpoint customer; */
		for (n = 0; n < namecnt / 2; n++) {
		    /*EXEC_SQL FETCH c_byname_p
		      INTO :c_id;*/
		    if(!(myrow = mysql_fetch_row(myres))) {
			mysql_free_result(myres);
			goto sqlerr;
		    }
		    c_id = strtol(myrow[0], NULL, 10);
		}

		/*EXEC_SQL CLOSE c_byname_p; */
		mysql_free_result(myres);

		DEBUGTPCCINFO("payment -- query byname onesql_coustimer middle", namecnt);

	}

        proceed = 4;
        /*EXEC_SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
          WHERE w_id =:w_id;*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                         "UPDATE /* tpcc_payment_05 */ onesql_warehouse SET w_ytd = w_ytd + %.2f WHERE w_id = %d",
                         h_amount, w_id);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

	DEBUGTPCCINFO("payment -- update onesql_warehouse", mysql_affected_rows(ctx[t_num]));

        proceed = 5;
        /*EXEC_SQL UPDATE district SET d_ytd = d_ytd + :h_amount
                        WHERE d_w_id = :w_id
                        AND d_id = :d_id;*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                         "UPDATE /* tpcc_payment_06 */ onesql_district SET d_ytd = d_ytd + %.2f WHERE d_w_id = %d AND d_id = %d",
                         h_amount, w_id, d_id);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

        DEBUGTPCCINFO("payment -- update onesql_district", mysql_affected_rows(ctx[t_num]));

	proceed = 6;
	/*EXEC_SQL SELECT c_first, c_middle, c_last, c_street_1,
		        c_street_2, c_city, c_state, c_zip, c_phone,
		        c_credit, c_credit_lim, c_discount, c_balance,
		        c_since
		INTO :c_first, :c_middle, :c_last, :c_street_1,
		     :c_street_2, :c_city, :c_state, :c_zip, :c_phone,
		     :c_credit, :c_credit_lim, :c_discount, :c_balance,
		     :c_since
		FROM customer
	        WHERE c_w_id = :c_w_id 
	        AND c_d_id = :c_d_id 
		AND c_id = :c_id
		FOR UPDATE;*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                          "SELECT /* tpcc_payment_07 */ c_first, c_middle, c_last, c_street_1,"
			  "       c_street_2, c_city, c_state, c_zip, c_phone,"
			  "       c_credit, c_credit_lim, c_discount, c_balance,"
			  "       c_since"
			  "  FROM onesql_customer"
                          "  WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d"
			  "  FOR UPDATE",
                          c_w_id, c_d_id, c_id);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
        if(!(myrow = mysql_fetch_row(myres))) {
                mysql_free_result(myres);
                goto sqlerr;
        }
	mylens = mysql_fetch_lengths(myres);
        strncpy(c_first, myrow[0], mylens[0] + 1);
	strncpy(c_middle, myrow[1], mylens[1] + 1);
	strncpy(c_last, myrow[2], mylens[2] + 1);
	strncpy(c_street_1, myrow[3], mylens[3] + 1);
	strncpy(c_street_2, myrow[4], mylens[4] + 1);
	strncpy(c_city, myrow[5], mylens[5] + 1);
	strncpy(c_state, myrow[6], mylens[6] + 1);
	strncpy(c_zip, myrow[7], mylens[7] + 1);
	strncpy(c_phone, myrow[8], mylens[8] + 1);
	strncpy(c_credit, myrow[9], mylens[9] + 1);
	c_credit_lim = strtol(myrow[10], NULL, 10);
	c_discount = strtod(myrow[11], NULL);
	c_balance = strtod(myrow[12], NULL);
	strncpy(c_since, myrow[13], mylens[13] + 1);
        mysql_free_result(myres);

	DEBUGTPCCINFO("payment -- query onesql_coustimer for update", 1);

	c_balance = c_balance - h_amount;
	c_credit[2] = '\0';
	if (strstr(c_credit, "BC")) {
		proceed = 7;
		/*EXEC_SQL SELECT c_data 
			INTO :c_data
		        FROM customer
		        WHERE c_w_id = :c_w_id 
			AND c_d_id = :c_d_id 
			AND c_id = :c_id; */

	        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
        	                  "SELECT /* tpcc_payment_08 */ c_data FROM onesql_customer"
                	          "  WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d",
	                          c_w_id, c_d_id, c_id);

	        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;
        	if(!(myres = mysql_store_result(ctx[t_num]))) goto sqlerr;
	        if(!(myrow = mysql_fetch_row(myres))) {
        	        mysql_free_result(myres);
                	goto sqlerr;
	        }
		mylens = mysql_fetch_lengths(myres);
	        strncpy(c_data, myrow[0], mylens[0] + 1);
	        mysql_free_result(myres);

		DEBUGTPCCINFO("payment -- query onesql_coustimer c_data", 1);

		sprintf(c_new_data, 
			"| %4d %2d %4d %2d %4d $%7.2f %12c %24c",
			c_id, c_d_id, c_w_id, d_id,
			w_id, h_amount,
			datetime, c_data);

		strncat(c_new_data, c_data, 
			500 - strlen(c_new_data));

		c_new_data[500] = '\0';

		proceed = 8;
		/*EXEC_SQL UPDATE customer
			SET c_balance = :c_balance, c_data = :c_new_data
			WHERE c_w_id = :c_w_id 
			AND c_d_id = :c_d_id 
			AND c_id = :c_id;*/

	        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
        	                 "UPDATE /* tpcc_payment_09 */ onesql_customer SET c_balance = %.2f, c_data = '%s'"
				 "  WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d",
                	         c_balance, c_data, c_w_id, c_d_id, c_id);

	        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

		DEBUGTPCCINFO("payment -- update onesql_customer case 1", mysql_affected_rows(ctx[t_num]));
	} else {
		proceed = 9;
		/*EXEC_SQL UPDATE customer 
			SET c_balance = :c_balance
			WHERE c_w_id = :c_w_id 
			AND c_d_id = :c_d_id 
			AND c_id = :c_id;*/

                sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                                 "UPDATE /* tpcc_payment_10 */ onesql_customer SET c_balance = %.2f"
				 " WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d",
                                 c_balance, c_w_id, c_d_id, c_id);

                if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

		DEBUGTPCCINFO("payment -- update onesql_customer case 2", mysql_affected_rows(ctx[t_num]));
	}
	strncpy(h_data, w_name, 10);
	h_data[10] = '\0';
	strncat(h_data, d_name, 10);
	h_data[20] = ' ';
	h_data[21] = ' ';
	h_data[22] = ' ';
	h_data[23] = ' ';
	h_data[24] = '\0';

	proceed = 10;
	/*EXEC_SQL INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id,
			                   h_w_id, h_date, h_amount, h_data)
	                VALUES(:c_d_id, :c_w_id, :c_id, :d_id,
		               :w_id, 
			       :datetime,
			       :h_amount, :h_data);*/

        sqllen = snprintf(sqlbuf, SQLBUF_SIZE,
                          "INSERT /* tpcc_payment_11 */ INTO onesql_history (h_c_d_id, h_c_w_id, h_c_id, h_d_id,"
			  " h_w_id, h_date, h_amount, h_data) VALUES (%d, %d, %d, %d, %d, '%s', %.2f, '%s')",
                          c_d_id, c_w_id, c_id, d_id, w_id, datetime, h_amount, h_data);

        if(mysql_real_query(ctx[t_num], sqlbuf, sqllen) ) goto sqlerr;

	DEBUGTPCCINFO("payment -- insert onesql_history", mysql_affected_rows(ctx[t_num]));

	/*EXEC_SQL COMMIT WORK;*/
	if( mysql_commit(ctx[t_num]) ) goto sqlerr;

	return (1);

sqlerr:
        fprintf(stderr, "payment %d:%d\n",t_num,proceed);
	error(ctx[t_num], sqllen, sqlbuf);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);
sqlerrerr:
	return (0);
}
