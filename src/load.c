/*
 * corresponds to A.6 in appendix A
 */

/*
 * ==================================================================+ | Load
 * TPCC tables
 * +==================================================================
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <limits.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"

#define IOCACHE_SIZE 131072

#define BATCH_LARGE  500
#define BATCH_MIDDLE  50
#define BATCH_SMALL    5 
#define BATCH_NONE     1

#define NNULL ((void *)0)
//#undef NULL

#define ITEM_COL_LIST "i_id, i_im_id, i_name, i_price, i_data"
#define WAREHOUSE_COL_LIST "w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd"
#define DISTRICT_COL_LIST "d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd,\n   d_next_o_id"
#define HISTORY_COL_LIST "h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data"
#define STOCK_COL_LIST "s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,\n   s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data"
#define NEW_ORDERS_COL_LIST "no_o_id, no_d_id, no_w_id"
#define ORDERS_COL_LIST "o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local"
#define ORDER_LINE_COL_LIST "ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity,\n   ol_amount, ol_dist_info"
#define CUSTOMER_COL_LIST "c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state,\n   c_zip,  c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment,\n   c_payment_cnt, c_delivery_cnt, c_data"

MYSQL *mysql;
MYSQL_STMT *stmt[17];

/* Global SQL Variables */
char		file_dir[512];
int		tm_len = 0;
char            timestamp[81];
long            count_ware = 0;
int             fd, seed;

int             particle_flg = 0; /* "1" means particle mode */
int             part_no = 0; /* 1:items 2:warehouse 3:customer 4:orders */
int		use_replace = 0;
long            min_ware = 0;
long            max_ware = 0;

/* Global Variables */
int             i;
int             option_debug = 0;	/* 1 if generating debug output    */
int             is_local = 1;           /* "1" mean local */

#define DB_STRING_MAX 51

#include "parse_port.h"

/* Return the number of digits of 'v' when converted to string in radix 10.
 * See ll2string() for more information. */
int32_t digits10(int64_t v) {
    if (v < 10) return 1;
    if (v < 100) return 2;
    if (v < 1000) return 3;
    if (v < 1000000000000UL) {
        if (v < 100000000UL) {
            if (v < 1000000) {
                if (v < 10000) return 4;
                return 5 + (v >= 100000);
            }
            return 7 + (v >= 10000000UL);
        }
        if (v < 10000000000UL) {
            return 9 + (v >= 1000000000UL);
        }
        return 11 + (v >= 100000000000UL);
    }
    return 12 + digits10(v / 1000000000000UL);
}

int ll2string(char* dst, size_t dstlen, long long svalue) {
    static const char digits[201] =
        "0001020304050607080910111213141516171819"
        "2021222324252627282930313233343536373839"
        "4041424344454647484950515253545556575859"
        "6061626364656667686970717273747576777879"
        "8081828384858687888990919293949596979899";
    int negative;
    unsigned long long value;

    /* The main loop works with 64bit unsigned integers for simplicity, so
     * we convert the number here and remember if it is negative. */
    if (svalue < 0) {
        if (svalue != LLONG_MIN) {
            value = -svalue;
        } else {
            value = ((unsigned long long) LLONG_MAX)+1;
        }
        negative = 1;
    } else {
        value = svalue;
        negative = 0;
    }

    /* Check length. */
    int32_t const length = digits10(value)+negative;
    if (length >= dstlen) return 0;

    /* Null term. */
    int32_t next = length;
    dst[next] = '\0';
    next--;
    while (value >= 100) {
        int const i = (value % 100) * 2;
        value /= 100;
        dst[next] = digits[i + 1];
        dst[next - 1] = digits[i];
        next -= 2;
    }

    /* Handle last 1-2 digits. */
    if (value < 10) {
        dst[next] = '0' + (int32_t) value;
    } else {
        int i = (int32_t) value * 2;
        dst[next] = digits[i + 1];
        dst[next - 1] = digits[i];
    }

    /* Add sign. */
    if (negative) dst[0] = '-';
    return length;
}

int
try_stmt_execute(MYSQL_STMT *mysql_stmt)
{
    int ret = mysql_stmt_execute(mysql_stmt);
    if (ret) {
        printf("\n%d, %s, %s\n", mysql_errno(mysql), mysql_sqlstate(mysql), mysql_error(mysql) );
        mysql_rollback(mysql);
    }
    return ret;
}

int batch_sql_prepare(MYSQL_STMT *stmt, const char *prefix, const char *suffix, int count) {
    int pos = 0;
    int ret = 0;
    char sqlbuf[65536];

    pos += sprintf(sqlbuf + pos, "%s %s", prefix, suffix);
    while((--count) > 0) {
      pos += sprintf(sqlbuf + pos, ",\n  %s", suffix);
    }
    ret = mysql_stmt_prepare(stmt, sqlbuf, pos);

    return ret;
}

inline static unsigned long long ut_time_usec()
{
    unsigned long long ret = 0;
    struct timespec tv;
    clock_gettime(CLOCK_MONOTONIC, &tv);
    ret = tv.tv_sec * 1000000ULL + tv.tv_nsec / 1000;
    return (ret);
}

/*
 * ==================================================================+ |
 * main() | ARGUMENTS |      Warehouses n [Debug] [Help]
 * +==================================================================
 */
void 
main(argc, argv)
	int             argc;
	char           *argv[];
{
	char            arg[2];
        char           *ptr;

	char           connect_string[DB_STRING_MAX];
	char           db_string[DB_STRING_MAX];
	char	       db_user[DB_STRING_MAX];
	char	       db_password[DB_STRING_MAX];
	char	       db_charset[DB_STRING_MAX] = "";
        int            port= 3306;

        unsigned long long tm_start = ut_time_usec();
        unsigned long long tm_end   = 0LL;

	int i,r,c;

	char set_names_cmd[512];
	MYSQL* resp;

	memset(file_dir, 0, 512);
	memset(set_names_cmd, 0, 512);
	/* initialize */
	count_ware = 0;

	printf("*************************************\n");
	printf("*** TPCC-MySQL Data Loader        ***\n");
	printf("*************************************\n");

  /* Parse args */

    while ( (c = getopt(argc, argv, "h:P:d:u:p:w:l:m:n:R:F:C:")) != -1) {
        switch (c) {
        case 'h':
            printf ("option h with value '%s'\n", optarg);
            strncpy(connect_string, optarg, DB_STRING_MAX);
            break;
        case 'd':
            printf ("option d with value '%s'\n", optarg);
            strncpy(db_string, optarg, DB_STRING_MAX);
            break;
        case 'u':
            printf ("option u with value '%s'\n", optarg);
            strncpy(db_user, optarg, DB_STRING_MAX);
            break;
        case 'p':
            printf ("option p with value '%s'\n", optarg);
            strncpy(db_password, optarg, DB_STRING_MAX);
            break;
        case 'w':
            printf ("option w with value '%s'\n", optarg);
            count_ware = atoi(optarg);
            break;
        case 'l':
            printf ("option l with value '%s'\n", optarg);
            part_no = atoi(optarg);
	    particle_flg = 1;
            break;
        case 'm':
            printf ("option m with value '%s'\n", optarg);
            min_ware = atoi(optarg);
            break;
        case 'n':
            printf ("option n with value '%s'\n", optarg);
            max_ware = atoi(optarg);
            break;
        case 'P':
            printf ("option P with value '%s'\n", optarg);
            port = atoi(optarg);
            break;
        case 'R':
            printf ("option R with value '%s'\n", optarg);
            use_replace = atoi(optarg);
            break;
	case 'F':
            printf ("option F with value '%s'\n", optarg);
	    strncpy(file_dir, optarg, 511);
            break;
        case 'C':
            printf ("option C (charset) with value '%s'\n", optarg);
            strncpy(db_charset, optarg, DB_STRING_MAX);
            break;
	case 'H':
        case '?':
    	    printf("Usage: tpcc_load -h server_host -P port -d database_name -u mysql_user\n");
	    printf("                 -p mysql_password -w warehouses -l part -m min_wh -n max_wh\n");
    	    printf("* [part]: 1=ITEMS 2=WAREHOUSE 3=CUSTOMER 4=ORDERS\n");
            exit(0);
        default:
            printf ("?? getopt returned character code 0%o ??\n", c);
        }
    }

    if (optind < argc) {
        printf ("non-option ARGV-elements: ");
        while (optind < argc)
            printf ("%s ", argv[optind++]);
        printf ("\n");
    }

	if(strcmp(connect_string,"l")==0){
	  is_local = 1;
	}else{
	  is_local = 0;
	}

	if (min_ware <= 0) min_ware = 1;
	if (max_ware <= 0) max_ware = count_ware;

	if (strlen(file_dir) == 0) {
		printf("<Parameters>\n");
		if(is_local==0)printf("     [server]: %s\n", connect_string);
		if(is_local==0)printf("     [port]: %d\n", port);
		printf("     [DBname]: %s\n", db_string);
		printf("       [user]: %s\n", db_user);
		printf("       [pass]: %s\n", db_password);

		printf("  [warehouse]: %d\n", count_ware);

		if(particle_flg==1){
		    printf("  [part(1-4)]: %d\n", part_no);
		    printf("     [MIN WH]: %d\n", min_ware);
		    printf("     [MAX WH]: %d\n", max_ware);
		}
	}

	fd = open("/dev/urandom", O_RDONLY);
	if (fd == -1) {
	    fd = open("/dev/random", O_RDONLY);
	    if (fd == -1) {
		struct timeval  tv;
		gettimeofday(&tv, NNULL);
		seed = (tv.tv_sec ^ tv.tv_usec) * tv.tv_sec * tv.tv_usec ^ tv.tv_sec;
	    }else{
		read(fd, &seed, sizeof(seed));
		close(fd);
	    }
	}else{
	    read(fd, &seed, sizeof(seed));
	    close(fd);
	}
	SetSeed(seed);

	/* Initialize timestamp (for date columns) */
	tm_len = gettimestamp(timestamp, STRFTIME_FORMAT, TIMESTAMP_LEN);

	/* EXEC SQL WHENEVER SQLERROR GOTO Error_SqlCall; */

	if (strlen(file_dir) == 0) {
		mysql = mysql_init(NULL);
		if(!mysql) goto Error_SqlCall;

		if(is_local==1){
		    /* exec sql connect :connect_string; */
		    resp = mysql_real_connect(mysql, "localhost", db_user, db_password, db_string, port, NULL, 0);
		}else{
		    /* exec sql connect :connect_string USING :db_string; */
		    resp = mysql_real_connect(mysql, connect_string, db_user, db_password, db_string, port, NULL, 0);
		}

		if(resp) {
		    if (strlen(db_charset) > 0) {
			i = 0;
		        mysql_options(mysql, MYSQL_SET_CHARSET_NAME, db_charset);
		        while(i < strlen(db_charset) && db_charset[i] != '_') i++;
		        r = snprintf(set_names_cmd, 512, "SET NAMES '%.*s' COLLATE '%s'", i, db_charset, db_charset);
		        mysql_real_query(mysql, set_names_cmd, r);
		    } else {
		        mysql_options(mysql, MYSQL_SET_CHARSET_NAME, MYSQL_AUTODETECT_CHARSET_NAME);
		    }
		    mysql_autocommit(mysql, 0);
		    mysql_query(mysql, "SET UNIQUE_CHECKS=0");
		    mysql_query(mysql, "SET FOREIGN_KEY_CHECKS=0");
		} else {
		    goto Error_SqlCall_close;
		}

		for( i=0; i<17; i++ ){
		    stmt[i] = mysql_stmt_init(mysql);
		    if(!stmt[i]) goto Error_SqlCall_close;
		}

		if( use_replace == 0) {
			if( batch_sql_prepare(stmt[0], "INSERT INTO onesql_item (" ITEM_COL_LIST ") VALUES", "(?,?,?,?,?)", BATCH_LARGE) ) goto Error_SqlCall_close;
			if( batch_sql_prepare(stmt[1], "INSERT INTO onesql_warehouse (" WAREHOUSE_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?)", 1) ) goto Error_SqlCall_close;
			if( batch_sql_prepare(stmt[2], "INSERT INTO onesql_stock (" STOCK_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,?)",  BATCH_LARGE) ) goto Error_SqlCall_close;
			if( batch_sql_prepare(stmt[3], "INSERT INTO onesql_district (" DISTRICT_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?,?)",  DIST_PER_WARE) ) goto Error_SqlCall_close;
			if( batch_sql_prepare(stmt[4], "INSERT INTO onesql_customer (" CUSTOMER_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 10.0, 1, 0,?)", BATCH_LARGE) ) goto Error_SqlCall_close;
			if( batch_sql_prepare(stmt[5], "INSERT INTO onesql_history (" HISTORY_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?)", BATCH_LARGE) ) goto Error_SqlCall_close;
			if( batch_sql_prepare(stmt[6], "INSERT INTO onesql_orders (" ORDERS_COL_LIST ") VALUES", "(?,?,?,?,?,NULL,?, 1)", BATCH_LARGE) ) goto Error_SqlCall_close;
			if( batch_sql_prepare(stmt[7], "INSERT INTO onesql_new_orders (" NEW_ORDERS_COL_LIST ") VALUES", "(?,?,?)", BATCH_LARGE) ) goto Error_SqlCall_close;
			if( batch_sql_prepare(stmt[8], "INSERT INTO onesql_orders (" ORDERS_COL_LIST ") VALUES", "(?,?,?,?,?,?,?, 1)",  BATCH_LARGE) ) goto Error_SqlCall_close;
			if( batch_sql_prepare(stmt[9], "INSERT INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?, NULL,?,?,?)", BATCH_LARGE) ) goto Error_SqlCall_close;
			if( batch_sql_prepare(stmt[10], "INSERT INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?)", BATCH_LARGE) ) goto Error_SqlCall_close;
	        	if( batch_sql_prepare(stmt[11], "INSERT INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?, NULL,?,?,?)", BATCH_MIDDLE) ) goto Error_SqlCall_close;
		        if( batch_sql_prepare(stmt[12], "INSERT INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?)", BATCH_MIDDLE) ) goto Error_SqlCall_close;
			if( batch_sql_prepare(stmt[13], "INSERT INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?, NULL,?,?,?)", BATCH_SMALL) ) goto Error_SqlCall_close;
			if( batch_sql_prepare(stmt[14], "INSERT INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?)", BATCH_SMALL) ) goto Error_SqlCall_close;
        		if( batch_sql_prepare(stmt[15], "INSERT INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?, NULL,?,?,?)", BATCH_NONE) ) goto Error_SqlCall_close;
	        	if( batch_sql_prepare(stmt[16], "INSERT INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?)", BATCH_NONE) ) goto Error_SqlCall_close;
		} else {
                        if( batch_sql_prepare(stmt[0], "REPLACE INTO onesql_item (" ITEM_COL_LIST ") VALUES", "(?,?,?,?,?)", BATCH_LARGE) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[1], "REPLACE INTO onesql_warehouse (" WAREHOUSE_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?)", 1) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[2], "REPLACE INTO onesql_stock (" STOCK_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,?)",  BATCH_LARGE) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[3], "REPLACE INTO onesql_district (" DISTRICT_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?,?)",  DIST_PER_WARE) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[4], "REPLACE INTO onesql_customer (" CUSTOMER_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 10.0, 1, 0,?)", BATCH_LARGE) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[5], "INSERT INTO onesql_history (" HISTORY_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?)", BATCH_LARGE) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[6], "REPLACE INTO onesql_orders (" ORDERS_COL_LIST ") VALUES", "(?,?,?,?,?,NULL,?, 1)", BATCH_LARGE) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[7], "REPLACE INTO onesql_new_orders (" NEW_ORDERS_COL_LIST ") VALUES", "(?,?,?)", BATCH_LARGE) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[8], "REPLACE INTO onesql_orders (" ORDERS_COL_LIST ") VALUES", "(?,?,?,?,?,?,?, 1)",  BATCH_LARGE) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[9], "REPLACE INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?, NULL,?,?,?)", BATCH_LARGE) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[10], "REPLACE INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?)", BATCH_LARGE) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[11], "REPLACE INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?, NULL,?,?,?)", BATCH_MIDDLE) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[12], "REPLACE INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?)", BATCH_MIDDLE) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[13], "REPLACE INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?, NULL,?,?,?)", BATCH_SMALL) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[14], "REPLACE INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?)", BATCH_SMALL) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[15], "REPLACE INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?, NULL,?,?,?)", BATCH_NONE) ) goto Error_SqlCall_close;
                        if( batch_sql_prepare(stmt[16], "REPLACE INTO onesql_order_line (" ORDER_LINE_COL_LIST ") VALUES", "(?,?,?,?,?,?,?,?,?,?)", BATCH_NONE) ) goto Error_SqlCall_close;
		}
	}

	/* exec sql begin transaction; */

	printf("TPCC Data Load Started...\n");

	tm_start = ut_time_usec();

	if (strlen(file_dir) == 0) {
		if(particle_flg==0){
		    if (min_ware == 1) {
		        LoadItems();
		    }
		    LoadWare();
		    LoadCust();
		    LoadOrd();
		}else if(particle_flg==1){
		    switch(part_no){
			case 1:
			    if (min_ware == 1) {
			        LoadItems();
			    }
			    break;
			case 2:
			    LoadWare();
			    break;
			case 3:
			    LoadCust();
			    break;
			case 4:
			    LoadOrd();
			    break;
			default:
			    printf("Unknown part_no\n");
			    printf("1:ITEMS 2:WAREHOUSE 3:CUSTOMER 4:ORDERS\n");
		    }
		}
	} else {
		GenerateAll(file_dir);
	}

	tm_end = ut_time_usec();

	/* EXEC SQL COMMIT WORK; */

	if (strlen(file_dir) == 0) {
		if( mysql_commit(mysql) ) goto Error_SqlCall;

		for( i=0; i<17; i++ ){
		    mysql_stmt_close(stmt[i]);
		}

		/* EXEC SQL DISCONNECT; */

		mysql_close(mysql);
	}

	printf("\nDATA LOADING COMPLETED SUCCESSFULLY in %lu ms.\n", (tm_end - tm_start) / 1000ULL);
	exit(0);
Error_SqlCall_close:
Error_SqlCall:
	Error(0);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      LoadItems | DESCRIPTION |      Loads the Item table |
 * ARGUMENTS |      none
 * +==================================================================
 */
struct Items_Record {
        int             i_id;
        int             i_im_id;
        char            i_name[25];
        float           i_price;
        char            i_data[51];
};

void 
LoadItems()
{
	int             i_id;
	int		tmp;

	struct Items_Record i_recs[BATCH_LARGE];

	int             idatasiz;
	int             orig[MAXITEMS+1];
	int             pos;
	int             i;

	MYSQL_BIND    param[5 * BATCH_LARGE];

	unsigned long long tm_start = ut_time_usec();
	unsigned long long tm_end   = 0LL;

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr; */

	printf("Loading Item ...\n");
	fflush(stdout);

	for (i = 0; i < MAXITEMS / 10; i++)
		orig[i] = 0;
	for (i = 0; i < MAXITEMS / 10; i++) {
		do {
			pos = RandomNumber(0L, MAXITEMS);
		} while (orig[pos]);
		orig[pos] = 1;
	}

	for (i_id = 1; i_id <= MAXITEMS; i_id+=BATCH_LARGE) {

		for(tmp = 0; tmp < BATCH_LARGE; tmp++) {
			/* Generate Item Data */
			i_recs[tmp].i_id = i_id + tmp;

			i_recs[tmp].i_im_id = RandomNumber(1L, 10000L);

                	i_recs[tmp].i_name[ MakeAlphaString(14, 24, i_recs[tmp].i_name) ] = 0;

			i_recs[tmp].i_price = ((int) RandomNumber(100L, 10000L)) / 100.0;

			idatasiz = MakeAlphaString(26, 50, i_recs[tmp].i_data);
			i_recs[tmp].i_data[idatasiz] = 0;

			if (orig[i_id + tmp]) {
				pos = RandomNumber(0L, idatasiz - 8);
				i_recs[tmp].i_data[pos] = 'o';
				i_recs[tmp].i_data[pos + 1] = 'r';
				i_recs[tmp].i_data[pos + 2] = 'i';
				i_recs[tmp].i_data[pos + 3] = 'g';
				i_recs[tmp].i_data[pos + 4] = 'i';
				i_recs[tmp].i_data[pos + 5] = 'n';
				i_recs[tmp].i_data[pos + 6] = 'a';
				i_recs[tmp].i_data[pos + 7] = 'l';
			}
		}

		/* EXEC SQL INSERT INTO
		                item
		                values(:i_id,:i_im_id,:i_name,:i_price,:i_data); */

		memset(param, 0, sizeof(MYSQL_BIND) * 5 * BATCH_LARGE); /* initialize */
		for(tmp = 0; tmp < BATCH_LARGE; tmp++) {
			param[tmp * 5 + 0].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 5 + 0].buffer = &i_recs[tmp].i_id;
			param[tmp * 5 + 1].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 5 + 1].buffer = &i_recs[tmp].i_im_id;
			param[tmp * 5 + 2].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 5 + 2].buffer = i_recs[tmp].i_name;
			param[tmp * 5 + 2].buffer_length = strlen(i_recs[tmp].i_name);
			param[tmp * 5 + 3].buffer_type = MYSQL_TYPE_FLOAT;
			param[tmp * 5 + 3].buffer = &i_recs[tmp].i_price;
			param[tmp * 5 + 4].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 5 + 4].buffer = i_recs[tmp].i_data;
			param[tmp * 5 + 4].buffer_length = strlen(i_recs[tmp].i_data);
		}
		if( mysql_stmt_bind_param(stmt[0], param) ) goto sqlerr;
		if( try_stmt_execute(stmt[0]) ) goto sqlerr;

#if 0
		printf("done executing sql\n");
		fflush(stdout);
#endif
	}

	/* EXEC SQL COMMIT WORK; */
	if( mysql_commit(mysql) ) goto sqlerr;

	tm_end = ut_time_usec();

	printf("... Item Done %d Rows in %lu ms. \n", (i_id - 1), (tm_end - tm_start) / 1000ULL);
	fflush(stdout);

	return;
sqlerr:
	Error(stmt[0]);
}

void
GenerateItems(FILE *fp)
{
        int             i_id;
        int             i_im_id;
        char            i_name[25];
        float           i_price;
        char            i_data[51];

        int             idatasiz;
        int             orig[MAXITEMS+1];
        int             pos;
        int             i;

	int		tmp = 0;
	char		linebuf[IOCACHE_SIZE];
	char		*pbuf;

        /* EXEC SQL WHENEVER SQLERROR GOTO sqlerr; */
        for (i = 0; i < MAXITEMS / 10; i++)
                orig[i] = 0;
        for (i = 0; i < MAXITEMS / 10; i++) {
                do {
                        pos = RandomNumber(0L, MAXITEMS);
                } while (orig[pos]);
                orig[pos] = 1;
        }

        for (i_id = 1; i_id <= MAXITEMS; i_id++) {
		tmp += ll2string(linebuf + tmp, 32, i_id);

                /* Generate Item Data */
                i_im_id = RandomNumber(1L, 10000L);

		linebuf[tmp++] = '\t';
		tmp += ll2string(linebuf + tmp, 32, i_im_id);

		i = MakeAlphaString(14, 24, i_name);

		linebuf[tmp++] = '\t';
		memcpy(linebuf + tmp, i_name, i);
		tmp += i;

                i_price = ((int) RandomNumber(100L, 10000L)) / 100.0;

		linebuf[tmp++] = '\t';
		tmp += sprintf(linebuf + tmp, "%.2f", i_price);

		linebuf[tmp++] = '\t';
		pbuf = linebuf + tmp;
                idatasiz = MakeAlphaString(26, 50, pbuf);
                if (orig[i_id]) {
                        pos = RandomNumber(0L, idatasiz - 8);
                        pbuf[pos] = 'o';
                        pbuf[pos + 1] = 'r';
                        pbuf[pos + 2] = 'i';
                        pbuf[pos + 3] = 'g';
                        pbuf[pos + 4] = 'i';
                        pbuf[pos + 5] = 'n';
                        pbuf[pos + 6] = 'a';
                        pbuf[pos + 7] = 'l';
                }
		tmp += idatasiz;

		linebuf[tmp++] = '\n';

		if (tmp + 2048 >= IOCACHE_SIZE) {
			fwrite(linebuf, 1, tmp, fp);
			tmp = 0;
		}
	}

	if (tmp > 0) {
		fwrite(linebuf, 1, tmp, fp);
	}

	fflush(fp);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      LoadWare | DESCRIPTION |      Loads the Warehouse
 * table |      Loads Stock, District as Warehouses are created | ARGUMENTS |
 * none +==================================================================
 */
void 
LoadWare()
{

	int             w_id;
        char            w_name[11];
        char            w_street_1[21];
        char            w_street_2[21];
        char            w_city[21];
        char            w_state[3];
        char            w_zip[10];
	float           w_tax;
	float           w_ytd;

	int             tmp;

	MYSQL_BIND    param[9];

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr; */

	printf("Loading Warehouse ...\n");
	fflush(stdout);

	w_id = min_ware;

	for (; w_id <= max_ware; w_id++) {

		/* Generate Warehouse Data */

                w_name[ MakeAlphaString(6, 10, w_name) ] = 0;

		MakeAddress(w_street_1, w_street_2, w_city, w_state, w_zip);

		w_tax = ((float) RandomNumber(10L, 20L)) / 100.0;
		w_ytd = 300000.00;

		if (option_debug)
			printf("WID = %ld, Name= %16s, Tax = %5.2f\n",
			       w_id, w_name, w_tax);

		/*EXEC SQL INSERT INTO
		                warehouse
		                values(:w_id,:w_name,
				       :w_street_1,:w_street_2,:w_city,:w_state,
				       :w_zip,:w_tax,:w_ytd);*/

		memset(param, 0, sizeof(MYSQL_BIND) * 9); /* initialize */
		param[0].buffer_type = MYSQL_TYPE_LONG;
		param[0].buffer = &w_id;
		param[1].buffer_type = MYSQL_TYPE_STRING;
		param[1].buffer = w_name;
		param[1].buffer_length = strlen(w_name);
		param[2].buffer_type = MYSQL_TYPE_STRING;
		param[2].buffer = w_street_1;
		param[2].buffer_length = strlen(w_street_1);
		param[3].buffer_type = MYSQL_TYPE_STRING;
		param[3].buffer = w_street_2;
		param[3].buffer_length = strlen(w_street_2);
		param[4].buffer_type = MYSQL_TYPE_STRING;
		param[4].buffer = w_city;
		param[4].buffer_length = strlen(w_city);
		param[5].buffer_type = MYSQL_TYPE_STRING;
		param[5].buffer = w_state;
		param[5].buffer_length = strlen(w_state);
		param[6].buffer_type = MYSQL_TYPE_STRING;
		param[6].buffer = w_zip;
		param[6].buffer_length = strlen(w_zip);
		param[7].buffer_type = MYSQL_TYPE_FLOAT;
		param[7].buffer = &w_tax;
		param[8].buffer_type = MYSQL_TYPE_FLOAT;
		param[8].buffer = &w_ytd;
		if( mysql_stmt_bind_param(stmt[1], param) ) goto sqlerr;
		if( try_stmt_execute(stmt[1]) ) goto sqlerr;

		/** Make Rows associated with Warehouse **/
		if( Stock(w_id) ) goto sqlerr;
		if( District(w_id) ) goto sqlerr;

		/* EXEC SQL COMMIT WORK; */
		if( mysql_commit(mysql) ) goto sqlerr;

	}

	return;
sqlerr:
	Error(0);
}

void
GenerateWare(FILE *fp_ware)
{

        int             w_id;
        char            w_name[11];
        char            w_street_1[21];
        char            w_street_2[21];
        char            w_city[21];
        char            w_state[3];
        char            w_zip[10];
        float           w_tax;
        float           w_ytd;

        int             tmp = 0;
        char            linebuf[IOCACHE_SIZE];

        for (w_id = min_ware; w_id <= max_ware; w_id++) {
                /* Generate Warehouse Data */
		tmp += ll2string(linebuf + tmp, 32, w_id);

		linebuf[tmp++] = '\t';
		tmp += MakeAlphaString(6, 10, linebuf + tmp);

                tmp += MakeAddressRecord(linebuf + tmp);

		w_tax = ((float) RandomNumber(10L, 20L)) / 100.0;
		linebuf[tmp++] = '\t';
		tmp += sprintf(linebuf + tmp, "%.2f", w_tax);

		linebuf[tmp++] = '\t';
		memcpy(linebuf + tmp, "300000.00", 9);
		tmp += 9;

		linebuf[tmp++] = '\n';

		if (tmp + 2048 >= IOCACHE_SIZE) {
			fwrite(linebuf, 1, tmp, fp_ware);
		}
	}

        if (tmp > 0) {
                fwrite(linebuf, 1, tmp, fp_ware);
        }

	fflush(fp_ware);
}

void
GenerateWareStock(FILE *fp_stock) {
	int             w_id;

        int             s_i_id;
        int             s_quantity;

        char            s_dist_01[25];
        char            s_dist_02[25];
        char            s_dist_03[25];
        char            s_dist_04[25];
        char            s_dist_05[25];
        char            s_dist_06[25];
        char            s_dist_07[25];
        char            s_dist_08[25];
        char            s_dist_09[25];
        char            s_dist_10[25];
        char            s_data[51];

        int             sdatasiz;
        int             orig[MAXITEMS+1];
        int             pos;
        int             i;

        int             tmp = 0;
        char            linebuf[IOCACHE_SIZE];
	char		*pbuf;

        for (i = 0; i < MAXITEMS / 10; i++)
                orig[i] = 0;
        for (i = 0; i < MAXITEMS / 10; i++) {
                do {
                        pos = RandomNumber(0L, MAXITEMS);
                } while (orig[pos]);
                orig[pos] = 1;
        }

	for (w_id = min_ware; w_id <= max_ware; w_id++) {
	        for (s_i_id = 1; s_i_id <= MAXITEMS; s_i_id++) {
                        /* Generate Stock Data */
			tmp += ll2string(linebuf + tmp, 32, s_i_id);

			linebuf[tmp++] = '\t';
			tmp += ll2string(linebuf + tmp, 32, w_id);

                        s_quantity = RandomNumber(10L, 100L);
			linebuf[tmp++] = '\t';
			tmp += ll2string(linebuf + tmp, 32, s_quantity);

			for(i=0; i < 10; i++) {
				linebuf[tmp++] = '\t';
				tmp += MakeAlphaString(24, 24, linebuf + tmp);
			}

			linebuf[tmp++] = '\t';
			linebuf[tmp++] = '0';

                        linebuf[tmp++] = '\t';
                        linebuf[tmp++] = '0';

                        linebuf[tmp++] = '\t';
                        linebuf[tmp++] = '0';

			linebuf[tmp++] = '\t';
			pbuf = linebuf + tmp;
			sdatasiz = MakeAlphaString(26, 50, pbuf);
                        if (orig[s_i_id]) {
                                pos = RandomNumber(0L, sdatasiz - 8);

                                pbuf[pos] = 'o';
                                pbuf[pos + 1] = 'r';
                                pbuf[pos + 2] = 'i';
                                pbuf[pos + 3] = 'g';
                                pbuf[pos + 4] = 'i';
                                pbuf[pos + 5] = 'n';
                                pbuf[pos + 6] = 'a';
                                pbuf[pos + 7] = 'l';

                        }
			tmp += sdatasiz;

			linebuf[tmp++] = '\n';

	                /*EXEC SQL INSERT INTO
        	                        stock
                	                values(:s_i_id,:s_w_id,:s_quantity,
                        	               :s_dist_01,:s_dist_02,:s_dist_03,:s_dist_04,:s_dist_05,
                                	       :s_dist_06,:s_dist_07,:s_dist_08,:s_dist_09,:s_dist_10,
	                                       0, 0, 0,:s_data);*/

	                if (tmp + 2048 >= IOCACHE_SIZE) {
        	                fwrite(linebuf, 1, tmp, fp_stock);
                	        tmp = 0;
	                }
                }
	}

        if (tmp > 0) {
                fwrite(linebuf, 1, tmp, fp_stock);
        }

	fflush(fp_stock);
}

void
GenerateWareDistrict(FILE *fp_distict) {
        int             w_id;

        int             d_id;

        char            d_name[11];
        char            d_street_1[21];
        char            d_street_2[21];
        char            d_city[21];
        char            d_state[3];
        char            d_zip[10];

        float           d_tax;
        float           d_ytd;
        int             d_next_o_id;

        int             tmp = 0;
        char            linebuf[IOCACHE_SIZE];

        for (w_id = min_ware; w_id <= max_ware; w_id++) {
		for (d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
                        /* Generate District Data */
                        tmp += ll2string(linebuf + tmp, 32, d_id);

                        linebuf[tmp++] = '\t';
                        tmp += ll2string(linebuf + tmp, 32, w_id);

			linebuf[tmp++] = '\t';
			tmp += MakeAlphaString(6L, 10L, linebuf + tmp);

                        tmp += MakeAddressRecord(linebuf + tmp);

                        d_tax = ((float) RandomNumber(10L, 20L)) / 100.0;
			linebuf[tmp++] = '\t';
			tmp += sprintf(linebuf + tmp, "%.2f", d_tax);

	                linebuf[tmp++] = '\t';
        	        memcpy(linebuf + tmp, "30000.0", 7);
                	tmp += 7;

	                linebuf[tmp++] = '\t';
	                memcpy(linebuf + tmp, "3001", 4);
        	        tmp += 4;

			linebuf[tmp++] = '\n';

	                /*EXEC SQL INSERT INTO
                                district
                                values(:d_id,:d_w_id,:d_name,
                                       :d_street_1,:d_street_2,:d_city,:d_state,:d_zip,
                                       :d_tax,:d_ytd,:d_next_o_id);*/

                        if (tmp + 2048 >= IOCACHE_SIZE) {
                                fwrite(linebuf, 1, tmp, fp_distict);
                                tmp = 0;
                        }
		}	
        }

        if (tmp > 0) {
                fwrite(linebuf, 1, tmp, fp_distict);
        }

	fflush(fp_distict);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      LoadCust | DESCRIPTION |      Loads the Customer Table
 * | ARGUMENTS |      none
 * +==================================================================
 */
void 
LoadCust()
{

	int             w_id;
	int             d_id;

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr; */

	for (w_id = min_ware; w_id <= max_ware; w_id++)
		for (d_id = 1L; d_id <= DIST_PER_WARE; d_id++)
			Customer(d_id, w_id);

	/* EXEC SQL COMMIT WORK;*/	/* Just in case */
	if( mysql_commit(mysql) ) goto sqlerr;

	return;
sqlerr:
	Error(0);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      LoadOrd | DESCRIPTION |      Loads the Orders and
 * Order_Line Tables | ARGUMENTS |      none
 * +==================================================================
 */
void 
LoadOrd()
{

	int             w_id;
	float           w_tax;
	int             d_id;
	float           d_tax;

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	for (w_id = min_ware; w_id <= max_ware; w_id++)
		for (d_id = 1L; d_id <= DIST_PER_WARE; d_id++)
			Orders(d_id, w_id);

	/* EXEC SQL COMMIT WORK; */	/* Just in case */
	if( mysql_commit(mysql) ) goto sqlerr;

	return;
sqlerr:
	Error(0);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      Stock | DESCRIPTION |      Loads the Stock table |
 * ARGUMENTS |      w_id - warehouse id
 * +==================================================================
 */
struct Stock_Record {
        int             s_i_id;
        int             s_w_id;
        int             s_quantity;

        char            s_dist_01[25];
        char            s_dist_02[25];
        char            s_dist_03[25];
        char            s_dist_04[25];
        char            s_dist_05[25];
        char            s_dist_06[25];
        char            s_dist_07[25];
        char            s_dist_08[25];
        char            s_dist_09[25];
        char            s_dist_10[25];
        char            s_data[51];
};

int 
Stock(w_id)
	int             w_id;
{
	int		tmp;
	int		s_i_id;
	struct Stock_Record s_recs[BATCH_LARGE];

	int             sdatasiz;
	int             orig[MAXITEMS+1];
	int             pos;
	int             i;
	int             error;

	MYSQL_BIND    param[14 * BATCH_LARGE];

        unsigned long long tm_start = ut_time_usec();
        unsigned long long tm_end   = 0LL;

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/
	printf("Loading Stock WID=%4ld ...\n", w_id);
	fflush(stdout);

	for (i = 0; i < MAXITEMS / 10; i++)
		orig[i] = 0;
	for (i = 0; i < MAXITEMS / 10; i++) {
		do {
			pos = RandomNumber(0L, MAXITEMS);
		} while (orig[pos]);
		orig[pos] = 1;
	}

	for (s_i_id = 1; s_i_id <= MAXITEMS; s_i_id+=BATCH_LARGE) {

		for(tmp=0; tmp < BATCH_LARGE; tmp++) {
			/* Generate Stock Data */
			s_recs[tmp].s_w_id = w_id;
			s_recs[tmp].s_i_id = s_i_id + tmp;
			s_recs[tmp].s_quantity = RandomNumber(10L, 100L);

			s_recs[tmp].s_dist_01[ MakeAlphaString(24, 24, s_recs[tmp].s_dist_01) ] = 0;
			s_recs[tmp].s_dist_02[ MakeAlphaString(24, 24, s_recs[tmp].s_dist_02) ] = 0;
			s_recs[tmp].s_dist_03[ MakeAlphaString(24, 24, s_recs[tmp].s_dist_03) ] = 0;
			s_recs[tmp].s_dist_04[ MakeAlphaString(24, 24, s_recs[tmp].s_dist_04) ] = 0;
			s_recs[tmp].s_dist_05[ MakeAlphaString(24, 24, s_recs[tmp].s_dist_05) ] = 0;
			s_recs[tmp].s_dist_06[ MakeAlphaString(24, 24, s_recs[tmp].s_dist_06) ] = 0;
			s_recs[tmp].s_dist_07[ MakeAlphaString(24, 24, s_recs[tmp].s_dist_07) ] = 0;
			s_recs[tmp].s_dist_08[ MakeAlphaString(24, 24, s_recs[tmp].s_dist_08) ] = 0;
			s_recs[tmp].s_dist_09[ MakeAlphaString(24, 24, s_recs[tmp].s_dist_09) ] = 0;
			s_recs[tmp].s_dist_10[ MakeAlphaString(24, 24, s_recs[tmp].s_dist_10) ] = 0;
			sdatasiz = MakeAlphaString(26, 50, s_recs[tmp].s_data);
			s_recs[tmp].s_data[sdatasiz] = 0;

			if (orig[s_i_id + tmp]) {
				pos = RandomNumber(0L, sdatasiz - 8);

				s_recs[tmp].s_data[pos] = 'o';
				s_recs[tmp].s_data[pos + 1] = 'r';
				s_recs[tmp].s_data[pos + 2] = 'i';
				s_recs[tmp].s_data[pos + 3] = 'g';
				s_recs[tmp].s_data[pos + 4] = 'i';
				s_recs[tmp].s_data[pos + 5] = 'n';
				s_recs[tmp].s_data[pos + 6] = 'a';
				s_recs[tmp].s_data[pos + 7] = 'l';

			}
		}

		/*EXEC SQL INSERT INTO
		                stock
		                values(:s_i_id,:s_w_id,:s_quantity,
				       :s_dist_01,:s_dist_02,:s_dist_03,:s_dist_04,:s_dist_05,
				       :s_dist_06,:s_dist_07,:s_dist_08,:s_dist_09,:s_dist_10,
				       0, 0, 0,:s_data);*/

		memset(param, 0, sizeof(MYSQL_BIND) * 14 * BATCH_LARGE); /* initialize */
		for(tmp = 0; tmp < BATCH_LARGE; tmp++) {
			param[tmp * 14 + 0].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 14 + 0].buffer = &s_recs[tmp].s_i_id;
			param[tmp * 14 + 1].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 14 + 1].buffer = &s_recs[tmp].s_w_id;
			param[tmp * 14 + 2].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 14 + 2].buffer = &s_recs[tmp].s_quantity;
			param[tmp * 14 + 3].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 14 + 3].buffer = s_recs[tmp].s_dist_01;
			param[tmp * 14 + 3].buffer_length = strlen(s_recs[tmp].s_dist_01);
			param[tmp * 14 + 4].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 14 + 4].buffer = s_recs[tmp].s_dist_02;
			param[tmp * 14 + 4].buffer_length = strlen(s_recs[tmp].s_dist_02);
			param[tmp * 14 + 5].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 14 + 5].buffer = s_recs[tmp].s_dist_03;
			param[tmp * 14 + 5].buffer_length = strlen(s_recs[tmp].s_dist_03);
			param[tmp * 14 + 6].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 14 + 6].buffer = s_recs[tmp].s_dist_04;
			param[tmp * 14 + 6].buffer_length = strlen(s_recs[tmp].s_dist_04);
			param[tmp * 14 + 7].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 14 + 7].buffer = s_recs[tmp].s_dist_05;
			param[tmp * 14 + 7].buffer_length = strlen(s_recs[tmp].s_dist_05);
			param[tmp * 14 + 8].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 14 + 8].buffer = s_recs[tmp].s_dist_06;
			param[tmp * 14 + 8].buffer_length = strlen(s_recs[tmp].s_dist_06);
			param[tmp * 14 + 9].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 14 + 9].buffer = s_recs[tmp].s_dist_07;
			param[tmp * 14 + 9].buffer_length = strlen(s_recs[tmp].s_dist_07);
			param[tmp * 14 + 10].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 14 + 10].buffer = s_recs[tmp].s_dist_08;
			param[tmp * 14 + 10].buffer_length = strlen(s_recs[tmp].s_dist_08);
			param[tmp * 14 + 11].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 14 + 11].buffer = s_recs[tmp].s_dist_09;
			param[tmp * 14 + 11].buffer_length = strlen(s_recs[tmp].s_dist_09);
			param[tmp * 14 + 12].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 14 + 12].buffer = s_recs[tmp].s_dist_10;
			param[tmp * 14 + 12].buffer_length = strlen(s_recs[tmp].s_dist_10);
			param[tmp * 14 + 13].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 14 + 13].buffer = s_recs[tmp].s_data;
			param[tmp * 14 + 13].buffer_length = strlen(s_recs[tmp].s_data);
		}
		if( mysql_stmt_bind_param(stmt[2], param) ) goto sqlerr;
		if( (error = try_stmt_execute(stmt[2])) ) goto out;
	}

	tm_end = ut_time_usec();

	printf("... Stock Done %d Rows in %lu ms.\n", (s_i_id - 1), (tm_end - tm_start) / 1000ULL);
	fflush(stdout);
out:
	return error;
sqlerr:
    Error(0);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      District | DESCRIPTION |      Loads the District table
 * | ARGUMENTS |      w_id - warehouse id
 * +==================================================================
 */
struct District_Record {
        int             d_id;
        int             d_w_id;

        char            d_name[11];
        char            d_street_1[21];
        char            d_street_2[21];
        char            d_city[21];
        char            d_state[3];
        char            d_zip[10];

        float           d_tax;
        float           d_ytd;
        int             d_next_o_id;
};

int 
District(w_id)
	int             w_id;
{
	int		d_id;
	int		tmp;
        int             error;

	struct District_Record d_recs[DIST_PER_WARE];

	MYSQL_BIND    param[11 * DIST_PER_WARE];

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	printf("Loading District WID=%4ld ...\n", w_id);
	fflush(stdout);

	for (d_id = 1; d_id <= DIST_PER_WARE; d_id+=DIST_PER_WARE) {

		for(tmp=0; tmp < DIST_PER_WARE;  tmp++) {
			/* Generate District Data */
			d_recs[tmp].d_w_id = w_id;
			d_recs[tmp].d_ytd = 30000.0;
			d_recs[tmp].d_next_o_id = 3001L;

			d_recs[tmp].d_id = d_id + tmp;

			d_recs[tmp].d_name[ MakeAlphaString(6L, 10L, d_recs[tmp].d_name) ] = 0;
			MakeAddress(d_recs[tmp].d_street_1, d_recs[tmp].d_street_2, d_recs[tmp].d_city, d_recs[tmp].d_state, d_recs[tmp].d_zip);

			d_recs[tmp].d_tax = ((float) RandomNumber(10L, 20L)) / 100.0;
		}

		/*EXEC SQL INSERT INTO
		                district
		                values(:d_id,:d_w_id,:d_name,
				       :d_street_1,:d_street_2,:d_city,:d_state,:d_zip,
				       :d_tax,:d_ytd,:d_next_o_id);*/

		memset(param, 0, sizeof(MYSQL_BIND) * 11 * DIST_PER_WARE); /* initialize */
		for(tmp=0; tmp < DIST_PER_WARE;  tmp++) {
			param[tmp * 11 + 0].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 11 + 0].buffer = &d_recs[tmp].d_id;
			param[tmp * 11 + 1].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 11 + 1].buffer = &d_recs[tmp].d_w_id;
			param[tmp * 11 + 2].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 11 + 2].buffer = d_recs[tmp].d_name;
			param[tmp * 11 + 2].buffer_length = strlen(d_recs[tmp].d_name);
			param[tmp * 11 + 3].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 11 + 3].buffer = d_recs[tmp].d_street_1;
			param[tmp * 11 + 3].buffer_length = strlen(d_recs[tmp].d_street_1);
			param[tmp * 11 + 4].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 11 + 4].buffer = d_recs[tmp].d_street_2;
			param[tmp * 11 + 4].buffer_length = strlen(d_recs[tmp].d_street_2);
			param[tmp * 11 + 5].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 11 + 5].buffer = d_recs[tmp].d_city;
			param[tmp * 11 + 5].buffer_length = strlen(d_recs[tmp].d_city);
			param[tmp * 11 + 6].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 11 + 6].buffer = d_recs[tmp].d_state;
			param[tmp * 11 + 6].buffer_length = strlen(d_recs[tmp].d_state);
			param[tmp * 11 + 7].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 11 + 7].buffer = d_recs[tmp].d_zip;
			param[tmp * 11 + 7].buffer_length = strlen(d_recs[tmp].d_zip);
			param[tmp * 11 + 8].buffer_type = MYSQL_TYPE_FLOAT;
			param[tmp * 11 + 8].buffer = &d_recs[tmp].d_tax;
			param[tmp * 11 + 9].buffer_type = MYSQL_TYPE_FLOAT;
			param[tmp * 11 + 9].buffer = &d_recs[tmp].d_ytd;
			param[tmp * 11 + 10].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 11 + 10].buffer = &d_recs[tmp].d_next_o_id;
		}
		if( mysql_stmt_bind_param(stmt[3], param) ) goto sqlerr;
		if( (error = try_stmt_execute(stmt[3])) ) goto out;
	}

out:
	return error;
sqlerr:
	Error(0);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      Customer | DESCRIPTION |      Loads Customer Table |
 * Also inserts corresponding history record | ARGUMENTS |      id   -
 * customer id |      d_id - district id |      w_id - warehouse id
 * +==================================================================
 */
struct Customer_Record {
        int             c_id;
        int             c_d_id;
        int             c_w_id;

        char            c_first[17];
        char            c_middle[3];
        char            c_last[17];
        char            c_street_1[21];
        char            c_street_2[21];
        char            c_city[21];
        char            c_state[3];
        char            c_zip[10];
        char            c_phone[17];
        char            c_since[12];
        char            c_credit[3];

        int             c_credit_lim;
        float           c_discount;
        float           c_balance;
        char            c_data[501];

        float           h_amount;

        char            h_data[25];
};

void 
Customer(d_id, w_id)
	int             d_id;
	int             w_id;
{
	int		c_id = 0;

	struct Customer_Record c_recs[BATCH_LARGE];

	int		tmp = 0;
	int		pos = 0;

	MYSQL_BIND    param[18 * BATCH_LARGE];

        unsigned long long tm_start = ut_time_usec();
        unsigned long long tm_end   = 0LL;

	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	printf("Loading Customer for DID=%2ld, WID=%4ld ...\n", d_id, w_id);
	fflush(stdout);

	for (c_id = 1; c_id <= CUST_PER_DIST; c_id+=BATCH_LARGE) {

		for(tmp = 0; tmp < BATCH_LARGE; tmp++) {
			/* Generate Customer Data */
			c_recs[tmp].c_id = c_id + tmp;
			c_recs[tmp].c_d_id = d_id;
			c_recs[tmp].c_w_id = w_id;

			c_recs[tmp].c_first[ MakeAlphaString(8, 16, c_recs[tmp].c_first) ] = 0;
			c_recs[tmp].c_middle[0] = 'O';
			c_recs[tmp].c_middle[1] = 'E';
			c_recs[tmp].c_middle[2] = 0;

			if (c_id + tmp <= 1000) {
				Lastname(c_id + tmp - 1, c_recs[tmp].c_last);
			} else {
				Lastname(NURand(255, 0, 999), c_recs[tmp].c_last);
			}

			MakeAddress(c_recs[tmp].c_street_1, c_recs[tmp].c_street_2, c_recs[tmp].c_city, c_recs[tmp].c_state, c_recs[tmp].c_zip);
			c_recs[tmp].c_phone[ MakeNumberString(16, 16, c_recs[tmp].c_phone) ] = 0;

			if (RandomNumber(0L, 1L))
				c_recs[tmp].c_credit[0] = 'G';
			else
				c_recs[tmp].c_credit[0] = 'B';
			c_recs[tmp].c_credit[1] = 'C';
			c_recs[tmp].c_credit[2] = 0;

			c_recs[tmp].c_credit_lim = 50000;
			c_recs[tmp].c_discount = ((float) RandomNumber(0L, 50L)) / 100.0;
			c_recs[tmp].c_balance = -10.0;

			c_recs[tmp].c_data[ MakeAlphaString(300, 500, c_recs[tmp].c_data) ] = 0;

	                c_recs[tmp].h_amount = 10.0;

	                c_recs[tmp].h_data[ MakeAlphaString(12, 24, c_recs[tmp].h_data) ] = 0;
		}

		/*EXEC SQL INSERT INTO
		                customer
		                values(:c_id,:c_d_id,:c_w_id,
				  :c_first,:c_middle,:c_last,
				  :c_street_1,:c_street_2,:c_city,:c_state,
				  :c_zip,
			          :c_phone, :timestamp,
				  :c_credit,
				  :c_credit_lim,:c_discount,:c_balance,
				  10.0, 1, 0,:c_data);*/

		memset(param, 0, sizeof(MYSQL_BIND) * 18 * BATCH_LARGE); /* initialize */
		for(tmp=0; tmp < BATCH_LARGE; tmp++) {
			param[tmp * 18 + 0].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 18 + 0].buffer = &c_recs[tmp].c_id;
			param[tmp * 18 + 1].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 18 + 1].buffer = &c_recs[tmp].c_d_id;
			param[tmp * 18 + 2].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 18 + 2].buffer = &c_recs[tmp].c_w_id;
			param[tmp * 18 + 3].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 18 + 3].buffer = c_recs[tmp].c_first;
			param[tmp * 18 + 3].buffer_length = strlen(c_recs[tmp].c_first);
			param[tmp * 18 + 4].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 18 + 4].buffer = c_recs[tmp].c_middle;
			param[tmp * 18 + 4].buffer_length = strlen(c_recs[tmp].c_middle);
			param[tmp * 18 + 5].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 18 + 5].buffer = c_recs[tmp].c_last;
			param[tmp * 18 + 5].buffer_length = strlen(c_recs[tmp].c_last);
			param[tmp * 18 + 6].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 18 + 6].buffer = c_recs[tmp].c_street_1;
			param[tmp * 18 + 6].buffer_length = strlen(c_recs[tmp].c_street_1);
			param[tmp * 18 + 7].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 18 + 7].buffer = c_recs[tmp].c_street_2;
			param[tmp * 18 + 7].buffer_length = strlen(c_recs[tmp].c_street_2);
			param[tmp * 18 + 8].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 18 + 8].buffer = c_recs[tmp].c_city;
			param[tmp * 18 + 8].buffer_length = strlen(c_recs[tmp].c_city);
			param[tmp * 18 + 9].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 18 + 9].buffer = c_recs[tmp].c_state;
			param[tmp * 18 + 9].buffer_length = strlen(c_recs[tmp].c_state);
			param[tmp * 18 + 10].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 18 + 10].buffer = c_recs[tmp].c_zip;
			param[tmp * 18 + 10].buffer_length = strlen(c_recs[tmp].c_zip);
			param[tmp * 18 + 11].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 18 + 11].buffer = c_recs[tmp].c_phone;
			param[tmp * 18 + 11].buffer_length = strlen(c_recs[tmp].c_phone);
			param[tmp * 18 + 12].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 18 + 12].buffer = timestamp;
			param[tmp * 18 + 12].buffer_length = tm_len;
			param[tmp * 18 + 13].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 18 + 13].buffer = c_recs[tmp].c_credit;
			param[tmp * 18 + 13].buffer_length = strlen(c_recs[tmp].c_credit);
			param[tmp * 18 + 14].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 18 + 14].buffer = &c_recs[tmp].c_credit_lim;
			param[tmp * 18 + 15].buffer_type = MYSQL_TYPE_FLOAT;
			param[tmp * 18 + 15].buffer = &c_recs[tmp].c_discount;
			param[tmp * 18 + 16].buffer_type = MYSQL_TYPE_FLOAT;
			param[tmp * 18 + 16].buffer = &c_recs[tmp].c_balance;
			param[tmp * 18 + 17].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 18 + 17].buffer = c_recs[tmp].c_data;
			param[tmp * 18 + 17].buffer_length = strlen(c_recs[tmp].c_data);
		}
		if( mysql_stmt_bind_param(stmt[4], param) ) goto sqlerr;
		if( try_stmt_execute(stmt[4]) ) goto sqlerr;

		/*EXEC SQL INSERT INTO
		                history
		                values(:c_id,:c_d_id,:c_w_id,
				       :c_d_id,:c_w_id, :timestamp,
				       :h_amount,:h_data);*/

		memset(param, 0, sizeof(MYSQL_BIND) * 8 * BATCH_LARGE); /* initialize */
		for(tmp=0; tmp < BATCH_LARGE; tmp++) {
			param[tmp * 8 + 0].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 8 + 0].buffer = &c_recs[tmp].c_id;
			param[tmp * 8 + 1].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 8 + 1].buffer = &c_recs[tmp].c_d_id;
			param[tmp * 8 + 2].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 8 + 2].buffer = &c_recs[tmp].c_w_id;
			param[tmp * 8 + 3].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 8 + 3].buffer = &c_recs[tmp].c_d_id;
			param[tmp * 8 + 4].buffer_type = MYSQL_TYPE_LONG;
			param[tmp * 8 + 4].buffer = &c_recs[tmp].c_w_id;
			param[tmp * 8 + 5].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 8 + 5].buffer = timestamp;
			param[tmp * 8 + 5].buffer_length = tm_len;
			param[tmp * 8 + 6].buffer_type = MYSQL_TYPE_FLOAT;
			param[tmp * 8 + 6].buffer = &c_recs[tmp].h_amount;
			param[tmp * 8 + 7].buffer_type = MYSQL_TYPE_STRING;
			param[tmp * 8 + 7].buffer = c_recs[tmp].h_data;
			param[tmp * 8 + 7].buffer_length = strlen(c_recs[tmp].h_data);
		}
		if( mysql_stmt_bind_param(stmt[5], param) ) goto sqlerr;
		if( try_stmt_execute(stmt[5]) ) goto sqlerr;
	}
	/* EXEC SQL COMMIT WORK; */
	if( mysql_commit(mysql) ) goto sqlerr;

	tm_end = ut_time_usec();

	printf("... Customer Done %d Rows in %lu ms.\n", (c_id - 1), (tm_end - tm_start) / 1000ULL);
	fflush(stdout);

	return;
sqlerr:
	Error(0);
}

void
GenerateWareCustomer(FILE *fp_customer, FILE *fp_history)
{
	int		w_id = 0;
	int		d_id = 0;
        int             c_id = 0;

        struct Customer_Record c_recs;

        int             pos = 0;
        int             tmp = 0;
        char            linebuf[IOCACHE_SIZE];

        int             tmp2 = 0;
        char            linebuf2[IOCACHE_SIZE];

        for (w_id = min_ware; w_id <= max_ware; w_id++) {
                for (d_id = 1L; d_id <= DIST_PER_WARE; d_id++) {
		        for (c_id = 1; c_id <= CUST_PER_DIST; c_id++) {
	                        /* Generate Customer Data */
				tmp += ll2string(linebuf + tmp, 32, c_id);

				linebuf[tmp++] = '\t';
				tmp += ll2string(linebuf + tmp, 32, d_id);

                                linebuf[tmp++] = '\t';
                                tmp += ll2string(linebuf + tmp, 32, w_id);

        	                c_recs.c_id = c_id;
                	        c_recs.c_d_id = d_id;
                        	c_recs.c_w_id = w_id;

				linebuf[tmp++] = '\t';
				tmp += MakeAlphaString(8, 16, linebuf + tmp);

        	                c_recs.c_middle[0] = 'O';
                	        c_recs.c_middle[1] = 'E';
                        	c_recs.c_middle[2] = 0;

				linebuf[tmp++] = '\t';
				linebuf[tmp++] = 'O';
				linebuf[tmp++] = 'E';

				linebuf[tmp++] = '\t';
	                        if (c_id <= 1000) {
        	                        tmp += Lastname(c_id - 1, linebuf + tmp);
                	        } else {
                        	        tmp += Lastname(NURand(255, 0, 999), linebuf + tmp);
	                        }

        	                tmp += MakeAddressRecord(linebuf + tmp);

                                linebuf[tmp++] = '\t';
				tmp += MakeNumberString(16, 16, linebuf + tmp);

                                linebuf[tmp++] = '\t';
                                memcpy(linebuf + tmp, timestamp, tm_len);
                                tmp += tm_len;

	                        if (RandomNumber(0L, 1L))
        	                        c_recs.c_credit[0] = 'G';
                	        else
                        	        c_recs.c_credit[0] = 'B';
	                        c_recs.c_credit[1] = 'C';
        	                c_recs.c_credit[2] = 0;

				linebuf[tmp++] = '\t';
				linebuf[tmp++] = c_recs.c_credit[0];
				linebuf[tmp++] = c_recs.c_credit[1];
	
        	                c_recs.c_credit_lim = 50000;
                                linebuf[tmp++] = '\t';
                                tmp += ll2string(linebuf + tmp, 32, c_recs.c_credit_lim);

                	        c_recs.c_discount = ((float) RandomNumber(0L, 50L)) / 100.0;
				linebuf[tmp++] = '\t';
				tmp += sprintf(linebuf + tmp, "%.2f", c_recs.c_discount);

                        	c_recs.c_balance = -10.0;
                                linebuf[tmp++] = '\t';
                                tmp += sprintf(linebuf + tmp, "%.2f", c_recs.c_balance);

				linebuf[tmp++] = '\t';
				linebuf[tmp++] = '1';
				linebuf[tmp++] = '0';
				linebuf[tmp++] = '.';
				linebuf[tmp++] = '0';

				linebuf[tmp++] = '\t';
				linebuf[tmp++] = '1';

				linebuf[tmp++] = '\t';
				linebuf[tmp++] = '0';

                                linebuf[tmp++] = '\t';
				tmp += MakeAlphaString(300, 500, linebuf + tmp);

				linebuf[tmp++] = '\n';

		                /*EXEC SQL INSERT INTO
                		                customer
                                		values(:c_id,:c_d_id,:c_w_id,
		                                  :c_first,:c_middle,:c_last,
                		                  :c_street_1,:c_street_2,:c_city,:c_state, :c_zip,
		                                  :c_phone, :timestamp,
                		                  :c_credit,
                                		  :c_credit_lim,:c_discount,:c_balance,
		                                  10.0, 1, 0,:c_data);*/

	                        if (tmp + 2048 >= IOCACHE_SIZE) {
	                                fwrite(linebuf, 1, tmp, fp_customer);
        	                        tmp = 0;
                	        }

		                /*EXEC SQL INSERT INTO
		                                history
		                                values(:c_id,:c_d_id,:c_w_id,
		                                       :c_d_id,:c_w_id, :timestamp,
		                                       :h_amount,:h_data);*/

                                tmp2 += ll2string(linebuf2 + tmp2, 32, c_id);

                                linebuf2[tmp2++] = '\t';
                                tmp2 += ll2string(linebuf2 + tmp2, 32, d_id);

                                linebuf2[tmp2++] = '\t';
                                tmp2 += ll2string(linebuf2 + tmp2, 32, w_id);

                                linebuf2[tmp2++] = '\t';
                                tmp2 += ll2string(linebuf2 + tmp2, 32, d_id);

                                linebuf2[tmp2++] = '\t';
                                tmp2 += ll2string(linebuf2 + tmp2, 32, w_id);

                                linebuf2[tmp2++] = '\t';
                                memcpy(linebuf2 + tmp2, timestamp, tm_len);
                                tmp2 += tm_len;

				c_recs.h_amount = 10.0;
                                linebuf2[tmp2++] = '\t';
                                tmp2 += sprintf(linebuf2 + tmp2, "%.2f", c_recs.h_amount);

                                linebuf2[tmp2++] = '\t';
				tmp2 +=  MakeAlphaString(12, 24, linebuf2+tmp2);

                                linebuf2[tmp2++] = '\n';

                                if (tmp2 + 2048 > IOCACHE_SIZE) {
                                        fwrite(linebuf2, 1, tmp2, fp_history);
                                        tmp2 = 0;
                                }
			}
                }
	}

	if (tmp > 0) {
		fwrite(linebuf, 1, tmp, fp_customer);
	}

	if (tmp2 > 0) {
		fwrite(linebuf2, 1, tmp2, fp_history);
	}

	fflush(fp_customer);
	fflush(fp_history);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      Orders | DESCRIPTION |      Loads the Orders table |
 * Also loads the Order_Line table on the fly | ARGUMENTS |      w_id -
 * warehouse id
 * +==================================================================
 */
struct Orders_Record {
        int             o_id;
        int             o_c_id;
        int             o_d_id;
        int             o_w_id;
        int             o_carrier_id;
        int             o_ol_cnt;
};

struct Orders_Line_Record {
        int             o_id;
        int             o_d_id;
        int             o_w_id;
        int             ol;
        int             ol_i_id;
        int             ol_supply_w_id;
        int             ol_quantity;
        float           ol_amount;
        char            ol_dist_info[25];
        float           i_price;
        float           c_discount;
};

void 
Orders(d_id, w_id)
	int             d_id, w_id;
{

	int             o_id;
	int             ol;

	int		tmp;
	int		ol_count;
	struct Orders_Record o_recs[BATCH_LARGE];
	struct Orders_Line_Record ol_recs[BATCH_LARGE * 15];

	MYSQL_BIND    param[10 * BATCH_LARGE];

        unsigned long long tm_start = ut_time_usec();
        unsigned long long tm_end   = 0LL;

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr; */

	printf("Loading Orders for DID=%2ld, WID= %4ld ...\n", d_id, w_id);
	fflush(stdout);

	InitPermutation();	/* initialize permutation of customer numbers */
	for (o_id = 1; o_id <= ORD_PER_DIST; o_id+=BATCH_LARGE) {

		for(tmp = 0; tmp < BATCH_LARGE; tmp++) {
			/* Generate Order Data */
			o_recs[tmp].o_d_id = d_id;
			o_recs[tmp].o_w_id = w_id;
			o_recs[tmp].o_id = o_id + tmp;
			o_recs[tmp].o_c_id = GetPermutation();
			o_recs[tmp].o_carrier_id = RandomNumber(1L, 10L);
			o_recs[tmp].o_ol_cnt = RandomNumber(5L, 15L);
		}

		if (o_id > 2000) {	/* the last 1000 orders have not been
					 * delivered) */
		    /*EXEC SQL INSERT INTO
			                orders
			                values(:o_id,:o_d_id,:o_w_id,:o_c_id,
					       :timestamp,
					       NULL,:o_ol_cnt, 1);*/

		    memset(param, 0, sizeof(MYSQL_BIND) * 6 * BATCH_LARGE); /* initialize */
		    for(tmp = 0; tmp < BATCH_LARGE; tmp++) {
			    param[tmp * 6 + 0].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 6 + 0].buffer = &o_recs[tmp].o_id;
			    param[tmp * 6 + 1].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 6 + 1].buffer = &o_recs[tmp].o_d_id;
			    param[tmp * 6 + 2].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 6 + 2].buffer = &o_recs[tmp].o_w_id;
			    param[tmp * 6 + 3].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 6 + 3].buffer = &o_recs[tmp].o_c_id;
			    param[tmp * 6 + 4].buffer_type = MYSQL_TYPE_STRING;
			    param[tmp * 6 + 4].buffer = timestamp;
			    param[tmp * 6 + 4].buffer_length = tm_len;
			    param[tmp * 6 + 5].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 6 + 5].buffer = &o_recs[tmp].o_ol_cnt;
		    }
		    if( mysql_stmt_bind_param(stmt[6], param) ) goto sqlerr;
		    if( try_stmt_execute(stmt[6]) ) goto sqlerr;

		    /*EXEC SQL INSERT INTO
			                new_orders
			                values(:o_id,:o_d_id,:o_w_id);*/

		    memset(param, 0, sizeof(MYSQL_BIND) * 3 * BATCH_LARGE); /* initialize */
		    for(tmp = 0; tmp < BATCH_LARGE; tmp++) {
			    param[tmp * 3 + 0].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 3 + 0].buffer = &o_recs[tmp].o_id;
			    param[tmp * 3 + 1].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 3 + 1].buffer = &o_recs[tmp].o_d_id;
			    param[tmp * 3 + 2].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 3 + 2].buffer = &o_recs[tmp].o_w_id;
		    }
		    if( mysql_stmt_bind_param(stmt[7], param) ) goto sqlerr;
		    if( try_stmt_execute(stmt[7]) ) goto sqlerr;
		} else {
		    /*EXEC SQL INSERT INTO
			    orders
			    values(:o_id,:o_d_id,:o_w_id,:o_c_id,
				   :timestamp,
				   :o_carrier_id,:o_ol_cnt, 1);*/

		    memset(param, 0, sizeof(MYSQL_BIND) * 7 * BATCH_LARGE); /* initialize */
		    for(tmp = 0; tmp < BATCH_LARGE; tmp++) {
			    param[tmp * 7 + 0].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 7 + 0].buffer = &o_recs[tmp].o_id;
			    param[tmp * 7 + 1].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 7 + 1].buffer = &o_recs[tmp].o_d_id;
			    param[tmp * 7 + 2].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 7 + 2].buffer = &o_recs[tmp].o_w_id;
			    param[tmp * 7 + 3].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 7 + 3].buffer = &o_recs[tmp].o_c_id;
			    param[tmp * 7 + 4].buffer_type = MYSQL_TYPE_STRING;
			    param[tmp * 7 + 4].buffer = timestamp;
			    param[tmp * 7 + 4].buffer_length = tm_len;
			    param[tmp * 7 + 5].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 7 + 5].buffer = &o_recs[tmp].o_carrier_id;
			    param[tmp * 7 + 6].buffer_type = MYSQL_TYPE_LONG;
			    param[tmp * 7 + 6].buffer = &o_recs[tmp].o_ol_cnt;
		    }
		    if( mysql_stmt_bind_param(stmt[8], param) ) goto sqlerr;
		    if( try_stmt_execute(stmt[8]) ) goto sqlerr;

		}

		ol_count = 0;
		for(tmp = 0; tmp < BATCH_LARGE; tmp++) {
			for (ol = 1; ol <= o_recs[tmp].o_ol_cnt; ol++) {
				/* Generate Order Line Data */
				ol_recs[ol_count].o_id = o_recs[tmp].o_id;
				ol_recs[ol_count].o_d_id = o_recs[tmp].o_d_id;
				ol_recs[ol_count].o_w_id = o_recs[tmp].o_w_id;
				ol_recs[ol_count].ol = ol;
				ol_recs[ol_count].ol_i_id = RandomNumber(1L, MAXITEMS);
				ol_recs[ol_count].ol_supply_w_id = o_recs[tmp].o_w_id;
				ol_recs[ol_count].ol_quantity = 5;
				ol_recs[ol_count].ol_amount = 0.0;
	
				ol_recs[ol_count].ol_dist_info[ MakeAlphaString(24, 24, ol_recs[ol_count].ol_dist_info) ] = 0;
	
				if (o_id > 2000) {
					ol_recs[ol_count].ol_amount = (float) (RandomNumber(10L, 10000L)) / 100.0;
				}

				ol_count++;
			}
		}

		for(ol=0; ol + BATCH_LARGE <= ol_count; ol += BATCH_LARGE) {
			if (o_id > 2000) {
			    /*EXEC SQL INSERT INTO
				                order_line
				                values(:o_id,:o_d_id,:o_w_id,:ol,
						       :ol_i_id,:ol_supply_w_id, NULL,
						       :ol_quantity,:tmp_float,:ol_dist_info);*/

			    memset(param, 0, sizeof(MYSQL_BIND) * 9 * BATCH_LARGE); /* initialize */
			    for(tmp = 0; tmp < BATCH_LARGE; tmp++) {
				    param[tmp * 9 + 0].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 0].buffer = &ol_recs[ol + tmp].o_id;
				    param[tmp * 9 + 1].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 1].buffer = &ol_recs[ol + tmp].o_d_id;
				    param[tmp * 9 + 2].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 2].buffer = &ol_recs[ol + tmp].o_w_id;
				    param[tmp * 9 + 3].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 3].buffer = &ol_recs[ol + tmp].ol;
				    param[tmp * 9 + 4].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 4].buffer = &ol_recs[ol + tmp].ol_i_id;
				    param[tmp * 9 + 5].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 5].buffer = &ol_recs[ol + tmp].ol_supply_w_id;
				    param[tmp * 9 + 6].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 6].buffer = &ol_recs[ol + tmp].ol_quantity;
				    param[tmp * 9 + 7].buffer_type = MYSQL_TYPE_FLOAT;
				    param[tmp * 9 + 7].buffer = &ol_recs[ol + tmp].ol_amount;
				    param[tmp * 9 + 8].buffer_type = MYSQL_TYPE_STRING;
				    param[tmp * 9 + 8].buffer = ol_recs[ol + tmp].ol_dist_info;
				    param[tmp * 9 + 8].buffer_length = strlen(ol_recs[ol + tmp].ol_dist_info);
			    }
			    if( mysql_stmt_bind_param(stmt[9], param) ) goto sqlerr;
			    if( try_stmt_execute(stmt[9]) ) goto sqlerr;

			} else {
			    /*EXEC SQL INSERT INTO
				    order_line
				    values(:o_id,:o_d_id,:o_w_id,:ol,
					   :ol_i_id,:ol_supply_w_id, 
					   :timestamp,
					   :ol_quantity,:ol_amount,:ol_dist_info);*/

			    memset(param, 0, sizeof(MYSQL_BIND) * 10 * BATCH_LARGE); /* initialize */
			    for(tmp = 0; tmp < BATCH_LARGE; tmp++) {
				    param[tmp * 10 + 0].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 0].buffer = &ol_recs[ol + tmp].o_id;
				    param[tmp * 10 + 1].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 1].buffer = &ol_recs[ol + tmp].o_d_id;
				    param[tmp * 10 + 2].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 2].buffer = &ol_recs[ol + tmp].o_w_id;
				    param[tmp * 10 + 3].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 3].buffer = &ol_recs[ol + tmp].ol;
				    param[tmp * 10 + 4].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 4].buffer = &ol_recs[ol + tmp].ol_i_id;
				    param[tmp * 10 + 5].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 5].buffer = &ol_recs[ol + tmp].ol_supply_w_id;
				    param[tmp * 10 + 6].buffer_type = MYSQL_TYPE_STRING;
				    param[tmp * 10 + 6].buffer = timestamp;
				    param[tmp * 10 + 6].buffer_length = tm_len;
				    param[tmp * 10 + 7].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 7].buffer = &ol_recs[ol + tmp].ol_quantity;
				    param[tmp * 10 + 8].buffer_type = MYSQL_TYPE_FLOAT;
				    param[tmp * 10 + 8].buffer = &ol_recs[ol + tmp].ol_amount;
				    param[tmp * 10 + 9].buffer_type = MYSQL_TYPE_STRING;
				    param[tmp * 10 + 9].buffer = ol_recs[ol + tmp].ol_dist_info;
				    param[tmp * 10 + 9].buffer_length = strlen(ol_recs[ol + tmp].ol_dist_info);
			    }
			    if( mysql_stmt_bind_param(stmt[10], param) ) goto sqlerr;
			    if( try_stmt_execute(stmt[10]) ) goto sqlerr;
			}
		}

                for(; ol + BATCH_MIDDLE <= ol_count; ol+=BATCH_MIDDLE) {
                        if (o_id > 2000) {
                            /*EXEC SQL INSERT INTO
                                                order_line
                                                values(:o_id,:o_d_id,:o_w_id,:ol,
                                                       :ol_i_id,:ol_supply_w_id, NULL,
                                                       :ol_quantity,:tmp_float,:ol_dist_info);*/

                            memset(param, 0, sizeof(MYSQL_BIND) * 9 * BATCH_MIDDLE); /* initialize */
                            for(tmp = 0; tmp < BATCH_MIDDLE; tmp++) {
                                    param[tmp * 9 + 0].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 9 + 0].buffer = &ol_recs[ol + tmp].o_id;
                                    param[tmp * 9 + 1].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 9 + 1].buffer = &ol_recs[ol + tmp].o_d_id;
                                    param[tmp * 9 + 2].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 9 + 2].buffer = &ol_recs[ol + tmp].o_w_id;
                                    param[tmp * 9 + 3].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 9 + 3].buffer = &ol_recs[ol + tmp].ol;
                                    param[tmp * 9 + 4].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 9 + 4].buffer = &ol_recs[ol + tmp].ol_i_id;
                                    param[tmp * 9 + 5].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 9 + 5].buffer = &ol_recs[ol + tmp].ol_supply_w_id;
                                    param[tmp * 9 + 6].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 9 + 6].buffer = &ol_recs[ol + tmp].ol_quantity;
                                    param[tmp * 9 + 7].buffer_type = MYSQL_TYPE_FLOAT;
                                    param[tmp * 9 + 7].buffer = &ol_recs[ol + tmp].ol_amount;
                                    param[tmp * 9 + 8].buffer_type = MYSQL_TYPE_STRING;
                                    param[tmp * 9 + 8].buffer = ol_recs[ol + tmp].ol_dist_info;
                                    param[tmp * 9 + 8].buffer_length = strlen(ol_recs[ol + tmp].ol_dist_info);
                            }
                            if( mysql_stmt_bind_param(stmt[11], param) ) goto sqlerr;
                            if( try_stmt_execute(stmt[11]) ) goto sqlerr;

                        } else {
                            /*EXEC SQL INSERT INTO
                                    order_line
                                    values(:o_id,:o_d_id,:o_w_id,:ol,
                                           :ol_i_id,:ol_supply_w_id,
                                           :timestamp,
                                           :ol_quantity,:ol_amount,:ol_dist_info);*/

                            memset(param, 0, sizeof(MYSQL_BIND) * 10 * BATCH_MIDDLE); /* initialize */
                            for(tmp = 0; tmp < BATCH_MIDDLE; tmp++) {
                                    param[tmp * 10 + 0].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 10 + 0].buffer = &ol_recs[ol + tmp].o_id;
                                    param[tmp * 10 + 1].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 10 + 1].buffer = &ol_recs[ol + tmp].o_d_id;
                                    param[tmp * 10 + 2].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 10 + 2].buffer = &ol_recs[ol + tmp].o_w_id;
                                    param[tmp * 10 + 3].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 10 + 3].buffer = &ol_recs[ol + tmp].ol;
                                    param[tmp * 10 + 4].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 10 + 4].buffer = &ol_recs[ol + tmp].ol_i_id;
                                    param[tmp * 10 + 5].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 10 + 5].buffer = &ol_recs[ol + tmp].ol_supply_w_id;
                                    param[tmp * 10 + 6].buffer_type = MYSQL_TYPE_STRING;
                                    param[tmp * 10 + 6].buffer = timestamp;
                                    param[tmp * 10 + 6].buffer_length = tm_len;
                                    param[tmp * 10 + 7].buffer_type = MYSQL_TYPE_LONG;
                                    param[tmp * 10 + 7].buffer = &ol_recs[ol + tmp].ol_quantity;
                                    param[tmp * 10 + 8].buffer_type = MYSQL_TYPE_FLOAT;
                                    param[tmp * 10 + 8].buffer = &ol_recs[ol + tmp].ol_amount;
                                    param[tmp * 10 + 9].buffer_type = MYSQL_TYPE_STRING;
                                    param[tmp * 10 + 9].buffer = ol_recs[ol + tmp].ol_dist_info;
                                    param[tmp * 10 + 9].buffer_length = strlen(ol_recs[ol + tmp].ol_dist_info);
                            }
                            if( mysql_stmt_bind_param(stmt[12], param) ) goto sqlerr;
                            if( try_stmt_execute(stmt[12]) ) goto sqlerr;
                        }
                }

		for(; ol + BATCH_SMALL <= ol_count; ol+=BATCH_SMALL) {
			if (o_id > 2000) {
			    /*EXEC SQL INSERT INTO
				                order_line
				                values(:o_id,:o_d_id,:o_w_id,:ol,
						       :ol_i_id,:ol_supply_w_id, NULL,
						       :ol_quantity,:tmp_float,:ol_dist_info);*/

			    memset(param, 0, sizeof(MYSQL_BIND) * 9 * BATCH_SMALL); /* initialize */
			    for(tmp = 0; tmp < BATCH_SMALL; tmp++) {
				    param[tmp * 9 + 0].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 0].buffer = &ol_recs[ol + tmp].o_id;
				    param[tmp * 9 + 1].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 1].buffer = &ol_recs[ol + tmp].o_d_id;
				    param[tmp * 9 + 2].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 2].buffer = &ol_recs[ol + tmp].o_w_id;
				    param[tmp * 9 + 3].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 3].buffer = &ol_recs[ol + tmp].ol;
				    param[tmp * 9 + 4].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 4].buffer = &ol_recs[ol + tmp].ol_i_id;
				    param[tmp * 9 + 5].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 5].buffer = &ol_recs[ol + tmp].ol_supply_w_id;
				    param[tmp * 9 + 6].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 6].buffer = &ol_recs[ol + tmp].ol_quantity;
				    param[tmp * 9 + 7].buffer_type = MYSQL_TYPE_FLOAT;
				    param[tmp * 9 + 7].buffer = &ol_recs[ol + tmp].ol_amount;
				    param[tmp * 9 + 8].buffer_type = MYSQL_TYPE_STRING;
				    param[tmp * 9 + 8].buffer = ol_recs[ol + tmp].ol_dist_info;
				    param[tmp * 9 + 8].buffer_length = strlen(ol_recs[ol + tmp].ol_dist_info);
			    }
			    if( mysql_stmt_bind_param(stmt[13], param) ) goto sqlerr;
			    if( try_stmt_execute(stmt[13]) ) goto sqlerr;

			} else {
			    /*EXEC SQL INSERT INTO
				    order_line
				    values(:o_id,:o_d_id,:o_w_id,:ol,
					   :ol_i_id,:ol_supply_w_id,
					   :timestamp,
					   :ol_quantity,:ol_amount,:ol_dist_info);*/

			    memset(param, 0, sizeof(MYSQL_BIND) * 10 * BATCH_SMALL); /* initialize */
			    for(tmp = 0; tmp < BATCH_SMALL; tmp++) {
				    param[tmp * 10 + 0].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 0].buffer = &ol_recs[ol + tmp].o_id;
				    param[tmp * 10 + 1].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 1].buffer = &ol_recs[ol + tmp].o_d_id;
				    param[tmp * 10 + 2].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 2].buffer = &ol_recs[ol + tmp].o_w_id;
				    param[tmp * 10 + 3].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 3].buffer = &ol_recs[ol + tmp].ol;
				    param[tmp * 10 + 4].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 4].buffer = &ol_recs[ol + tmp].ol_i_id;
				    param[tmp * 10 + 5].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 5].buffer = &ol_recs[ol + tmp].ol_supply_w_id;
				    param[tmp * 10 + 6].buffer_type = MYSQL_TYPE_STRING;
				    param[tmp * 10 + 6].buffer = timestamp;
				    param[tmp * 10 + 6].buffer_length = tm_len;
				    param[tmp * 10 + 7].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 7].buffer = &ol_recs[ol + tmp].ol_quantity;
				    param[tmp * 10 + 8].buffer_type = MYSQL_TYPE_FLOAT;
				    param[tmp * 10 + 8].buffer = &ol_recs[ol + tmp].ol_amount;
				    param[tmp * 10 + 9].buffer_type = MYSQL_TYPE_STRING;
				    param[tmp * 10 + 9].buffer = ol_recs[ol + tmp].ol_dist_info;
				    param[tmp * 10 + 9].buffer_length = strlen(ol_recs[ol + tmp].ol_dist_info);
			    }
			    if( mysql_stmt_bind_param(stmt[14], param) ) goto sqlerr;
			    if( try_stmt_execute(stmt[14]) ) goto sqlerr;
			}
		}

		for(; ol + BATCH_NONE <= ol_count; ol+=BATCH_NONE) {
			if (o_id > 2000) {
			    /*EXEC SQL INSERT INTO
				                order_line
				                values(:o_id,:o_d_id,:o_w_id,:ol,
						       :ol_i_id,:ol_supply_w_id, NULL,
						       :ol_quantity,:tmp_float,:ol_dist_info);*/

			    memset(param, 0, sizeof(MYSQL_BIND) * 9 * BATCH_NONE); /* initialize */
			    for(tmp = 0; tmp < BATCH_NONE; tmp++) {
				    param[tmp * 9 + 0].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 0].buffer = &ol_recs[ol + tmp].o_id;
				    param[tmp * 9 + 1].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 1].buffer = &ol_recs[ol + tmp].o_d_id;
				    param[tmp * 9 + 2].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 2].buffer = &ol_recs[ol + tmp].o_w_id;
				    param[tmp * 9 + 3].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 3].buffer = &ol_recs[ol + tmp].ol;
				    param[tmp * 9 + 4].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 4].buffer = &ol_recs[ol + tmp].ol_i_id;
				    param[tmp * 9 + 5].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 5].buffer = &ol_recs[ol + tmp].ol_supply_w_id;
				    param[tmp * 9 + 6].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 9 + 6].buffer = &ol_recs[ol + tmp].ol_quantity;
				    param[tmp * 9 + 7].buffer_type = MYSQL_TYPE_FLOAT;
				    param[tmp * 9 + 7].buffer = &ol_recs[ol + tmp].ol_amount;
				    param[tmp * 9 + 8].buffer_type = MYSQL_TYPE_STRING;
				    param[tmp * 9 + 8].buffer = ol_recs[ol + tmp].ol_dist_info;
				    param[tmp * 9 + 8].buffer_length = strlen(ol_recs[ol + tmp].ol_dist_info);
			    }
			    if( mysql_stmt_bind_param(stmt[15], param) ) goto sqlerr;
			    if( try_stmt_execute(stmt[15]) ) goto sqlerr;

			} else {
			    /*EXEC SQL INSERT INTO
				    order_line
				    values(:o_id,:o_d_id,:o_w_id,:ol,
					   :ol_i_id,:ol_supply_w_id,
					   :timestamp,
					   :ol_quantity,:ol_amount,:ol_dist_info);*/

			    memset(param, 0, sizeof(MYSQL_BIND) * 10 * BATCH_NONE); /* initialize */
			    for(tmp = 0; tmp < BATCH_NONE; tmp++) {
				    param[tmp * 10 + 0].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 0].buffer = &ol_recs[ol + tmp].o_id;
				    param[tmp * 10 + 1].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 1].buffer = &ol_recs[ol + tmp].o_d_id;
				    param[tmp * 10 + 2].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 2].buffer = &ol_recs[ol + tmp].o_w_id;
				    param[tmp * 10 + 3].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 3].buffer = &ol_recs[ol + tmp].ol;
				    param[tmp * 10 + 4].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 4].buffer = &ol_recs[ol + tmp].ol_i_id;
				    param[tmp * 10 + 5].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 5].buffer = &ol_recs[ol + tmp].ol_supply_w_id;
				    param[tmp * 10 + 6].buffer_type = MYSQL_TYPE_STRING;
				    param[tmp * 10 + 6].buffer = timestamp;
				    param[tmp * 10 + 6].buffer_length = tm_len;
				    param[tmp * 10 + 7].buffer_type = MYSQL_TYPE_LONG;
				    param[tmp * 10 + 7].buffer = &ol_recs[ol + tmp].ol_quantity;
				    param[tmp * 10 + 8].buffer_type = MYSQL_TYPE_FLOAT;
				    param[tmp * 10 + 8].buffer = &ol_recs[ol + tmp].ol_amount;
				    param[tmp * 10 + 9].buffer_type = MYSQL_TYPE_STRING;
				    param[tmp * 10 + 9].buffer = ol_recs[ol + tmp].ol_dist_info;
				    param[tmp * 10 + 9].buffer_length = strlen(ol_recs[ol + tmp].ol_dist_info);
			    }
			    if( mysql_stmt_bind_param(stmt[16], param) ) goto sqlerr;
			    if( try_stmt_execute(stmt[16]) ) goto sqlerr;
			}
		}
	}
	/*EXEC SQL COMMIT WORK;*/
	if( mysql_commit(mysql) ) goto sqlerr;

	tm_end = ut_time_usec();

	printf("... Orders Done %d Rows in %lu ms.\n", (o_id - 1), (tm_end - tm_start) / 1000ULL);
	fflush(stdout);

	return;
sqlerr:
	Error(0);
}

void
GenerateWareOrders(FILE *fp_orders, FILE *fp_new_orders, FILE *fp_order_line)
{
	int		d_id;
	int		w_id;
        int             o_id;
        int             ol;
        int             ol_count;

        struct Orders_Record o_recs;
        struct Orders_Line_Record ol_recs;

        int             tmp = 0;
        char            linebuf[IOCACHE_SIZE];

        int             tmp2 = 0;
        char            linebuf2[IOCACHE_SIZE];

        int             tmp3 = 0;
        char            linebuf3[IOCACHE_SIZE];

        for (w_id = min_ware; w_id <= max_ware; w_id++) {
                for (d_id = 1L; d_id <= DIST_PER_WARE; d_id++) {
			InitPermutation();      /* initialize permutation of customer numbers */
		        for (o_id = 1; o_id <= ORD_PER_DIST; o_id++) {
	                        /* Generate Order Data */
        	                o_recs.o_d_id = d_id;
                	        o_recs.o_w_id = w_id;
                        	o_recs.o_id = o_id;
	                        o_recs.o_c_id = GetPermutation();
        	                o_recs.o_carrier_id = RandomNumber(1L, 10L);
                	        o_recs.o_ol_cnt = RandomNumber(5L, 15L);

		                if (o_id > 2000) {      /* the last 1000 orders have not been
                		                         * delivered) */
		                    /*EXEC SQL INSERT INTO
                		                        orders
                                		        values(:o_id,:o_d_id,:o_w_id,:o_c_id,
		                                               :timestamp,
	                                               NULL,:o_ol_cnt, 1);*/

                                    tmp += ll2string(linebuf + tmp, 32, o_recs.o_id);

                                    linebuf[tmp++] = '\t';
                                    tmp += ll2string(linebuf + tmp, 32, o_recs.o_d_id);

                                    linebuf[tmp++] = '\t';
                                    tmp += ll2string(linebuf + tmp, 32, o_recs.o_w_id);

                                    linebuf[tmp++] = '\t';
                                    tmp += ll2string(linebuf + tmp, 32, o_recs.o_c_id);

                                    linebuf[tmp++] = '\t';
                                    memcpy(linebuf + tmp, timestamp, tm_len);
                                    tmp += tm_len;

				    linebuf[tmp++] = '\t';
				    linebuf[tmp++] = '\\';
				    linebuf[tmp++] = 'N';

                                    linebuf[tmp++] = '\t';
                                    tmp += ll2string(linebuf + tmp, 32, o_recs.o_ol_cnt);

				    linebuf[tmp++] = '\t';
				    linebuf[tmp++] = '1';

                                    linebuf[tmp++] = '\n';

                                    if (tmp + 2048 >= IOCACHE_SIZE) {
                                        fwrite(linebuf, 1, tmp, fp_orders);
                                        tmp = 0;
                                    }

		                    /*EXEC SQL INSERT INTO
                		                        new_orders
                                		        values(:o_id,:o_d_id,:o_w_id);*/

        	                    tmp2 += ll2string(linebuf2 + tmp2, 32, o_recs.o_id);
	
        	                    linebuf2[tmp2++] = '\t';
                	            tmp2 += ll2string(linebuf2 + tmp2, 32, o_recs.o_d_id);

                        	    linebuf2[tmp2++] = '\t';
                                    tmp2 += ll2string(linebuf2 + tmp2, 32, o_recs.o_w_id);

				    linebuf2[tmp2++] = '\n';

                                    if (tmp2 + 2048 >= IOCACHE_SIZE) {
                                        fwrite(linebuf2, 1, tmp2, fp_new_orders);
                                        tmp2 = 0;
                                    }
				} else {
		                    /*EXEC SQL INSERT INTO
                		            orders
		                            values(:o_id,:o_d_id,:o_w_id,:o_c_id,
		                                   :timestamp,
		                                   :o_carrier_id,:o_ol_cnt, 1);*/

                                    tmp += ll2string(linebuf + tmp, 32, o_recs.o_id);

                                    linebuf[tmp++] = '\t';
                                    tmp += ll2string(linebuf + tmp, 32, o_recs.o_d_id);

                                    linebuf[tmp++] = '\t';
                                    tmp += ll2string(linebuf + tmp, 32, o_recs.o_w_id);

                                    linebuf[tmp++] = '\t';
                                    tmp += ll2string(linebuf + tmp, 32, o_recs.o_c_id);

                                    linebuf[tmp++] = '\t';
                                    memcpy(linebuf + tmp, timestamp, tm_len);
                                    tmp += tm_len;

                                    linebuf[tmp++] = '\t';
                                    tmp += ll2string(linebuf + tmp, 32, o_recs.o_carrier_id);

                                    linebuf[tmp++] = '\t';
                                    tmp += ll2string(linebuf + tmp, 32, o_recs.o_ol_cnt);

                                    linebuf[tmp++] = '\t';
                                    linebuf[tmp++] = '1';

                                    linebuf[tmp++] = '\n';

                                    if (tmp + 2048 >= IOCACHE_SIZE) {
                                        fwrite(linebuf, 1, tmp, fp_orders);
                                        tmp = 0;
                                    }
				}

	                        for (ol = 1; ol <= o_recs.o_ol_cnt; ol++) {
        	                        /* Generate Order Line Data */
                	                ol_recs.o_id = o_recs.o_id;
                        	        ol_recs.o_d_id = o_recs.o_d_id;
	                                ol_recs.o_w_id = o_recs.o_w_id;
        	                        ol_recs.ol = ol;
                	                ol_recs.ol_i_id = RandomNumber(1L, MAXITEMS);
                        	        ol_recs.ol_supply_w_id = o_recs.o_w_id;
                                	ol_recs.ol_quantity = 5;
	                                ol_recs.ol_amount = 0.0;

	                                if (o_id > 2000) {
        	                        	ol_recs.ol_amount = (float) (RandomNumber(10L, 10000L)) / 100.0;
					}

		                        /*EXEC SQL INSERT INTO
                       		                       order_line
		                                        values(:o_id,:o_d_id,:o_w_id,:ol,
                       		                               :ol_i_id,:ol_supply_w_id, NULL,
		                                               :ol_quantity,:tmp_float,:ol_dist_info);*/

                                        tmp3 += ll2string(linebuf3 + tmp3, 32, ol_recs.o_id);

                                        linebuf3[tmp3++] = '\t';
                                        tmp3 += ll2string(linebuf3 + tmp3, 32, ol_recs.o_d_id);

                                        linebuf3[tmp3++] = '\t';
                                        tmp3 += ll2string(linebuf3 + tmp3, 32, ol_recs.o_w_id);

                                        linebuf3[tmp3++] = '\t';
                                        tmp3 += ll2string(linebuf3 + tmp3, 32, ol_recs.ol);

                                        linebuf3[tmp3++] = '\t';
                                        tmp3 += ll2string(linebuf3 + tmp3, 32, ol_recs.ol_i_id);

                                        linebuf3[tmp3++] = '\t';
                                        tmp3 += ll2string(linebuf3 + tmp3, 32, ol_recs.ol_supply_w_id);

                                        linebuf3[tmp3++] = '\t';
                                        linebuf3[tmp3++] = '\\';
                                        linebuf3[tmp3++] = 'N';

					linebuf3[tmp3++] = '\t';
					tmp3 += ll2string(linebuf3 + tmp3, 32, ol_recs.ol_quantity);

                                        linebuf3[tmp3++] = '\t';
                                        tmp3 += sprintf(linebuf3 + tmp3, "%.2f", ol_recs.ol_amount);

					linebuf3[tmp3++] = '\t';
					tmp3 += MakeAlphaString(24, 24, linebuf3 + tmp3);

                                        linebuf3[tmp3++] = '\n';

                                        if (tmp3 + 2048 >= IOCACHE_SIZE) {
                                            fwrite(linebuf3, 1, tmp3, fp_order_line);
                                            tmp3 = 0;
                                        }
	                        }
				
	                }
		}
	}

        if (tmp > 0) {
                fwrite(linebuf, 1, tmp, fp_orders);
        }

        if (tmp2 > 0) {
                fwrite(linebuf2, 1, tmp2, fp_new_orders);
        }

        if (tmp3 > 0) {
                fwrite(linebuf3, 1, tmp3, fp_order_line);
        }

	fflush(fp_orders);
	fflush(fp_new_orders);
	fflush(fp_order_line);
}

void GenerateLoadSQL(FILE *fp)
{
	fprintf(fp, "load data local infile 'customer.csv' into table onesql_customer\n  (" CUSTOMER_COL_LIST ");\n\n");
	fprintf(fp, "load data local infile 'district.csv' into table onesql_district\n  (" DISTRICT_COL_LIST ");\n\n");
	fprintf(fp, "load data local infile 'history.csv' into table onesql_history\n  (" HISTORY_COL_LIST ");\n\n");
	if (min_ware == 1) {
		fprintf(fp, "load data local infile 'item.csv' into table onesql_item\n  (" ITEM_COL_LIST ");\n\n");
	}
	fprintf(fp, "load data local infile 'new_orders.csv' into table onesql_new_orders\n  (" NEW_ORDERS_COL_LIST ");\n\n");
	fprintf(fp, "load data local infile 'order_line.csv' into table onesql_order_line\n  (" ORDER_LINE_COL_LIST ");\n\n");
	fprintf(fp, "load data local infile 'orders.csv' into table onesql_orders\n  (" ORDERS_COL_LIST ");\n\n");
	fprintf(fp, "load data local infile 'stock.csv' into table onesql_stock\n  (" STOCK_COL_LIST ");\n\n");
	fprintf(fp, "load data local infile 'warehouse.csv' into table onesql_warehouse\n  (" WAREHOUSE_COL_LIST ");\n\n");

	fflush(fp);
}

void GenerateAll(const char *dname) {
	char buf[512];
	FILE *fp1 = NULL, *fp2 = NULL, *fp3 = NULL;
        unsigned long long tm_start = ut_time_usec();
        unsigned long long tm_end   = 0LL;

	mkdir(dname, 0755);

        memset(buf, 0, 512);
        sprintf(buf, "%s/load.sql", dname);
        if ((fp1 = fopen(buf, "wb+")) != NULL) {
                GenerateLoadSQL(fp1);
                printf("Generate Load SQL Done.\n");
                fflush(stdout);
        }

        if (fp1 != NULL) {
                fclose(fp1);
                fp1 = NULL;
        }

	if (min_ware == 1) {
	        memset(buf, 0, 512);
        	sprintf(buf, "%s/item.csv", dname);
	        if ((fp1 = fopen(buf, "wb+")) != NULL) {
			tm_start = ut_time_usec();
                	GenerateItems(fp1);
			tm_end = ut_time_usec();
        	        printf("Generate Item Done in %llu ms.\n", (tm_end - tm_start) / 1000ULL);
                	fflush(stdout);
	        }

        	if (fp1 != NULL) {
	                fclose(fp1);
        	        fp1 = NULL;
	        }
	}

	memset(buf, 0, 512);
	sprintf(buf, "%s/warehouse.csv", dname);
	if ((fp1 = fopen(buf, "wb+")) != NULL) {
		tm_start = ut_time_usec();
		GenerateWare(fp1);
		tm_end = ut_time_usec();
		printf("Generate Warehouse Done in %llu ms.\n", (tm_end - tm_start) / 1000ULL);
		fflush(stdout);
	}

        if (fp1 != NULL) {
                fclose(fp1);
                fp1 = NULL;
        }

        memset(buf, 0, 512);
        sprintf(buf, "%s/stock.csv", dname);
        if ((fp1 = fopen(buf, "wb+")) != NULL) {
		tm_start = ut_time_usec();
                GenerateWareStock(fp1);
                tm_end = ut_time_usec();
                printf("Generate Stock Done in %llu ms.\n", (tm_end - tm_start) / 1000ULL);
                fflush(stdout);
        }

        if (fp1 != NULL) {
                fclose(fp1);
                fp1 = NULL;
        }

        memset(buf, 0, 512);
        sprintf(buf, "%s/district.csv", dname);
        if ((fp1 = fopen(buf, "wb+")) != NULL) {
		tm_start = ut_time_usec();
                GenerateWareDistrict(fp1);
                tm_end = ut_time_usec();
                printf("Generate District Done in %llu ms.\n", (tm_end - tm_start) / 1000ULL);
                fflush(stdout);
        }

	if (fp1 != NULL) {
                fclose(fp1);
                fp1 = NULL;
        }

	memset(buf, 0, 512);
	sprintf(buf, "%s/customer.csv", dname);
	fp1 = fopen(buf, "wb+");
        memset(buf, 0, 512);
        sprintf(buf, "%s/history.csv", dname);
        fp2 = fopen(buf, "wb+");
	if (fp1 != NULL && fp2 != NULL) {
		tm_start = ut_time_usec();
		GenerateWareCustomer(fp1, fp2);
                tm_end = ut_time_usec();
                printf("Generate Customer And History Done in %llu ms.\n", (tm_end - tm_start) / 1000ULL);
                fflush(stdout);
	}

	if (fp1 != NULL) {
		fclose(fp1);
		fp1 = NULL;
	}

        if (fp2 != NULL) {
                fclose(fp2);
                fp2 = NULL;
        }

        memset(buf, 0, 512);
        sprintf(buf, "%s/orders.csv", dname);
        fp1 = fopen(buf, "wb+");
        memset(buf, 0, 512);
        sprintf(buf, "%s/new_orders.csv", dname);
        fp2 = fopen(buf, "wb+");
	memset(buf, 0, 512);
        sprintf(buf, "%s/order_line.csv", dname);
        fp3 = fopen(buf, "wb+");
        if (fp1 != NULL && fp2 != NULL && fp3 != NULL) {
		tm_start = ut_time_usec();
                GenerateWareOrders(fp1, fp2, fp3);
                tm_end = ut_time_usec();
                printf("Generate Orders, New_Orders And Order_line Done in %llu ms.\n", (tm_end - tm_start) / 1000ULL);
                fflush(stdout);
        }

        if (fp1 != NULL) {
                fclose(fp1);
                fp1 = NULL;
        }

        if (fp2 != NULL) {
                fclose(fp2);
                fp2 = NULL;
        }

        if (fp3 != NULL) {
                fclose(fp3);
                fp3 = NULL;
        }
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      MakeAddress() | DESCRIPTION |      Build an Address |
 * ARGUMENTS
 * +==================================================================
 */
void 
MakeAddress(str1, str2, city, state, zip)
	char           *str1;
	char           *str2;
	char           *city;
	char           *state;
	char           *zip;
{
	str1[ MakeAlphaString(10, 20, str1) ] = 0;	/* Street 1 */
	str2[ MakeAlphaString(10, 20, str2) ] = 0;	/* Street 2 */
	city[ MakeAlphaString(10, 20, city) ] = 0;	/* City */
	state[ MakeAlphaString(2, 2, state) ] = 0;	/* State */
	zip[ MakeNumberString(9, 9, zip) ] = 0;	/* Zip */
}

int
MakeAddressRecord(buf)
	char	       *buf;
{
	int total = 0;

	buf[total++] = '\t';
	total += MakeAlphaString(10, 20, buf + total);

        buf[total++] = '\t';
	total += MakeAlphaString(10, 20, buf + total);

        buf[total++] = '\t';
	total += MakeAlphaString(10, 20, buf + total);

        buf[total++] = '\t';
	total += MakeAlphaString(2, 2, buf + total);

        buf[total++] = '\t';
	total += MakeNumberString(9, 9, buf + total);

	return total;
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      Error() | DESCRIPTION |      Handles an error from a
 * SQL call. | ARGUMENTS
 * +==================================================================
 */
void 
Error(mysql_stmt)
        MYSQL_STMT   *mysql_stmt;
{
    if(mysql_stmt) {
	printf("\n%d, %s, %s", mysql_stmt_errno(mysql_stmt),
	       mysql_stmt_sqlstate(mysql_stmt), mysql_stmt_error(mysql_stmt) );
    }
    printf("\n%d, %s, %s\n", mysql_errno(mysql), mysql_sqlstate(mysql), mysql_error(mysql) );

    /*EXEC SQL WHENEVER SQLERROR CONTINUE;*/

    /*EXEC SQL ROLLBACK WORK;*/
    mysql_rollback(mysql);

    /*EXEC SQL DISCONNECT;*/
    mysql_close(mysql);

	exit(-1);
}
