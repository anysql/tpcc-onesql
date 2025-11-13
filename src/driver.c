/*
 * driver.c
 * driver for the tpcc transactions
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/times.h>
#include <time.h>
#include <errno.h>
#include "tpc.h"      /* prototypes for misc. functions */
#include "trans_if.h" /* prototypes for transacation interface calls */
#include "sequence.h"
#include "rthist.h"
#include "sb_percentile.h"

#include <mysql.h>

static int other_ware (int home_ware);
static int do_neword (int t_num);
static int do_payment (int t_num);
static int do_ordstat (int t_num);
static int do_delivery (int t_num);
static int do_slev (int t_num);

extern int fixed_mode;
extern int fixed_batch;
extern int fixed_scale;
extern int fixed_interval;
extern int fixed_terminal;
extern int measure_time;
extern int num_ware;
extern int num_conn;
extern int activate_transaction;
extern int counting_on;

extern int num_node;
extern int time_count;
extern FILE *freport_file;

extern int success[];
extern int late[];
extern int retry[];
extern int failure[];

extern int* success2[];
extern int* late2[];
extern int* retry2[];
extern int* failure2[];

extern double max_rt[];
extern double total_rt[];

extern int rt_limit[];

extern long clk_tck;
extern sb_percentile_t local_percentile;

#define MAX_RETRY 2000

__thread unsigned int thread_num = 0;
volatile unsigned int tpcc_tran_loops  = 0;
volatile unsigned int next_ware_number = 0;

inline static unsigned long long ut_time_usec()
{
    unsigned long long ret = 0;
    struct timespec tv;
    clock_gettime(CLOCK_MONOTONIC, &tv);
    ret = tv.tv_sec * 1000000ULL + tv.tv_nsec / 1000;
    return (ret);
}

inline static void ut_usleep(int interval)
{
    struct timespec tv = {0, 1000ULL * interval};
    tv.tv_sec = tv.tv_nsec / 1000000000ULL;
    tv.tv_nsec = tv.tv_nsec % 1000000000ULL;
    while(nanosleep(&tv, &tv) == -1 && errno == EINTR) {}
}

void debugTpccInfo(const char *info, int rows) {
    if (num_conn == 1 && tpcc_tran_loops > 0 &&  tpcc_tran_loops <= 100) {
            printf("TrxID = %3u : %s rows = %d\n", tpcc_tran_loops, info, rows);
            fflush(stdout);
    }
}

void setThreadId(int tnum)
{
    thread_num = tnum;
}

static void incCurrentWareNumber()
{
    unsigned int step = (fixed_mode * 5) / 100;
    __sync_fetch_and_add(&next_ware_number, (step > 0 ? step : 1));
}

static int real_fixed_mode()
{
    return round((1.0f * fixed_mode * measure_time) /
                 (measure_time + fixed_scale * time_count));
}

static unsigned int db_ware_rand(int fixed) {
    return (fixed_mode > 0 ?
            next_ware_number + 
                (fixed > 0 ? thread_num : RandomNumber(1, real_fixed_mode()))  : db_rand(0));
}

static int RandomWareNumber(int min, int max)
{
    return min + (db_ware_rand(fixed_terminal) % ((max - min) + 1));
}

static int RandomOtherNumber(int min, int max)
{
    return min + (db_ware_rand(0) % ((max - min) + 1));
}

int driver (int t_num)
{
    int loops = 0;
    int i, j;
    unsigned long long tm1=0, tm2=0;
    
    /* Actually, WaitTimes are needed... */
    tm1 = tm2 = ut_time_usec();
    while( activate_transaction ){
      switch(seq_get()){
      case 0:
        tm2 = ut_time_usec();
        if ((tm2 - tm1) + 100ULL < (1000ULL * fixed_interval)) {
          ut_usleep((1000ULL * fixed_interval) - (tm2 - tm1));
          tm1 = ut_time_usec();
        } else {
          tm1 = tm2;
        }
        if (fixed_mode > 0) {
          if ((__sync_fetch_and_add(&tpcc_tran_loops, 1) % fixed_batch) == 0) {
            incCurrentWareNumber();
          }
        }
	do_neword(t_num);
	break;
      case 1:
	do_payment(t_num);
	break;
      case 2:
	do_ordstat(t_num);
	break;
      case 3:
	do_delivery(t_num);
	break;
      case 4:
	do_slev(t_num);
	break;
      default:
	printf("Error - Unknown sequence.\n");
      }
    }

    return(0);

}

/*
 * prepare data and execute the new order transaction for one order
 * officially, this is supposed to be simulated terminal I/O
 */
static int do_neword (int t_num)
{
    int c_num;
    int i,j,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, d_id, c_id, ol_cnt;
    int  all_local = 1;
    int  notfound = MAXITEMS+1;  /* valid item ids are numbered consecutively
				    [1..MAXITEMS] */
    int rbk;
    int  itemid[MAX_NUM_ITEMS];
    int  supware[MAX_NUM_ITEMS];
    int  qty[MAX_NUM_ITEMS];

    if(num_node==0){
	w_id = RandomWareNumber(1, num_ware);
    }else{
	c_num = ((num_node * t_num)/num_conn); /* drop moduls */
	w_id = RandomWareNumber(1 + (num_ware * c_num)/num_node,
			    (num_ware * (c_num + 1))/num_node);
    }
    d_id = RandomNumber(1, DIST_PER_WARE);
    c_id = NURand(1023, 1, CUST_PER_DIST);

    ol_cnt = RandomNumber(5, 15);
    rbk = RandomNumber(1, 100);

    for (i = 0; i < ol_cnt; i++) {
	/* Generate unique itemid list */
        do {
            ret = 1;
            itemid[i] = NURand(8191, 1, MAXITEMS);
            for(j = 0; j < i; j++) {
                if (itemid[i] == itemid[j]) {
		    ret = 0;
		    break;
		}
            }
        } while (ret == 0);

	if ((i == ol_cnt - 1) && (rbk == 1)) {
	    itemid[i] = notfound;
	}
	if (RandomNumber(1, 100) != 1) {
	    supware[i] = w_id;
	} else {
	    supware[i] = other_ware(w_id);
	    all_local = 0;
	}
	qty[i] = RandomNumber(1, 10);
    }

    clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = neword(t_num, w_id, d_id, c_id, ol_cnt, all_local, itemid, supware, qty);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
        //printf("NOT : %.3f\n", rt);
        if (freport_file != NULL) {
          fprintf(freport_file,"%d %.3f\n", time_count, rt);
        }

	if(rt > max_rt[0])
	  max_rt[0]=rt;
	total_rt[0] += rt;
	sb_percentile_update(&local_percentile, rt);
	hist_inc(0, rt);
	if(counting_on){
	  if( rt < rt_limit[0]){
	    success[0]++;
	    success2[0][t_num]++;
	  }else{
	    late[0]++;
	    late2[0][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[0]++;
	  retry2[0][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[0]--;
      retry2[0][t_num]--;
      failure[0]++;
      failure2[0][t_num]++;
    }

    return (0);
}

/*
 * produce the id of a valid warehouse other than home_ware
 * (assuming there is one)
 */
static int other_ware (int home_ware)
{
    int tmp;

    if (num_ware == 1) return home_ware;
    while ((tmp = RandomOtherNumber(1, num_ware)) == home_ware);
    return tmp;
}

/*
 * prepare data and execute payment transaction
 */
static int do_payment (int t_num)
{
    int c_num;
    int byname,i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, d_id, c_w_id, c_d_id, c_id, h_amount;
    char c_last[17];

    if(num_node==0){
	w_id = RandomWareNumber(1, num_ware);
    }else{
	c_num = ((num_node * t_num)/num_conn); /* drop moduls */
	w_id = RandomWareNumber(1 + (num_ware * c_num)/num_node,
			    (num_ware * (c_num + 1))/num_node);
    }
    d_id = RandomNumber(1, DIST_PER_WARE);
    c_id = NURand(1023, 1, CUST_PER_DIST); 
    Lastname(NURand(255,0,999), c_last); 
    h_amount = RandomNumber(1,5000);
    if (RandomNumber(1, 100) <= 60) {
        byname = 1; /* select by last name */
    }else{
        byname = 0; /* select by customer id */
    }
    if (RandomNumber(1, 100) <= 85) {
        c_w_id = w_id;
        c_d_id = d_id;
    }else{
        c_w_id = other_ware(w_id);
        c_d_id = RandomNumber(1, DIST_PER_WARE);
    }

    clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = payment(t_num, w_id, d_id, byname, c_w_id, c_d_id, c_id, c_last, h_amount);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
	if(rt > max_rt[1])
	  max_rt[1]=rt;
	total_rt[1] += rt;
	hist_inc(1, rt);
	if(counting_on){
	  if( rt < rt_limit[1]){
	    success[1]++;
	    success2[1][t_num]++;
	  }else{
	    late[1]++;
	    late2[1][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[1]++;
	  retry2[1][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[1]--;
      retry2[1][t_num]--;
      failure[1]++;
      failure2[1][t_num]++;
    }

    return (0);
}

/*
 * prepare data and execute order status transaction
 */
static int do_ordstat (int t_num)
{
    int c_num;
    int byname,i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, d_id, c_id;
    char c_last[17] = "";

    if(num_node==0){
	w_id = RandomWareNumber(1, num_ware);
    }else{
	c_num = ((num_node * t_num)/num_conn); /* drop moduls */
	w_id = RandomWareNumber(1 + (num_ware * c_num)/num_node,
			    (num_ware * (c_num + 1))/num_node);
    }
    d_id = RandomNumber(1, DIST_PER_WARE);
    c_id = NURand(1023, 1, CUST_PER_DIST); 
    Lastname(NURand(255,0,999), c_last); 
    if (RandomNumber(1, 100) <= 60) {
        byname = 1; /* select by last name */
    }else{
        byname = 0; /* select by customer id */
    }

      clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = ordstat(t_num, w_id, d_id, byname, c_id, c_last);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
	if(rt > max_rt[2])
	  max_rt[2]=rt;
	total_rt[2] += rt;
	hist_inc(2, rt);
	if(counting_on){
	  if( rt < rt_limit[2]){
	    success[2]++;
	    success2[2][t_num]++;
	  }else{
	    late[2]++;
	    late2[2][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[2]++;
	  retry2[2][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[2]--;
      retry2[2][t_num]--;
      failure[2]++;
      failure2[2][t_num]++;
    }

    return (0);

}

/*
 * execute delivery transaction
 */
static int do_delivery (int t_num)
{
    int c_num;
    int i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, o_carrier_id;

    if(num_node==0){
	w_id = RandomWareNumber(1, num_ware);
    }else{
	c_num = ((num_node * t_num)/num_conn); /* drop moduls */
	w_id = RandomWareNumber(1 + (num_ware * c_num)/num_node,
			    (num_ware * (c_num + 1))/num_node);
    }
    o_carrier_id = RandomNumber(1, 10);

      clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = delivery(t_num, w_id, o_carrier_id);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
	if(rt > max_rt[3])
	  max_rt[3]=rt;
	total_rt[3] += rt;
	hist_inc(3, rt );
	if(counting_on){
	  if( rt < rt_limit[3]){
	    success[3]++;
	    success2[3][t_num]++;
	  }else{
	    late[3]++;
	    late2[3][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[3]++;
	  retry2[3][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[3]--;
      retry2[3][t_num]--;
      failure[3]++;
      failure2[3][t_num]++;
    }

    return (0);

}

/* 
 * prepare data and execute the stock level transaction
 */
static int do_slev (int t_num)
{
    int c_num;
    int i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, d_id, level;

    if(num_node==0){
	w_id = RandomWareNumber(1, num_ware);
    }else{
	c_num = ((num_node * t_num)/num_conn); /* drop moduls */
	w_id = RandomWareNumber(1 + (num_ware * c_num)/num_node,
			    (num_ware * (c_num + 1))/num_node);
    }
    d_id = RandomNumber(1, DIST_PER_WARE); 
    level = RandomNumber(10, 20); 

      clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = slev(t_num, w_id, d_id, level);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
	if(rt > max_rt[4])
	  max_rt[4]=rt;
	total_rt[4] += rt;
	hist_inc(4, rt );
	if(counting_on){
	  if( rt < rt_limit[4]){
	    success[4]++;
	    success2[4][t_num]++;
	  }else{
	    late[4]++;
	    late2[4][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[4]++;
	  retry2[4][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[4]--;
      retry2[4][t_num]--;
      failure[4]++;
      failure2[4][t_num]++;
    }

    return (0);

}
