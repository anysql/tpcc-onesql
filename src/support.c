/*
 * support.c
 * routines needed for the tpcc loading and transaction programs
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <ctype.h>

#include "tpc.h"

static int nums[CUST_PER_DIST];
static int perm_count;

__thread unsigned int ut_rnd_ulint_counter = 0;

#define UT_RND1 151117737
#define UT_RND2 119785373
#define UT_RND3 85689495
#define UT_RND4 76595339
#define UT_SUM_RND2 98781234
#define UT_SUM_RND3 126792457
#define UT_SUM_RND4 63498502
#define UT_XOR_RND1 187678878
#define UT_XOR_RND2 143537923

static inline unsigned int ut_rnd_gen_next_uint(
    unsigned int rnd) /*!< in: the previous random number value */
{
  unsigned int n_bits;

  n_bits = 8 * sizeof(unsigned int);

  rnd = UT_RND2 * rnd + UT_SUM_RND3;
  rnd = UT_XOR_RND1 ^ rnd;
  rnd = (rnd << 20) + (rnd >> (n_bits - 20));
  rnd = UT_RND3 * rnd + UT_SUM_RND4;
  rnd = UT_XOR_RND2 ^ rnd;
  rnd = (rnd << 20) + (rnd >> (n_bits - 20));
  rnd = UT_RND1 * rnd + UT_SUM_RND2;

  return (rnd);
}

unsigned int db_rand(unsigned int adj) {
  unsigned int rnd = ut_rnd_ulint_counter;
  if (rnd == 0) {
    rnd = 65654363;
  }
  rnd = UT_RND1 * rnd + UT_RND2;
  ut_rnd_ulint_counter = rnd + adj;
  return (ut_rnd_gen_next_uint(rnd) + adj);
}

void SetSeed (int seed)
{
	srand(seed);
}

void SetThreadSeed()
{
        ut_rnd_ulint_counter = rand();
}

/*
 * return number uniformly distributed b/w min and max, inclusive
 */
int RandomNumber (int min, int max)
{
	return min + (db_rand(0) % ((max - min) + 1));
}

/*
 * non uniform random -- see p. 15
 *
 * the constant C depends on which value of A is passed, but the same
 * value of C should be used for all calls with the same value of
 * A.  however, we know in advance which values of A will be used.
 */
int NURand (unsigned A, unsigned x, unsigned y)
{
	static int first = 1;
	unsigned C, C_255, C_1023, C_8191;

	if (first) {
		C_255 = RandomNumber(0, 255);
		C_1023 = RandomNumber(0, 1023);
		C_8191 = RandomNumber(0, 8191);
		first = 0;
	}

	switch (A) {
	case 255: C = C_255; break;
	case 1023: C = C_1023; break;
	case 8191: C = C_8191; break;
	default:
		fprintf(stderr,
			"NURand: unexpected value (%d) of A used\n",
			A);
		abort();
	}

	return (int)
	       (((RandomNumber(0, A) | RandomNumber(x, y)) + C) % (y-x+1)) + x;
}

/*
 * p. 54
 *
 * make a ``random a-string'': a string of random alphanumeric
 * characters of a random length of minimum x, maximum y, and
 * mean (y+x)/2
 */
int MakeAlphaString (int x, int y, char str[])
{
	static char *alphanum = "0123456789"
			        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			        "abcdefghijklmnopqrstuvwxyz";
	int arrmax = 61;  /* index of last array element */
	register int i, len;

	len = RandomNumber(x, y);

	for (i = 0; i < len; i++)
		str[i] = alphanum[RandomNumber(0, arrmax)];

	str[len] = 0;

	return len;
}

/*
 * like MakeAlphaString, only numeric characters only
 */
int MakeNumberString (int x, int y, char str[])
{
	static char *numeric = "0123456789";
	int arrmax = 9;
	register int i, len;

	len = RandomNumber(x, y);

	for (i = 0; i < len; i++)
		str[i] = numeric[RandomNumber(0, arrmax)];

	str[len] = 0;

	return len;
}

/*
 * turn system time into database format
 * the format argument should be a strftime() format string that produces
 *   a datetime string acceptable to the database
 */
int gettimestamp (char str[], char *format, size_t len)
{
	int r = 0;
	time_t t;
	struct tm *datetime;

	t = time(NULL);
	datetime = localtime(&t);

	r = strftime(str, len, format, datetime);
	if (!r) {
		fprintf(stderr, "error writing timestamp to string\n");
		abort();
	}

	str[r] = 0;

	return r;
}

/*
 * permute the list of customer ids for the order table
 */
void InitPermutation (void)
{
	int *cur;
	int i,j;

	perm_count = 0;

	/* initialize with consecutive values [1..ORD_PER_DIST] */
	for (i = 0, cur = nums; i < ORD_PER_DIST; i++, cur++) {
		*cur = i + 1;
	}

	/* now, shuffle */
	for (i = 0; i < ORD_PER_DIST-1; i++) {
		j = (int)RandomNumber(i+1, ORD_PER_DIST-1);
		swap_int(nums[i], nums[j]);
	}
}

int GetPermutation (void)
{
	if ( perm_count >= ORD_PER_DIST ) {
		fprintf(stderr, "GetPermutation: past end of list!\n");
		abort();
	}
	return nums[perm_count++];
}

/*==================================================================+
 | ROUTINE NAME
 |      Lastname
 | DESCRIPTION
 |      TPC-C Lastname Function.
 | ARGUMENTS 
 |      num  - non-uniform random number
 |      name - last name string
 +==================================================================*/
int Lastname(num, name)
  int num;
  char *name;
{
  static int  nl[] = {3, 5, 4, 3, 4, 3, 4, 5, 5, 4};
  static char *n[] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

  int len = 0;

  memcpy(name + len, n[num/100], nl[num/100]);
  len += nl[num/100];
  memcpy(name + len, n[(num/10)%10], nl[(num/10)%10]);
  len += nl[(num/10)%10];
  memcpy(name + len, n[num%10], nl[num%10]);
  len += nl[num%10];

  name[len] = 0;
 
 return len;
}

