/*
 * tpc.h
 * definitions for tpcc loading program && transactions
 */

#ifdef __cplusplus
extern "C" {
#endif

/*
 * correct values
 */
#define MAXITEMS      100000 
#define CUST_PER_DIST 3000 
#define DIST_PER_WARE 10 
#define ORD_PER_DIST  3000
/*
 */

/* 
 * small values

#define MAXITEMS 	1000
#define CUST_PER_DIST 	30
#define DIST_PER_WARE	3
#define ORD_PER_DIST	30

 */ 

#define INVALID_W_ID 0x7FFFFFFF
#define INVALID_D_ID 0x7FFFFFFF
#define INVALID_I_ID 0x7FFFFFFF
#define INVALID_C_ID 0
#define INVALID_O_ID 0

#define SQLBUF_SIZE 65535

/* definitions for new order transaction */
#define MAX_NUM_ITEMS 15
#define MAX_ITEM_LEN  24

#define swap_int(a,b) {int tmp; tmp=a; a=b; b=tmp;}

/*
 * hack MakeAddress() into a macro so that we can pass Oracle
 * VARCHARs instead of char *s
 */
#define MakeAddressMacro(str1,str2,city,state,zip) \
{int tmp; \
 tmp = MakeAlphaString(10,20,str1.arr); \
 str1.len = tmp; \
 tmp = MakeAlphaString(10,20,str2.arr); \
 str2.len = tmp; \
 tmp = MakeAlphaString(10,20,city.arr); \
 city.len = tmp; \
 tmp = MakeAlphaString(2,2,state.arr); \
 state.len = tmp; \
 tmp = MakeNumberString(9,9,zip.arr); \
 zip.len = tmp;}

/*
 * while we're at it, wrap MakeAlphaString() and MakeNumberString()
 * in a similar way
 */
#define MakeAlphaStringMacro(x,y,str) \
{int tmp; tmp = MakeAlphaString(x,y,str.arr); str.len = tmp;}
#define MakeNumberStringMacro(x,y,str) \
{int tmp; tmp = MakeNumberString(x,y,str.arr); str.len = tmp;}

/*
 * likewise, for Lastname()
 * counts on Lastname() producing null-terminated strings
 */
#define LastnameMacro(num,str) \
{str.len = Lastname(num, str.arr);}

extern long count_ware;
  
/* Functions */
    
void	 GenerateAll(const char *dname);
void         LoadItems();
void         LoadWare();   
void         LoadCust();
void         LoadOrd();
void         LoadNewOrd();   
int          Stock();
int          District();
void         Customer();
void         Orders();
void         New_Orders();
void         MakeAddress();
int	     MakeAddressRecord();
void         Error();

#ifdef __STDC__
extern int num_conn;

void debugTpccInfo(const char *info, int rows);
#define DEBUGTPCCINFO(a, b) \
	if (num_conn == 1) {    \
		debugTpccInfo(a, b); \
	}

void SetSeed (int seed);
void SetThreadSeed();
void setThreadId(int tnum);
unsigned int db_rand(unsigned int adj);
int RandomNumber (int min, int max);
int NURand (unsigned A, unsigned x, unsigned y);
int MakeAlphaString (int x, int y, char str[]);
int MakeNumberString (int x, int y, char str[]);
int gettimestamp (char str[], char *format, size_t n);
void InitPermutation (void);
int GetPermutation (void);
int Lastname(int num, char* name);

#endif /* __STDC__ */
    
#ifdef __cplusplus
}
#endif
