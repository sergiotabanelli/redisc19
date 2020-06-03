#include "redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#define MAXKL 128
#define FL 64
#define SIDP 0
#define LGP 1
#define GP 2
#define MAXINCR 20
#define MAXVL 1024
#define CLOSE_MARKER '#'
int C19Close_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    size_t l0, l1;
    const char *p0, *p1; 
    char k[MAXKL];
    const char *rerr = NULL;
    if (argc != 3) return RedisModule_WrongArity(ctx);
    long long tid;
    long long depth;
	int rsret = RedisModule_StringToLongLong(argv[1], &tid);
    if (rsret != REDISMODULE_OK || tid <= 0) return RedisModule_ReplyWithError(ctx,"Wrong token");
	rsret = RedisModule_StringToLongLong(argv[2], &depth);
    if (rsret != REDISMODULE_OK || depth <= 0) return RedisModule_ReplyWithError(ctx,"Wrong depth");
    p0 = RedisModule_StringPtrLen(argv[0],&l0);
    p1 = RedisModule_StringPtrLen(argv[1],&l1);
    if (sizeof(k) < l0 + l1 + 1) return RedisModule_ReplyWithError(ctx,"Key too long");
    memcpy((void *)k,(void *)p0,l0);
    k[l0] = ':';
    memcpy((void *)k + l0 + 1,(void *)p1,l1);
    RedisModuleString *rmk = RedisModule_CreateString(ctx,k,l0 + l1 + 1);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,rmk,REDISMODULE_READ|REDISMODULE_WRITE);
    int kt = RedisModule_KeyType(key);
    if (kt != REDISMODULE_KEYTYPE_STRING ||
        kt == REDISMODULE_KEYTYPE_EMPTY)
    {
        rerr = REDISMODULE_ERRORMSG_WRONGTYPE;
        goto err;
    }
    size_t lv;
    char *vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
    if (lv != MAXVL) {
        RedisModule_StringTruncate(key,MAXVL);
        vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
    }
    vp[MAXVL - 1] = CLOSE_MARKER;
    if (tid - depth > 0) {
        RedisModule_FreeString(ctx, rmk);
        l1 = sprintf(k + l0 + 1, "%ll", tid - depth);
        rmk = RedisModule_CreateString(ctx,k,l0 + l1 + 1);
        RedisModule_CloseKey(key);
        key = RedisModule_OpenKey(ctx,rmk,REDISMODULE_READ|REDISMODULE_WRITE);
        RedisModule_DeleteKey(key);
    }
    vp[FL * GP] = 0; // Gtid empty
err:
    if (key) RedisModule_CloseKey(key);
    if (rmk) RedisModule_FreeString(ctx, rmk);
    if (rerr) return RedisModule_ReplyWithError(ctx, rerr);
    RedisModule_ReplyWithSimpleString(ctx,"OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

int C19Save_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    char k[MAXKL];
    const char *rerr = NULL;
    RedisModuleString *rmk = NULL;
    RedisModuleKey *key = NULL;
    size_t lv;
    char *vp;
    if (argc != 3 && argc != 6) return RedisModule_WrongArity(ctx);
    if (argc == 6) { // Write and read ctx
        size_t l0, l1, l3, l4, l5;
        const char *p0, *p1, *p3, *p4, *p5; 
        long long tid;
        long long depth;
        int rsret = RedisModule_StringToLongLong(argv[1], &tid);
        if (rsret != REDISMODULE_OK || tid <= 0) return RedisModule_ReplyWithError(ctx,"Wrong token");
        rsret = RedisModule_StringToLongLong(argv[2], &depth);
        if (rsret != REDISMODULE_OK || depth <= 0) return RedisModule_ReplyWithError(ctx,"Wrong depth");
        p0 = RedisModule_StringPtrLen(argv[0],&l0); // Write key
        p1 = RedisModule_StringPtrLen(argv[1],&l1); // Token
        p3 = RedisModule_StringPtrLen(argv[3],&l3); // Read key
        p4 = RedisModule_StringPtrLen(argv[4],&l4); // Server id
        p5 = RedisModule_StringPtrLen(argv[5],&l5); // Gtid
        if (sizeof(k) < l0 + l1 + 1) return RedisModule_ReplyWithError(ctx,"Write key too long");
        if (sizeof(k) < l3) return RedisModule_ReplyWithError(ctx,"Read key too long");
        if (FL < l4 + 1) return RedisModule_ReplyWithError(ctx,"Server id too long");
        if (FL < l5 + 1) return RedisModule_ReplyWithError(ctx,"Gtid too long");
        memcpy((void *)k,(void *)p0,l0);
        k[l0] = ':';
        memcpy((void *)k + l0 + 1,(void *)p1,l1);
        rmk = RedisModule_CreateString(ctx,k,l0 + l1 + 1);
        key = RedisModule_OpenKey(ctx,rmk,REDISMODULE_READ|REDISMODULE_WRITE);
        int kt = RedisModule_KeyType(key);
        if (kt != REDISMODULE_KEYTYPE_STRING ||
            kt == REDISMODULE_KEYTYPE_EMPTY)
        {
            rerr = REDISMODULE_ERRORMSG_WRONGTYPE;
            goto err;
        }
        vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
        if (lv != MAXVL) {
            RedisModule_StringTruncate(key,MAXVL);
            vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
        }
        vp[MAXVL - 1] = CLOSE_MARKER;
        memcpy((void *)vp, (void *)p4, l4);
        vp[l4] = 0;
        vp += FL * GP;
        memcpy((void *)vp, (void *)p5, l5);
        vp[l5] = 0;
        if (tid - depth > 0) {
            RedisModule_FreeString(ctx, rmk);
            l1 = sprintf(k + l0 + 1, "%ll", tid - depth);
            rmk = RedisModule_CreateString(ctx,k,l0 + l1 + 1);
            RedisModule_CloseKey(key);
            key = RedisModule_OpenKey(ctx,rmk,REDISMODULE_READ|REDISMODULE_WRITE);
            RedisModule_DeleteKey(key);
        }
        RedisModule_CloseKey(key);
        RedisModule_FreeString(ctx, rmk);
        rmk = NULL;
        key = RedisModule_OpenKey(ctx,argv[3],REDISMODULE_READ|REDISMODULE_WRITE);
        vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
        if (lv != MAXVL) {
            RedisModule_StringTruncate(key,MAXVL);
            vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
        }
        memcpy((void *)vp, (void *)p4, l4);
        vp[l4] = 0;
        vp += FL * 2;
        memcpy((void *)vp, (void *)p5, l5);
        vp[l5] = 0;
    } else { // Only read ctx
        size_t l0, l1, l2;
        const char *p0, *p1, *p2; 
        p0 = RedisModule_StringPtrLen(argv[0],&l0); // Read key
        p1 = RedisModule_StringPtrLen(argv[1],&l1); // Server id
        p2 = RedisModule_StringPtrLen(argv[2],&l2); // Gtid
        if (sizeof(k) < l0) return RedisModule_ReplyWithError(ctx,"Read key too long");
        if (FL < l1 + 1) return RedisModule_ReplyWithError(ctx,"Server id too long");
        if (FL < l2 + 1) return RedisModule_ReplyWithError(ctx,"Gtid too long");
        key = RedisModule_OpenKey(ctx,argv[0],REDISMODULE_READ|REDISMODULE_WRITE);
        vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
        if (lv != MAXVL) {
            RedisModule_StringTruncate(key,MAXVL);
            vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
        }
        memcpy((void *)vp, (void *)p1, l1);
        vp[l1] = 0;
        vp += FL * GP;
        memcpy((void *)vp, (void *)p2, l2);
        vp[l2] = 0;
    }
err:
    if (key) RedisModule_CloseKey(key);
    if (rmk) RedisModule_FreeString(ctx, rmk);
    if (rerr) return RedisModule_ReplyWithError(ctx, rerr);
    RedisModule_ReplyWithSimpleString(ctx,"OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

int C19GetWrite_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    size_t l0, l1;
    const char *p0, *p1; 
    char k[MAXKL];
    const char *rerr = NULL;
    if (argc != 2) return RedisModule_WrongArity(ctx);
    long long depth;
    long long tid, prev = 0;
	int rsret = RedisModule_StringToLongLong(argv[1], &depth);
    if (rsret != REDISMODULE_OK || depth <= 0) return RedisModule_ReplyWithError(ctx,"Wrong depth");
    p0 = RedisModule_StringPtrLen(argv[0],&l0);
    if (sizeof(k) < l0 + MAXINCR + 1) return RedisModule_ReplyWithError(ctx,"Key too long");
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[0],REDISMODULE_READ|REDISMODULE_WRITE);
    int kt = RedisModule_KeyType(key);
    if (kt != REDISMODULE_KEYTYPE_STRING &&
        kt != REDISMODULE_KEYTYPE_EMPTY)
    {
        rerr = REDISMODULE_ERRORMSG_WRONGTYPE;
        goto err;
    }
    size_t lv;
    char *vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
    if (lv != MAXINCR) {
        RedisModule_StringTruncate(key,MAXINCR);
        vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
    }
    vp[MAXINCR - 1] = 0;
	tid = strtoll(vp, NULL, 10);
    if (tid < 0) {
        rerr = "Negative token";
        goto err;
    }
    tid++;
    sprintf(vp, "%ll", tid);
    RedisModule_CloseKey(key);
    key = NULL;
    long long i = tid;
	long long ii = tid > depth ? tid - depth : 1;
    memcpy((void *)k,(void *)p0,l0);
    k[l0] = ':';
    char *kk = k + l0 + 1;
    RedisModuleString *rmk = NULL;
    char v[MAXVL];
    v[FL * SIDP] = 0;
    v[FL * LGP] = 0;
    v[FL * GP] = 0;
    v[MAXVL - 1] = 0;
    long long pg = 0;
    long long cg = 0;
	while(i >= ii) {
        l1 = sprintf(kk,"%ll",i);
        if (key) RedisModule_CloseKey(key);
        if (rmk) RedisModule_FreeString(ctx, rmk);
        rmk = RedisModule_CreateString(ctx,kk,l0 + l1 + 1);
        RedisModuleKey *key = RedisModule_OpenKey(ctx,rmk,REDISMODULE_READ);
        kt = RedisModule_KeyType(key);
        if (kt == REDISMODULE_KEYTYPE_STRING) {
            vp = RedisModule_StringDMA(key,&lv,REDISMODULE_READ);
            if (lv != MAXVL) {
                rerr = "Wrong value length";
                goto err;
            }
            prev = i;
            if (v[FL * LGP] == 0 && vp[FL * LGP]) {
                strncpy(v + (FL * LGP),vp + (FL * LGP),FL);
            }
            if (v[FL * SIDP] == 0) {
                strncpy(v + (FL * SIDP),vp + (FL * SIDP),FL);
            } else {
                if (strncmp(v[FL * SIDP],vp[FL * SIDP],FL)) {
                    break;
                }
            }
            v[MAXVL - 1] = vp[MAXVL - 1];
            if (vp[MAXVL - 1] != CLOSE_MARKER) {
                break;
            }
            if (v[FL * GP] == 0 && vp[FL * GP]) {
                strncpy(v + (FL * GP),vp + (FL * GP),FL);
                char *vt = strchr(v + (FL * GP), ':');
                if (!vt || (vt - (v + (FL * GP))) > FL) {
                    rerr = "Wrong gtid";
                    goto err;
                }
	            pg = strtoll(vt + 1, NULL, 10);
                if (pg <= 0) {
                    rerr = "Wrong gtid";
                    goto err;
                }
            } else if (vp[FL * GP]) {
                char *vpt = strchr(vp + (FL * GP), ':');
                if (!vpt || (vpt - (vp + (FL * GP))) > FL) {
                    rerr = "Wrong gtid";
                    goto err;
                }
	            cg = strtoll(vpt + 1, NULL, 10);
                if (cg <= 0) {
                    rerr = "Wrong gtid";
                    goto err;
                }
                if (cg > pg) {
                    strncpy(v + (FL * GP),vp + (FL * GP),FL);
                    pg = cg;
                }
            }            
        } else if (prev || kt != REDISMODULE_KEYTYPE_EMPTY) {
            rerr = REDISMODULE_ERRORMSG_WRONGTYPE;
            goto err;
        }
    }





    memcpy((void *)k,(void *)p0,l0);
    k[l0] = ':';
    memcpy((void *)k + l0 + 1,(void *)p1,l1);
    RedisModuleString *rmk = RedisModule_CreateString(ctx,k,l0 + l1 + 1);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,rmk,REDISMODULE_READ|REDISMODULE_WRITE);
    int kt = RedisModule_KeyType(key);
    if (kt != REDISMODULE_KEYTYPE_STRING ||
        kt == REDISMODULE_KEYTYPE_EMPTY)
    {
        rerr = REDISMODULE_ERRORMSG_WRONGTYPE;
        goto err;
    }
    size_t lv;
    char *vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
    if (lv != MAXVL) {
        RedisModule_StringTruncate(key,MAXVL);
        vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
    }
    vp[MAXVL - 1] = CLOSE_MARKER;
    if (tid - depth > 0) {
        RedisModule_FreeString(ctx, rmk);
        l1 = sprintf(k + l0 + 1, "%ll", tid - depth);
        rmk = RedisModule_CreateString(ctx,k,l0 + l1 + 1);
        RedisModule_CloseKey(key);
        key = RedisModule_OpenKey(ctx,rmk,REDISMODULE_READ|REDISMODULE_WRITE);
        RedisModule_DeleteKey(key);
    }
    vp[FL * GP] = 0; // Gtid empty
err:
    if (key) RedisModule_CloseKey(key);
    if (rmk) RedisModule_FreeString(ctx, rmk);
    if (rerr) return RedisModule_ReplyWithError(ctx, rerr);
    RedisModule_ReplyWithSimpleString(ctx,"OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

int C19Validate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    char k[MAXKL];
    const char *rerr = NULL;
    RedisModuleString *rmk = NULL;
    RedisModuleKey *key = NULL;
    size_t lv;
    char *vp;
    if (argc != 4 && argc != 5) return RedisModule_WrongArity(ctx);
    long long tid;
    long long prev;    
    int rsret = RedisModule_StringToLongLong(argv[0], &tid);
    if (rsret != REDISMODULE_OK || tid <= 0) return RedisModule_ReplyWithError(ctx,"Wrong token");
    rsret = RedisModule_StringToLongLong(argv[1], &prev);
    if (rsret != REDISMODULE_OK || prev >= tid) return RedisModule_ReplyWithError(ctx,"Wrong prev");
//DA QUIIIIIIIIIIIIIII
    if (argc == 6) { // Write and read ctx
        size_t l0, l1, l3, l4, l5;
        const char *p0, *p1, *p3, *p4, *p5; 
        long long tid;
        long long depth;
        int rsret = RedisModule_StringToLongLong(argv[1], &tid);
        if (rsret != REDISMODULE_OK || tid <= 0) return RedisModule_ReplyWithError(ctx,"Wrong token");
        rsret = RedisModule_StringToLongLong(argv[2], &depth);
        if (rsret != REDISMODULE_OK || depth <= 0) return RedisModule_ReplyWithError(ctx,"Wrong depth");
        p0 = RedisModule_StringPtrLen(argv[0],&l0); // Write key
        p1 = RedisModule_StringPtrLen(argv[1],&l1); // Token
        p3 = RedisModule_StringPtrLen(argv[3],&l3); // Read key
        p4 = RedisModule_StringPtrLen(argv[4],&l4); // Server id
        p5 = RedisModule_StringPtrLen(argv[5],&l5); // Gtid
        if (sizeof(k) < l0 + l1 + 1) return RedisModule_ReplyWithError(ctx,"Write key too long");
        if (sizeof(k) < l3) return RedisModule_ReplyWithError(ctx,"Read key too long");
        if (FL < l4 + 1) return RedisModule_ReplyWithError(ctx,"Server id too long");
        if (FL < l5 + 1) return RedisModule_ReplyWithError(ctx,"Gtid too long");
        memcpy((void *)k,(void *)p0,l0);
        k[l0] = ':';
        memcpy((void *)k + l0 + 1,(void *)p1,l1);
        rmk = RedisModule_CreateString(ctx,k,l0 + l1 + 1);
        key = RedisModule_OpenKey(ctx,rmk,REDISMODULE_READ|REDISMODULE_WRITE);
        int kt = RedisModule_KeyType(key);
        if (kt != REDISMODULE_KEYTYPE_STRING ||
            kt == REDISMODULE_KEYTYPE_EMPTY)
        {
            rerr = REDISMODULE_ERRORMSG_WRONGTYPE;
            goto err;
        }
        vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
        if (lv != MAXVL) {
            RedisModule_StringTruncate(key,MAXVL);
            vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
        }
        vp[MAXVL - 1] = CLOSE_MARKER;
        memcpy((void *)vp, (void *)p4, l4);
        vp[l4] = 0;
        vp += FL * GP;
        memcpy((void *)vp, (void *)p5, l5);
        vp[l5] = 0;
        if (tid - depth > 0) {
            RedisModule_FreeString(ctx, rmk);
            l1 = sprintf(k + l0 + 1, "%ll", tid - depth);
            rmk = RedisModule_CreateString(ctx,k,l0 + l1 + 1);
            RedisModule_CloseKey(key);
            key = RedisModule_OpenKey(ctx,rmk,REDISMODULE_READ|REDISMODULE_WRITE);
            RedisModule_DeleteKey(key);
        }
        RedisModule_CloseKey(key);
        RedisModule_FreeString(ctx, rmk);
        rmk = NULL;
        key = RedisModule_OpenKey(ctx,argv[3],REDISMODULE_READ|REDISMODULE_WRITE);
        vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
        if (lv != MAXVL) {
            RedisModule_StringTruncate(key,MAXVL);
            vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
        }
        memcpy((void *)vp, (void *)p4, l4);
        vp[l4] = 0;
        vp += FL * 2;
        memcpy((void *)vp, (void *)p5, l5);
        vp[l5] = 0;
    } else { // Only read ctx
        size_t l0, l1, l2;
        const char *p0, *p1, *p2; 
        p0 = RedisModule_StringPtrLen(argv[0],&l0); // Read key
        p1 = RedisModule_StringPtrLen(argv[1],&l1); // Server id
        p2 = RedisModule_StringPtrLen(argv[2],&l2); // Gtid
        if (sizeof(k) < l0) return RedisModule_ReplyWithError(ctx,"Read key too long");
        if (FL < l1 + 1) return RedisModule_ReplyWithError(ctx,"Server id too long");
        if (FL < l2 + 1) return RedisModule_ReplyWithError(ctx,"Gtid too long");
        key = RedisModule_OpenKey(ctx,argv[0],REDISMODULE_READ|REDISMODULE_WRITE);
        vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
        if (lv != MAXVL) {
            RedisModule_StringTruncate(key,MAXVL);
            vp = RedisModule_StringDMA(key,&lv,REDISMODULE_WRITE);
        }
        memcpy((void *)vp, (void *)p1, l1);
        vp[l1] = 0;
        vp += FL * GP;
        memcpy((void *)vp, (void *)p2, l2);
        vp[l2] = 0;
    }
err:
    if (key) RedisModule_CloseKey(key);
    if (rmk) RedisModule_FreeString(ctx, rmk);
    if (rerr) return RedisModule_ReplyWithError(ctx, rerr);
    RedisModule_ReplyWithSimpleString(ctx,"OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}