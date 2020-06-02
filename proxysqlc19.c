#include "redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#define MAXKL 128
#define FL 64
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
	int rsret = RedisModule_StringToLongLong(argv[2], &depth);
    if (rsret != REDISMODULE_OK || depth < 0) return RedisModule_ReplyWithError(ctx,"Wrong depth");
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
    vp[sizeof(MAXVL) - 1] = CLOSE_MARKER;
    if (tid - depth > 0) {
        RedisModule_FreeString(ctx, rmk);
        l1 = sprintf(k + l0 + 1, "%ll", tid - depth);
        rmk = RedisModule_CreateString(ctx,k,l0 + l1 + 1);
        RedisModule_CloseKey(key);
        key = RedisModule_OpenKey(ctx,rmk,REDISMODULE_READ|REDISMODULE_WRITE);
        RedisModule_DeleteKey(key);
    }
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
        int rsret = RedisModule_StringToLongLong(argv[2], &depth);
        if (rsret != REDISMODULE_OK || depth < 0) return RedisModule_ReplyWithError(ctx,"Wrong depth");
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
        vp[sizeof(MAXVL) - 1] = CLOSE_MARKER;
        memcpy((void *)vp, (void *)p4, l4);
        vp[l4] = 0;
        vp += FL * 2;
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
        vp += FL * 2;
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