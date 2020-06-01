#include "redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#define MAXKL 112
#define MAXVL 1024

int C19Close_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    size_t l0, l1, l2;
    const char *p0, *p1, *p2; 
    char k[MAXKL];
    
    if (argc != 3) return RedisModule_WrongArity(ctx);
    p0 = RedisModule_StringPtrLen(argv[0],&l0);
    p1 = RedisModule_StringPtrLen(argv[0],&l1);
    p2 = RedisModule_StringPtrLen(argv[0],&l2);
    if (sizeof(k) - 1 < l0 + l2) return RedisModule_ReplyWithError(ctx,"Key too long");
    memcpy((void *)k,(void *)p0,l0);
    k[l0] = ':';
    memcpy((void *)k + l0 + 1,(void *)p1,l1);
    RedisModuleString *rmk = RedisModule_CreateString(ctx,k,l0 + l1 + 1);
    RedisModuleKey *srckey = RedisModule_OpenKey(ctx,rmk,REDISMODULE_READ|REDISMODULE_WRITE);
    

}