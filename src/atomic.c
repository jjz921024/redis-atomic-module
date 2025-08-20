#include "redismodule.h"

// HCAS key field old_value new_value
// -1: 字段不存在, 1: 执行成功, 0: 执行失败
int HashCAS_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 5) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && type != REDISMODULE_KEYTYPE_HASH) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR; // todo err
    } else if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, -1);
        return REDISMODULE_OK;
    }

    // 获取字段当前值
    RedisModuleString *oldval;
    RedisModule_HashGet(key, REDISMODULE_HASH_NONE, argv[2], &oldval, NULL);
    if (oldval == NULL) {
        RedisModule_ReplyWithLongLong(ctx, -1);
        return REDISMODULE_OK;
    }

    // todo: 直接使用argv
    RedisModuleString *expecte = RedisModule_CreateStringFromString(ctx, argv[3]);
    if (RedisModule_StringCompare(oldval, expecte) == 0) {
    } else {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    return REDISMODULE_OK;
}

// HCAD key field old_value
int HashCAD_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    RedisModule_AutoMemory(ctx);

    return REDISMODULE_OK;
}

// HSETEX key field value [EX/EXAT/PX/PXAT time]
int HashSetExpire_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    RedisModule_AutoMemory(ctx);

    return REDISMODULE_OK;
}

int ListLPopIFF_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    RedisModule_AutoMemory(ctx);

    return REDISMODULE_OK;
}

int ListRPopIFF_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    RedisModule_AutoMemory(ctx);

    return REDISMODULE_OK;
}

// 创建module上自定义的命令
int Module_CreateCommands(RedisModuleCtx *ctx) {
#define CREATE_CMD(name, tgt, attr)                                                       \
    do {                                                                                  \
        if (RedisModule_CreateCommand(ctx, name, tgt, attr, 1, 1, 1) != REDISMODULE_OK) { \
            return REDISMODULE_ERR;                                                       \
        }                                                                                 \
    } while (0);

    CREATE_CMD("hcas", HashCAS_RedisCommand, "write use-memory fast @hash")
    CREATE_CMD("hcad", HashCAD_RedisCommand, "write fast @hash")
    CREATE_CMD("hsetex", HashSetExpire_RedisCommand, "write use-memory fast @hash")
    CREATE_CMD("lpopiff", ListLPopIFF_RedisCommand, "write fast @list")
    CREATE_CMD("rpopiff", ListRPopIFF_RedisCommand, "write fast @list")

    return REDISMODULE_OK;
}

int __attribute__((visibility("default"))) RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "atomic-module", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        RedisModule_Log(ctx, "warning", "load atomic module error");
        return REDISMODULE_ERR;
    }

    if (REDISMODULE_ERR == Module_CreateCommands(ctx)) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
