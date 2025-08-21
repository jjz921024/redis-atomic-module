#include "atomic.h"

#include <assert.h>
#include <string.h>
#include <strings.h>

#include "redismodule.h"

#define MODULE_EXPIRE_NO_FLAG 0
#define MODULE_EXPIRE_EX_FLAG (1 << 0)
#define MODULE_EXPIRE_PX_FLAG (1 << 1)
#define MODULE_EXPIRE_ABS_FLAG (1 << 2)

static int mstringcasecmp(const RedisModuleString *rs1, const char *s2) {
    size_t n1 = strlen(s2);
    size_t n2;
    const char *s1 = RedisModule_StringPtrLen(rs1, &n2);
    if (n1 != n2) {
        return -1;
    }
    return strncasecmp(s1, s2, n1);
}

// -1: 字段不存在, 1: 更新成功, 0: 更新失败
int hashFieldCompareGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int arity) {
    RedisModule_AutoMemory(ctx);
    if (argc != arity) {
        return RedisModule_WrongArity(ctx);
    }

    int is_delete = 0;
    if (mstringcasecmp(argv[0], "HCAD") == 0) {
        is_delete = 1;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && type != REDISMODULE_KEYTYPE_HASH) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_OK;
    } else if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, -1);
        return REDISMODULE_OK;
    }

    // 获取字段当前值
    RedisModuleString *old_val;
    RedisModule_HashGet(key, REDISMODULE_HASH_NONE, argv[2], &old_val, NULL);
    if (old_val == NULL) {
        RedisModule_ReplyWithLongLong(ctx, -1);
        return REDISMODULE_OK;
    }

    if (RedisModule_StringCompare(old_val, argv[3]) == 0) {
        // action by cmd
        if (is_delete) {
            RedisModule_HashSet(key, REDISMODULE_HASH_NONE, argv[2], REDISMODULE_HASH_DELETE, NULL);
            RedisModule_Replicate(ctx, "HDEL", "ss", argv[1], argv[2]);
        } else {
            RedisModule_HashSet(key, REDISMODULE_HASH_NONE, argv[2], argv[4], NULL);
            RedisModule_Replicate(ctx, "HSET", "sss", argv[1], argv[2], argv[4]);
        }
        RedisModule_ReplyWithLongLong(ctx, 1);
    } else {
        RedisModule_ReplyWithLongLong(ctx, 0);
    }

    return REDISMODULE_OK;
}

// HCAS key field expected_value new_value
int HashCASRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return hashFieldCompareGenericCommand(ctx, argv, argc, 5);
}

// HCAD key field expected_value
int HashCADRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return hashFieldCompareGenericCommand(ctx, argv, argc, 4);
}

/**
 * 安全消费 精确一次语义
 * key不存在返回nil, 队头元素不匹配返回0
 */
int popIfGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    // 只允许 eq 或 ne
    if (mstringcasecmp(argv[2], "eq") != 0 && mstringcasecmp(argv[2], "ne") != 0) {
        RedisModule_ReplyWithError(ctx, ERRORMSG_INVALID_FLAG);
        return REDISMODULE_OK;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && type != REDISMODULE_KEYTYPE_LIST) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_OK;
    } else if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
        return REDISMODULE_OK;
    }

    int is_left = mstringcasecmp(argv[0], "LPOPIF") == 0;
    const char *index = is_left ? "0" : "-1";
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "LINDEX", "sc", argv[1], index);
    RedisModuleString *old_value = RedisModule_CreateStringFromCallReply(reply);

    // 相等为0
    int is_flag_eq = mstringcasecmp(argv[2], "eq") == 0 ? 0 : 1;
    int is_val_eq = RedisModule_StringCompare(old_value, argv[3]) == 0 ? 0 : 1;
    if (is_val_eq == is_flag_eq) {
        const char *pop_cmd = is_left ? "LPOP" : "RPOP";
        RedisModuleCallReply *pop_element = RedisModule_Call(ctx, pop_cmd, "s!", argv[1]);
        assert(pop_element != NULL);
        RedisModuleString *value = RedisModule_CreateStringFromCallReply(pop_element);
        RedisModule_ReplyWithString(ctx, value);
    } else {
        RedisModule_ReplyWithLongLong(ctx, 0);
    }

    return REDISMODULE_OK;
}

// LPOPIF key [eq/ne] value
// 判断队头元素是否满足条件
int LPopIfRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return popIfGenericCommand(ctx, argv, argc);
}

// RPOPIF key [eq/ne] value
int RPopIfRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return popIfGenericCommand(ctx, argv, argc);
}

/**
 * push时维护队列长度不超过 limit, 超过后自动执行rpop命令删除队尾元素至满足limit长度
 *
 * 如果队列长度小于 limit, 返回队列长度
 * 如果队列长度大于等于 limit, 返回所有删除的队尾元素
 */
int pushRingGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    // limit 必须是正整数
    long long limit;
    if (RedisModule_StringToLongLong(argv[2], &limit) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, ERRORMSG_NO_INT);
    } else if (limit <= 0) {
        return RedisModule_ReplyWithError(ctx, ERRORMSG_NO_POSITIVE_INT);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && type != REDISMODULE_KEYTYPE_LIST) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_OK;
    }

    const char *pop_cmd = "LPOP", *push_cmd = "RPUSH";
    int pop_direction = REDISMODULE_LIST_HEAD, push_direction = REDISMODULE_LIST_TAIL;
    if (mstringcasecmp(argv[0], "LPUSHRING") == 0) {
        pop_cmd = "RPOP", push_cmd = "LPUSH";
        pop_direction = REDISMODULE_LIST_TAIL, push_direction = REDISMODULE_LIST_HEAD;
    }

    // 获取当前队列长度, 判断是否超过限制
    long long len = RedisModule_ValueLength(key);
    if (len < limit) {
        RedisModule_ReplyWithLongLong(ctx, len + 1);
    } else {
        // 删除元素并返回客户端, 直到满足限制
        RedisModule_ReplyWithArray(ctx, len - limit + 1);
        while (len >= limit) {
            RedisModuleString *value = RedisModule_ListPop(key, pop_direction);
            RedisModule_ReplyWithString(ctx, value);
            RedisModule_Replicate(ctx, pop_cmd, "s", argv[1]);
            len--;
        }
    }

    // 添加元素入队
    RedisModule_ListPush(key, push_direction, argv[3]);
    RedisModule_Replicate(ctx, push_cmd, "ss", argv[1], argv[3]);
    return REDISMODULE_OK;
}

// LPushRing key len value
// 在队头添加元素, 在队尾删除元素
int LPushRingRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return pushRingGenericCommand(ctx, argv, argc);
}

// RPushRing key len value
int RPushRingRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return pushRingGenericCommand(ctx, argv, argc);
}

/**
 * push时, 判断push完成后长度是否将会超过指容量 cap
 * 如果会超过则返回错误, 不执行push请求
 * 否则执行push请求, 返回值与push命令一致
 */
int pushNotFullGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    long long capacity;
    if (RedisModule_StringToLongLong(argv[2], &capacity) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, ERRORMSG_NO_INT);
    } else if (capacity <= 0) {
        return RedisModule_ReplyWithError(ctx, ERRORMSG_NO_POSITIVE_INT);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && type != REDISMODULE_KEYTYPE_LIST) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_OK;
    }

    long long len = RedisModule_ValueLength(key);
    // 计算push后是否超过容量限制
    // 若超过返回负数, 代表共需多少容量才能完成本次push
    size_t num = argc - 3;
    if (len + num > capacity) {
        return RedisModule_ReplyWithLongLong(ctx, capacity - (len + num));
    }

    // 添加元素入队
    const char *push_cmd = "RPUSH";
    int direction = REDISMODULE_LIST_TAIL;
    if (mstringcasecmp(argv[0], "LPUSHNF") == 0) {
        direction = REDISMODULE_LIST_HEAD;
        push_cmd = "LPUSH";
    }

    for (size_t i = 3; i < argc; i++) {
        RedisModule_ListPush(key, direction, argv[i]);
        RedisModule_Replicate(ctx, push_cmd, "ss", argv[1], argv[i]);
    }

    RedisModule_ReplyWithLongLong(ctx, len + num);
    return REDISMODULE_OK;
}

// LPUSHNF key len value [value ...]
int LPushNFRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return pushNotFullGenericCommand(ctx, argv, argc);
}

// RPUSHNF key len value [value ...]
int RPushNFRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return pushNotFullGenericCommand(ctx, argv, argc);
}

typedef enum { CMP_EQ, CMP_NE, CMP_GT, CMP_LT, CMP_GTE, CMP_LTE, CMP_INVALID } CompareOp;

static CompareOp parseCompareOp(const RedisModuleString *op_str) {
    if (!mstringcasecmp(op_str, "eq")) return CMP_EQ;
    if (!mstringcasecmp(op_str, "ne")) return CMP_NE;
    if (!mstringcasecmp(op_str, "gt")) return CMP_GT;
    if (!mstringcasecmp(op_str, "lt")) return CMP_LT;
    if (!mstringcasecmp(op_str, "gte")) return CMP_GTE;
    if (!mstringcasecmp(op_str, "lte")) return CMP_LTE;
    return CMP_INVALID;
}

static int compareByOp(double a, double b, CompareOp op) {
    switch (op) {
        case CMP_EQ: return a == b;
        case CMP_NE: return a != b;
        case CMP_GT: return a > b;
        case CMP_LT: return a < b;
        case CMP_GTE: return a >= b;
        case CMP_LTE: return a <= b;
        default: return 0;
    }
}

int zPopIfGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    CompareOp op = parseCompareOp(argv[2]);
    if (op == CMP_INVALID) {
        RedisModule_ReplyWithError(ctx, ERRORMSG_INVALID_FLAG);
        return REDISMODULE_OK;
    }

    double target_score = 0;
    if (RedisModule_StringToDouble(argv[3], &target_score) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, ERRORMSG_NO_FLOAT);
        return REDISMODULE_OK;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && type != REDISMODULE_KEYTYPE_ZSET) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_OK;
    } else if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithEmptyArray(ctx);
        return REDISMODULE_OK;
    }

    const char *range_cmd = mstringcasecmp(argv[0], "ZPOPMAXIF") == 0 ? "ZREVRANGE" : "ZRANGE";
    RedisModuleCallReply *reply = RedisModule_Call(ctx, range_cmd, "sllc", argv[1], 0, 0, "WITHSCORES");
    // 取score
    RedisModuleCallReply *score_reply = RedisModule_CallReplyArrayElement(reply, 1);
    RedisModuleString *score_str = RedisModule_CreateStringFromCallReply(score_reply);
    double score = 0;
    if (RedisModule_StringToDouble(score_str, &score) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, ERRORMSG_NO_FLOAT);
        return REDISMODULE_OK;
    }

    if (compareByOp(score, target_score, op)) {
        const char *pop_cmd = mstringcasecmp(argv[0], "ZPOPMAXIF") == 0 ? "ZPOPMAX" : "ZPOPMIN";
        RedisModuleCallReply *pop_element = RedisModule_Call(ctx, pop_cmd, "s!", argv[1]);
        assert(pop_element != NULL);
        RedisModule_ReplyWithCallReply(ctx, pop_element);
    } else {
        RedisModule_ReplyWithLongLong(ctx, 0);
    }

    return REDISMODULE_OK;
}

// ZPOPMAXIF key [gt/lt/gte/lte/eq/ne] score
int ZPopMaxIfRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return zPopIfGenericCommand(ctx, argv, argc);
}

// ZPOPMINIF key [gt/lt/gte/lte/eq/ne] score
int ZPopMinIfRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return zPopIfGenericCommand(ctx, argv, argc);
}

// 从倒数第二位参数开始解析
static int parseExpireFlags(RedisModuleString **argv, int argc, int *ex_flag, RedisModuleString **time_str) {
    int flags = MODULE_EXPIRE_NO_FLAG;
    for (int i = argc - 2; i < argc; i++) {
        RedisModuleString *next = (i == argc - 1) ? NULL : argv[i + 1];
        if (time_str != NULL && !mstringcasecmp(argv[i], "ex") && next) {
            if (flags & (MODULE_EXPIRE_EX_FLAG | MODULE_EXPIRE_PX_FLAG)) {
                return REDISMODULE_ERR;
            }
            flags |= MODULE_EXPIRE_EX_FLAG;
            *time_str = next;
            i++;
        } else if (time_str != NULL && !mstringcasecmp(argv[i], "exat") && next) {
            if (flags & (MODULE_EXPIRE_EX_FLAG | MODULE_EXPIRE_PX_FLAG)) {
                return REDISMODULE_ERR;
            }
            flags |= MODULE_EXPIRE_EX_FLAG;
            flags |= MODULE_EXPIRE_ABS_FLAG;
            *time_str = next;
            i++;
        } else if (time_str != NULL && !mstringcasecmp(argv[i], "px") && next) {
            if (flags & (MODULE_EXPIRE_EX_FLAG | MODULE_EXPIRE_PX_FLAG)) {
                return REDISMODULE_ERR;
            }
            flags |= MODULE_EXPIRE_PX_FLAG;
            *time_str = next;
            i++;
        } else if (time_str != NULL && !mstringcasecmp(argv[i], "pxat") && next) {
            if (flags & (MODULE_EXPIRE_EX_FLAG | MODULE_EXPIRE_PX_FLAG)) {
                return REDISMODULE_ERR;
            }
            flags |= MODULE_EXPIRE_PX_FLAG;
            flags |= MODULE_EXPIRE_ABS_FLAG;
            *time_str = next;
            i++;
        } else {
            return REDISMODULE_ERR;
        }
    }

    *ex_flag = flags;
    return REDISMODULE_OK;
}

// 集合类型新增命令 + expire组合
int insertAndExpireGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int arity) {
    RedisModule_AutoMemory(ctx);
    if ((arity > 0 && argc != arity) || (arity < 0 && argc < -arity)) {
        return RedisModule_WrongArity(ctx);
    }

    int is_hash = !mstringcasecmp(argv[0], "HSETEX");
    if (is_hash && argc % 2 != 0) {
        return RedisModule_WrongArity(ctx);
    }

    // 将倒数两个参数 解析为过期时间
    int expire_flag = MODULE_EXPIRE_NO_FLAG;
    RedisModuleString *time_str = NULL;
    if (parseExpireFlags(argv, argc, &expire_flag, &time_str) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, ERRORMSG_SYNTAX);
        return REDISMODULE_OK;
    } else if (expire_flag == MODULE_EXPIRE_NO_FLAG || time_str == NULL) {
        // 解析不到过期时间也是语法错误
        RedisModule_ReplyWithError(ctx, ERRORMSG_SYNTAX);
        return REDISMODULE_OK;
    }

    // 过期时间必须是正整数, 等于0无意义
    long long expire = 0;
    if ((RedisModule_StringToLongLong(time_str, &expire) != REDISMODULE_OK) || expire <= 0) {
        RedisModule_ReplyWithError(ctx, ERRORMSG_INVALID_EXPIRE_TIME);
        return REDISMODULE_OK;
    }

    // 去掉命令后两个EX字符
    size_t len = 0;
    const char *str = RedisModule_StringPtrLen(argv[0], &len);
    assert(len > 2);
    RedisModuleString *real_cmd = RedisModule_CreateString(ctx, str, len - 2);
    const char *cmd_str = RedisModule_StringPtrLen(real_cmd, &len);

    int pair_num = is_hash ? (argc - 4) / 2 : argc - 4;
    long long added = 0;
    for (size_t i = 0; i < pair_num; i++) {
        RedisModuleCallReply *reply = NULL;
        if (is_hash) {
            reply = RedisModule_Call(ctx, cmd_str, "sss!", argv[1], argv[i * 2 + 2], argv[i * 2 + 3]);
        } else {
            reply = RedisModule_Call(ctx, cmd_str, "ss!", argv[1], argv[i + 2]);
        }

        // 若返回不为int, 中断执行, 返回错误报文
        // 理论上第一次成功, 之后继续增加元素都不会失败
        if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER) {
            RedisModule_ReplyWithError(ctx, ERRORMSG_WRONGTYPE);
            return REDISMODULE_OK;
        }

        added += RedisModule_CallReplyInteger(reply);
    }

    if (expire_flag & MODULE_EXPIRE_EX_FLAG) {
        expire *= 1000;
    }
    if (expire_flag & MODULE_EXPIRE_ABS_FLAG) {
        // 绝对时间戳, 减去当前时间
        expire -= RedisModule_Milliseconds();
        if (expire < 0) {
            expire = 0;
        }
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    RedisModule_SetExpire(key, expire);
    RedisModule_Replicate(ctx, "PEXPIREAT", "sl", argv[1], (expire + RedisModule_Milliseconds()));

    // 特殊处理list类型, 获取新增后list长度
    if (!mstringcasecmp(argv[0], "LPUSHEX") || !mstringcasecmp(argv[0], "RPUSHEX")) {
        added = RedisModule_ValueLength(key);
    }

    RedisModule_ReplyWithLongLong(ctx, added);
    return REDISMODULE_OK;
}

// HSETEX key field value [field value ...] [EX/EXAT/PX/PXAT] time
int HashSetExRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return insertAndExpireGenericCommand(ctx, argv, argc, -6);
}

// LPUSHEX key value [value ...] [EX/EXAT/PX/PXAT] time
int LPushExRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return insertAndExpireGenericCommand(ctx, argv, argc, -5);
}

// RPUSHEX key value [value ...] [EX/EXAT/PX/PXAT] time
int RPushExRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return insertAndExpireGenericCommand(ctx, argv, argc, -5);
}

// SADDEX key value [value ...] [EX/EXAT/PX/PXAT] time
int SAddExRedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return insertAndExpireGenericCommand(ctx, argv, argc, -5);
}

// 创建module上自定义的命令
int Module_CreateCommands(RedisModuleCtx *ctx) {
#define CREATE_CMD(name, tgt, attr)                                                       \
    do {                                                                                  \
        if (RedisModule_CreateCommand(ctx, name, tgt, attr, 1, 1, 1) != REDISMODULE_OK) { \
            return REDISMODULE_ERR;                                                       \
        }                                                                                 \
    } while (0);

    CREATE_CMD("hcas", HashCASRedisCommand, "write fast")
    CREATE_CMD("hcad", HashCADRedisCommand, "write fast")
    CREATE_CMD("lpushring", LPushRingRedisCommand, "write")
    CREATE_CMD("rpushring", RPushRingRedisCommand, "write")
    CREATE_CMD("lpushnf", LPushNFRedisCommand, "write fast")
    CREATE_CMD("rpushnf", RPushNFRedisCommand, "write fast")
    CREATE_CMD("lpopif", LPopIfRedisCommand, "write fast")
    CREATE_CMD("rpopif", RPopIfRedisCommand, "write fast")
    CREATE_CMD("zpopmaxif", ZPopMaxIfRedisCommand, "write fast")
    CREATE_CMD("zpopminif", ZPopMinIfRedisCommand, "write fast")
    // 集合类型新增命令 + expire组合
    CREATE_CMD("hsetex", HashSetExRedisCommand, "write fast")
    CREATE_CMD("lpushex", LPushExRedisCommand, "write fast")
    CREATE_CMD("rpushex", RPushExRedisCommand, "write fast")
    CREATE_CMD("saddex", SAddExRedisCommand, "write fast")

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
