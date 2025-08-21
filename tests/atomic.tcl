set testmodule [file normalize module_path/atomic-module.so]

start_server {tags {"atomic module"} overrides {bind 0.0.0.0}} {
    r module load $testmodule

    test {hcas basic} {
        r del hash_key
        assert_equal -1 [r hcas hash_key field val1 val2]
        r hset hash_key field val1
        assert_equal 1 [r hcas hash_key field val1 val2]
        assert_equal val2 [r hget hash_key field]
        assert_equal 0 [r hcas hash_key field val1 val3]
    }

    test {hcad basic} {
        r del hash_key
        assert_equal -1 [r hcad hash_key field val1]
        r hset hash_key field val1
        assert_equal 1 [r hcad hash_key field val1]
        assert_equal 0 [r hexists hash_key field]
        r hset hash_key field val2
        assert_equal 0 [r hcad hash_key field val1]
    }

    test {lpopif basic} {
        r del list_key
        r rpush list_key a b c
        assert_equal a [r lpopif list_key eq a]
        assert_equal 0 [r lpopif list_key eq x]
        assert_equal b [r lpopif list_key eq b]
        assert_equal c [r lpopif list_key eq c]
        assert_equal "" [r lpopif list_key eq c]

        r del list_key
        r rpush list_key a b c
        assert_equal a [r lpopif list_key ne x]
        assert_equal 0 [r lpopif list_key ne b]
        assert_equal b [r lpopif list_key ne x]
        assert_equal c [r lpopif list_key ne x]
        assert_equal "" [r lpopif list_key ne x]
    }

    test {rpopif basic} {
        r del list_key
        r rpush list_key a b c
        assert_equal c [r rpopif list_key eq c]
        assert_equal 0 [r rpopif list_key eq x]
        assert_equal b [r rpopif list_key eq b]
        assert_equal a [r rpopif list_key eq a]
        assert_equal "" [r rpopif list_key eq a]

        r del list_key
        r rpush list_key a b c
        assert_equal c [r rpopif list_key ne x]
        assert_equal 0 [r rpopif list_key ne b]
        assert_equal b [r rpopif list_key ne x]
        assert_equal a [r rpopif list_key ne x]
        assert_equal "" [r rpopif list_key ne x]
    }

    test {zpopmaxif basic} {
        r del zset_key
        r zadd zset_key 1 a 2 b 3 c

        assert_equal 0 [r zpopmaxif zset_key eq 1]
        assert_equal 0 [r zpopmaxif zset_key ne 3]
        assert_equal 0 [r zpopmaxif zset_key gte 5]
        assert_equal 0 [r zpopmaxif zset_key lte -1]

        assert_equal {c 3} [r zpopmaxif zset_key eq 3]
        assert_equal {b 2} [r zpopmaxif zset_key gt 1.5]
        assert_equal {a 1} [r zpopmaxif zset_key lt 2]
    }

    test {zpopminif basic} {
        r del zset_key
        r zadd zset_key 1 a 2 b 3 c

        assert_equal 0 [r zpopminif zset_key eq 3]
        assert_equal 0 [r zpopminif zset_key ne 1]
        assert_equal 0 [r zpopminif zset_key gte 5]
        assert_equal 0 [r zpopminif zset_key lte -1]

        assert_equal {a 1} [r zpopminif zset_key eq 1]
        assert_equal {b 2} [r zpopminif zset_key gt 1.5]
        assert_equal {c 3} [r zpopminif zset_key lt 5]
    }

    test {lpushring basic} {
        r del ring_key
        assert_equal 1 [r lpushring ring_key 3 x]
        assert_equal 2 [r lpushring ring_key 3 y]
        assert_equal 3 [r lpushring ring_key 3 z]
        assert_equal 3 [r llen ring_key]

        assert_equal x [r lpushring ring_key 3 a]
        assert_equal {a z y} [r lrange ring_key 0 -1]

        assert_equal y [r lpushring ring_key 3 b]
        assert_equal z [r lpushring ring_key 3 c]
        assert_equal {c b a} [r lrange ring_key 0 -1]

        # 缩容, 返回数组
        assert_equal {a b} [r lpushring ring_key 2 1]
        assert_equal {1 c} [r lrange ring_key 0 -1]

        # 扩容
        assert_equal 3 [r lpushring ring_key 3 2]
        assert_equal {2 1 c} [r lrange ring_key 0 -1]
    }

    test {rpushring basic} {
        r del ring_key
        assert_equal 1 [r rpushring ring_key 2 a]
        assert_equal 2 [r rpushring ring_key 2 b]
        assert_equal a [r rpushring ring_key 2 c]
        assert_equal {b c} [r lrange ring_key 0 -1]

        assert_equal b [r rpushring ring_key 2 x]
        assert_equal {c x} [r lrange ring_key 0 -1]

        # 扩容
        assert_equal 3 [r rpushring ring_key 5 y]
        assert_equal 4 [r rpushring ring_key 5 z]
        assert_equal {c x y z} [r lrange ring_key 0 -1]

        # 缩容, 返回数组
        assert_equal {c x y z} [r rpushring ring_key 1 1]
        assert_equal {1} [r lrange ring_key 0 -1]
    }

    test {lpushnf basic} {
        r del nf_key
        assert_equal 1 [r lpushnf nf_key 2 a]
        assert_equal 2 [r lpushnf nf_key 2 b]

        assert_equal -1 [r lpushnf nf_key 2 c]
        assert_equal {b a} [r lrange nf_key 0 -1]

        # 改变容量
        assert_equal 3 [r lpushnf nf_key 3 c]
        assert_equal {c b a} [r lrange nf_key 0 -1]

        assert_equal -3 [r lpushnf nf_key 1 d]
        assert_equal {c b a} [r lrange nf_key 0 -1]

        # multi
        assert_equal 5 [r lpushnf nf_key 6 e f]
        assert_equal -2 [r lpushnf nf_key 6 j k l]
    }

    test {rpushnf basic} {
        r del nf_key
        assert_equal 1 [r rpushnf nf_key 2 a]
        assert_equal 2 [r rpushnf nf_key 2 b]

        assert_equal -1 [r rpushnf nf_key 2 c]
        assert_equal {a b} [r lrange nf_key 0 -1]

        assert_equal 3 [r rpushnf nf_key 3 c]
        assert_equal {a b c} [r lrange nf_key 0 -1]

        assert_equal -3 [r rpushnf nf_key 1 c]
        assert_equal {a b c} [r lrange nf_key 0 -1]
    }

    test {hsetex basic} {
        r del hash_key
        set now [r time]
        assert_equal 1 [r hsetex hash_key field value EX 1]
        assert_equal value [r hget hash_key field]
        set ttl [r ttl hash_key]
        assert {$ttl > 0 && $ttl <= 2}

        # 值和过期时间都被更新
        assert_equal 0 [r hsetex hash_key field value2 EX 2]
        assert_equal value2 [r hget hash_key field]
        set ttl [r ttl hash_key]
        assert {$ttl > 1 && $ttl <= 2}

        after 2100
        assert_equal 0 [r exists hash_key]
    }

    test {hsetex multi fields} {
        r del hash_key
        assert_equal 2 [r hsetex hash_key f1 v1 f2 v2 EX 5]
        assert_equal v1 [r hget hash_key f1]
        assert_equal v2 [r hget hash_key f2]
        set ttl [r ttl hash_key]
        assert {$ttl > 0 && $ttl <= 5}

        assert_equal 1 [r hsetex hash_key f1 v1 f3 v3 EX 7]
        set ttl [r ttl hash_key]
        assert {$ttl > 5 && $ttl <= 7}
    }

    test {hsetex px pxat exat} {
        r del hash_key
        # PX 毫秒
        assert_equal 1 [r hsetex hash_key f1 v1 PX 100]
        set ttl [r pttl hash_key]
        assert {$ttl > 0 && $ttl <= 100}
        # PXAT 绝对毫秒
        set now [r time]
        set abs [expr {[lindex $now 0]*1000 + [lindex $now 1]/1000 + 500}]
        assert_equal 1 [r hsetex hash_key f2 v2 PXAT $abs]
        set ttl2 [r pttl hash_key]
        assert {$ttl2 > 0 && $ttl2 <= 500}
        # EXAT 绝对秒
        set abs2 [expr {[lindex $now 0] + 2}]
        assert_equal 1 [r hsetex hash_key f3 v3 EXAT $abs2]
        set ttl3 [r ttl hash_key]
        assert {$ttl3 > 0 && $ttl3 <= 2}
    }

    test {lpushex basic} {
        r del list_key
        assert_equal 1 [r lpushex list_key a EX 1]
        assert_equal {a} [r lrange list_key 0 -1]
        set ttl [r ttl list_key]
        assert {$ttl > 0 && $ttl <= 1}

        assert_equal 2 [r lpushex list_key b EX 2]
        assert_equal {b a} [r lrange list_key 0 -1]
        set ttl [r ttl list_key]
        assert {$ttl > 0 && $ttl <= 2}

        after 2100
        assert_equal 0 [r exists list_key]
    }

    test {lpushex multi values} {
        r del list_key
        assert_equal 2 [r lpushex list_key a b EX 1]
        assert_equal {b a} [r lrange list_key 0 -1]
        set ttl [r ttl list_key]
        assert {$ttl > 0 && $ttl <= 3}
    }

    test {lpushex px pxat exat} {
        r del list_key
        assert_equal 1 [r lpushex list_key a PX 100]
        set ttl [r pttl list_key]
        assert {$ttl > 0 && $ttl <= 100}

        set now [r time]
        set abs [expr {[lindex $now 0]*1000 + [lindex $now 1]/1000 + 500}]
        assert_equal 2 [r lpushex list_key b PXAT $abs]
        set ttl2 [r pttl list_key]
        assert {$ttl2 > 0 && $ttl2 <= 500}

        set abs2 [expr {[lindex $now 0] + 2}]
        assert_equal 3 [r lpushex list_key c EXAT $abs2]
        set ttl3 [r ttl list_key]
        assert {$ttl3 > 0 && $ttl3 <= 2}
    }

    test {rpushex basic} {
        r del list_key
        assert_equal 1 [r rpushex list_key a EX 1]
        assert_equal {a} [r lrange list_key 0 -1]
        set ttl [r ttl list_key]
        assert {$ttl > 0 && $ttl <= 1}

        assert_equal 2 [r rpushex list_key b EX 2]
        assert_equal {a b} [r lrange list_key 0 -1]
        set ttl [r ttl list_key]
        assert {$ttl > 0 && $ttl <= 2}

        after 2100
        assert_equal 0 [r exists list_key]
    }

    test {rpushex multi values} {
        r del list_key
        assert_equal 2 [r rpushex list_key a b EX 1]
        assert_equal {a b} [r lrange list_key 0 -1]
        set ttl [r ttl list_key]
        assert {$ttl > 0 && $ttl <= 3}
    }

    test {rpushex px pxat exat} {
        r del list_key
        assert_equal 1 [r rpushex list_key a PX 100]
        set ttl [r pttl list_key]
        assert {$ttl > 0 && $ttl <= 100}

        set now [r time]
        set abs [expr {[lindex $now 0]*1000 + [lindex $now 1]/1000 + 500}]
        assert_equal 2 [r rpushex list_key b PXAT $abs]
        set ttl2 [r pttl list_key]
        assert {$ttl2 > 0 && $ttl2 <= 500}

        set abs2 [expr {[lindex $now 0] + 2}]
        assert_equal 3 [r rpushex list_key c EXAT $abs2]
        set ttl3 [r ttl list_key]
        assert {$ttl3 > 0 && $ttl3 <= 2}
    }

    test {saddex basic} {
        r del set_key
        set now [r time]
        assert_equal 1 [r saddex set_key v1 EX 1]
        assert_equal 1 [r sismember set_key v1]
        set ttl [r ttl set_key]
        assert {$ttl > 0 && $ttl <= 2}

        assert_equal 0 [r saddex set_key v1 EX 2]
        assert_equal 1 [r saddex set_key v2 EX 2]
        assert_equal 1 [r sismember set_key v2]
        set ttl [r ttl set_key]
        assert {$ttl > 1 && $ttl <= 2}

        after 2100
        assert_equal 0 [r exists set_key]
    }

    test {saddex multi values} {
        r del set_key
        assert_equal 2 [r saddex set_key v1 v2 EX 5]
        assert_equal 1 [r sismember set_key v1]
        assert_equal 1 [r sismember set_key v2]
        set ttl [r ttl set_key]
        assert {$ttl > 0 && $ttl <= 5}

        assert_equal 1 [r saddex set_key v1 v3 EX 7]
        set ttl [r ttl set_key]
        assert {$ttl > 5 && $ttl <= 7}
    }

    test {saddex px pxat exat} {
        r del set_key
        assert_equal 1 [r saddex set_key v1 PX 100]
        set ttl [r pttl set_key]
        assert {$ttl > 0 && $ttl <= 100}

        set now [r time]
        set abs [expr {[lindex $now 0]*1000 + [lindex $now 1]/1000 + 500}]
        assert_equal 1 [r saddex set_key v2 PXAT $abs]
        set ttl2 [r pttl set_key]
        assert {$ttl2 > 0 && $ttl2 <= 500}

        set abs2 [expr {[lindex $now 0] + 2}]
        assert_equal 1 [r saddex set_key v3 EXAT $abs2]
        set ttl3 [r ttl set_key]
        assert {$ttl3 > 0 && $ttl3 <= 2}
    }

    # --- 语法错误与类型错误测试 ---

    test {hcas wrong} {
        assert_error "ERR wrong number*" {r hcas}
        assert_error "ERR wrong number*" {r hcas hash_key field val1}
        assert_error "ERR wrong number*" {r hcas hash_key field val1 val2 extra}

        r set hash_key "not_a_hash"
        assert_error "WRONGTYPE*" {r hcas hash_key field val1 val2}
    }

    test {hcad wrong} {
        assert_error "ERR wrong number*" {r hcad}
        assert_error "ERR wrong number*" {r hcad hash_key field}
        assert_error "ERR wrong number*" {r hcad hash_key field val1 extra}

        r set hash_key "not_a_hash"
        assert_error "WRONGTYPE*" {r hcad hash_key field val1}
    }

    test {lpopif wrong} {
        assert_error "ERR wrong number*" {r lpopif}
        assert_error "ERR wrong number*" {r lpopif list_key}
        assert_error "ERR wrong number*" {r lpopif list_key eq a extra}

        assert_error "ERR invalid comparison flag" {r lpopif list_key xx a}

        r set list_key "not_a_list"
        assert_error "WRONGTYPE*" {r lpopif list_key eq a}
        assert_error "WRONGTYPE*" {r lpopif list_key ne a}
    }

    test {rpopif wrong} {
        assert_error "ERR wrong number*" {r rpopif}
        assert_error "ERR wrong number*" {r rpopif list_key}
        assert_error "ERR wrong number*" {r rpopif list_key eq a extra}

        assert_error "ERR invalid comparison flag" {r rpopif list_key xx a}

        r set list_key "not_a_list"
        assert_error "WRONGTYPE*" {r rpopif list_key eq a}
        assert_error "WRONGTYPE*" {r rpopif list_key ne a}
    }

    test {zpopmaxif wrong} {
        assert_error "ERR wrong number*" {r zpopmaxif}
        assert_error "ERR wrong number*" {r zpopmaxif zset_key}
        assert_error "ERR wrong number*" {r zpopmaxif zset_key eq}

        assert_error "ERR invalid comparison flag" {r zpopmaxif zset_key xx 1}
        assert_error "ERR value is not a valid float" {r zpopmaxif zset_key eq a}

        r set zset_key "not_a_zset"
        assert_error "WRONGTYPE*" {r zpopmaxif zset_key eq 1}
        assert_error "WRONGTYPE*" {r zpopmaxif zset_key ne 1}
    }

    test {zpopminif wrong} {
        assert_error "ERR wrong number*" {r zpopminif}
        assert_error "ERR wrong number*" {r zpopminif zset_key}
        assert_error "ERR wrong number*" {r zpopminif zset_key eq}

        assert_error "ERR invalid comparison flag" {r zpopminif zset_key xx 1}
        assert_error "ERR value is not a valid float" {r zpopminif zset_key eq a}

        r set zset_key "not_a_zset"
        assert_error "WRONGTYPE*" {r zpopminif zset_key eq 1}
        assert_error "WRONGTYPE*" {r zpopminif zset_key ne 1}
    }

    test {lpushring wrong} {
        assert_error "ERR wrong number*" {r lpushring}
        assert_error "ERR wrong number*" {r lpushring ring_key 3}
        assert_error "ERR wrong number*" {r lpushring ring_key 3 x extra}

        r set ring_key "not_a_list"
        assert_error "WRONGTYPE*" {r lpushring ring_key 3 x}
    }

    test {lpushring param} {
        assert_error "ERR value is not an integer" {r lpushring ring_key foo x}
        assert_error "ERR param must be a positive integer" {r lpushring ring_key 0 x}
        assert_error "ERR param must be a positive integer" {r lpushring ring_key -1 x}
    }

    test {rpushring wrong} {
        assert_error "ERR wrong number*" {r rpushring}
        assert_error "ERR wrong number*" {r rpushring ring_key 2}
        assert_error "ERR wrong number*" {r rpushring ring_key 2 x extra}

        r set ring_key "not_a_list"
        assert_error "WRONGTYPE*" {r rpushring ring_key 2 x}
    }

    test {rpushring param} {
        assert_error "ERR value is not an integer" {r rpushring ring_key foo x}
        assert_error "ERR param must be a positive integer" {r rpushring ring_key 0 x}
        assert_error "ERR param must be a positive integer" {r rpushring ring_key -1 x}
    }

    test {lpushnf wrong} {
        assert_error "ERR wrong number*" {r lpushnf}
        assert_error "ERR wrong number*" {r lpushnf nf_key 2}

        r set nf_key "not_a_list"
        assert_error "WRONGTYPE*" {r lpushnf nf_key 2 a}
    }

    test {lpushnf param} {
        assert_error "ERR value is not an integer" {r lpushnf nf_key foo a}
        assert_error "ERR param must be a positive integer" {r lpushnf nf_key 0 a}
        assert_error "ERR param must be a positive integer" {r lpushnf nf_key -1 a}
    }

    test {rpushnf wrong} {
        assert_error "ERR wrong number*" {r rpushnf}
        assert_error "ERR wrong number*" {r rpushnf nf_key 2}

        r set nf_key "not_a_list"
        assert_error "WRONGTYPE*" {r rpushnf nf_key 2 a}
    }

    test {rpushnf param} {
        assert_error "ERR value is not an integer" {r rpushnf nf_key foo a}
        assert_error "ERR param must be a positive integer" {r rpushnf nf_key 0 a}
        assert_error "ERR param must be a positive integer" {r rpushnf nf_key -1 a}
    }

    test {hsetex wrong} {
        assert_error "ERR wrong number*" {r hsetex}
        assert_error "ERR wrong number*" {r hsetex hash_key field}
        assert_error "ERR wrong number*" {r hsetex hash_key field value}
        assert_error "ERR wrong number*" {r hsetex hash_key field value EX}

        assert_error "ERR syntax error" {r hsetex hash_key field value XXXX 10}
        assert_error "ERR syntax error" {r hsetex hash_key field value field value}

        assert_error "ERR invalid expire time*" {r hsetex hash_key field value EX not_int}
        assert_error "ERR invalid expire time*" {r hsetex hash_key field value EX 0}
        assert_error "ERR invalid expire time*" {r hsetex hash_key field value EX -5}
        assert_error "ERR invalid expire time*" {r hsetex hash_key field value EX 1.1}
    }

    test {lpushex wrong} {
        assert_error "ERR wrong number*" {r lpushex}
        assert_error "ERR wrong number*" {r lpushex list_key}
        assert_error "ERR wrong number*" {r lpushex list_key value}
        assert_error "ERR wrong number*" {r lpushex list_key value EX}

        assert_error "ERR syntax error" {r lpushex list_key value value XXXX}
        assert_error "ERR syntax error" {r lpushex list_key value XXXX 10}

        assert_error "ERR invalid expire time*" {r lpushex list_key value EX not_int}
        assert_error "ERR invalid expire time*" {r lpushex list_key value EX 0}
        assert_error "ERR invalid expire time*" {r lpushex list_key value EX -5}
        assert_error "ERR invalid expire time*" {r lpushex list_key value EX 1.1}
    }

    test {rpushex wrong} {
        assert_error "ERR wrong number*" {r rpushex}
        assert_error "ERR wrong number*" {r rpushex list_key}
        assert_error "ERR wrong number*" {r rpushex list_key value}
        assert_error "ERR wrong number*" {r rpushex list_key value EX}

        assert_error "ERR syntax error" {r rpushex list_key value value XXXX}
        assert_error "ERR syntax error" {r rpushex list_key value XXXX 10}

        assert_error "ERR invalid expire time*" {r rpushex list_key value EX not_int}
        assert_error "ERR invalid expire time*" {r rpushex list_key value EX 0}
        assert_error "ERR invalid expire time*" {r rpushex list_key value EX -5}
        assert_error "ERR invalid expire time*" {r rpushex list_key value EX 1.1}
    }

    test {saddex wrong} {
        assert_error "ERR wrong number*" {r saddex}
        assert_error "ERR wrong number*" {r saddex set_key}
        assert_error "ERR wrong number*" {r saddex set_key value}

        assert_error "ERR wrong number*" {r saddex set_key value EX}

        assert_error "ERR syntax error" {r saddex set_key value value XXXX}
        assert_error "ERR syntax error" {r saddex set_key value XXXX 10}

        assert_error "ERR invalid expire time*" {r saddex set_key value EX not_int}
        assert_error "ERR invalid expire time*" {r saddex set_key value EX 0}
        assert_error "ERR invalid expire time*" {r saddex set_key value EX -5}
        assert_error "ERR invalid expire time*" {r saddex set_key value EX 1.1}
    }

    # --- 边界场景 ---

    test {hcas field not exist} {
        r del hash_key
        r hset hash_key field1 val1
        assert_equal -1 [r hcas hash_key field2 val1 val2]
    }

    test {hcad field not exist} {
        r del hash_key
        r hset hash_key field1 val1
        assert_equal -1 [r hcad hash_key field2 val1]
    }

    test {popif empty} {
        r del list_key
        assert_equal "" [r lpopif list_key eq a]
        assert_equal "" [r rpopif list_key eq a]

        assert_equal "" [r lpopif list_key ne a]
        assert_equal "" [r rpopif list_key ne a]
    }

    # --- 主从同步 ---

    test {hcas hcad propagation} {
        r del hash_key
        set repl [attach_to_replication_stream]

        assert_equal -1 [r hcas hash_key field val1 val2]
        r hset hash_key field val1
        assert_equal 1 [r hcas hash_key field val1 val2]
        assert_equal val2 [r hget hash_key field]
        assert_equal 0 [r hcas hash_key field val1 val3]

        assert_equal 0 [r hcad hash_key field val1]
        assert_equal 1 [r hcad hash_key field val2]

        assert_replication_stream $repl {
            {select *}
            {hset hash_key field val1}
            {multi}
            {hset hash_key field val2}
            {exec}
            {multi}
            {hdel hash_key field}
            {exec}
        }
        close_replication_stream $repl
    }

    test {popif propagation} {
        r del list_key
        r rpush list_key a b c

        set repl [attach_to_replication_stream]

        assert_equal 0 [r lpopif list_key eq x]
        assert_equal 0 [r lpopif list_key ne a]
        assert_equal a [r lpopif list_key eq a]

        assert_replication_stream $repl {
            {select *}
            {multi}
            {lpop list_key}
            {exec}
        }
        close_replication_stream $repl
    }

    test {pushnf propagation} {
        r del nf_key
        assert_equal 2 [r lpushnf nf_key 2 a b]
        
        set repl [attach_to_replication_stream]

        assert_equal -1 [r lpushnf nf_key 2 c]
        assert_equal 4 [r lpushnf nf_key 4 c d]

        assert_replication_stream $repl {
            {select *}
            {multi}
            {lpush nf_key c}
            {lpush nf_key d}
            {exec}
        }
        close_replication_stream $repl
    }

    test {pushring propagation} {
        r del ring_key
        assert_equal 1 [r rpushring ring_key 2 a]
        assert_equal 2 [r rpushring ring_key 2 b]
        
        set repl [attach_to_replication_stream]

        assert_equal a [r rpushring ring_key 2 c]
        assert_equal {b c} [r rpushring ring_key 1 d]

        assert_replication_stream $repl {
            {select *}
            {multi}
            {lpop ring_key}
            {rpush ring_key c}
            {exec}
            {multi}
            {lpop ring_key}
            {lpop ring_key}
            {rpush ring_key d}
            {exec}
        }
        close_replication_stream $repl
    }

    test {zpopif propagation} {
        r del zset_key
        r zadd zset_key 1 a 2 b 3 c

        set repl [attach_to_replication_stream]

        assert_equal 0 [r zpopmaxif zset_key eq 1]
        assert_equal 0 [r zpopmaxif zset_key ne 3]
        assert_equal 0 [r zpopmaxif zset_key gte 5]
        assert_equal 0 [r zpopmaxif zset_key lte -1]

        assert_equal {c 3} [r zpopmaxif zset_key eq 3]
        assert_equal {b 2} [r zpopmaxif zset_key gt 1.5]
        assert_equal {a 1} [r zpopmaxif zset_key lt 2]

        assert_replication_stream $repl {
            {select *}
            {multi}
            {zpopmax zset_key}
            {exec}
            {multi}
            {zpopmax zset_key}
            {exec}
            {multi}
            {zpopmax zset_key}
            {exec}
        }
        close_replication_stream $repl
    }

    test {hash expire propagation} {
        r del hash_key
        
        set repl [attach_to_replication_stream]

        assert_equal 1 [r hsetex hash_key field val1 EX 1]
        assert_equal 0 [r hsetex hash_key field val2 EX 2]
        assert_equal 2 [r hsetex hash_key f1 v1 f2 v2 EX 5]

        assert_replication_stream $repl {
            {select *}
            {multi}
            {hset hash_key field val1}
            {pexpireat hash_key *}
            {exec}
            {multi}
            {hset hash_key field val2}
            {pexpireat hash_key *}
            {exec}
            {multi}
            {hset hash_key f1 v1}
            {hset hash_key f2 v2}
            {pexpireat hash_key *}
            {exec}
        }
        close_replication_stream $repl
    }

    test {List expire propagation} {
        r del list_key
        
        set repl [attach_to_replication_stream]

        assert_equal 1 [r rpushex list_key a EX 5]
        assert_equal 2 [r lpushex list_key b EX 5]
        assert_equal 4 [r rpushex list_key c d EX 5]

        assert_replication_stream $repl {
            {select *}
            {multi}
            {rpush list_key a}
            {pexpireat list_key *}
            {exec}
            {multi}
            {lpush list_key b}
            {pexpireat list_key *}
            {exec}
            {multi}
            {rpush list_key c}
            {rpush list_key d}
            {pexpireat list_key *}
            {exec}
        }
        close_replication_stream $repl
    }

    test {Set expire propagation} {
        r del set_key
        
        set repl [attach_to_replication_stream]

        assert_equal 1 [r saddex set_key a EX 1]
        # 对集合中存在的元素的sadd操作 不会被传播
        assert_equal 2 [r saddex set_key a b c EX 1]
        assert_equal 0 [r saddex set_key a EX 1]

        assert_replication_stream $repl {
            {select *}
            {multi}
            {sadd set_key a}
            {pexpireat set_key *}
            {exec}
            {multi}
            {sadd set_key b}
            {sadd set_key c}
            {pexpireat set_key *}
            {exec}
            {multi}
            {pexpireat set_key *}
            {exec}
        }
        close_replication_stream $repl
    }

}
