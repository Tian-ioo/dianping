package com.hmdp.utils;


import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;


/**
 * @author tian
 * @description
 */
@Slf4j
@Component
public class CacheClient {

    private final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    private  StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    // 将任意对象序列化为json并存储在string类型的key中,并且可以设置TTL过期时间
    public void set(String key,Object value,Long time,TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    // 将任意对象序列化为json并存储在string类型的key中,并且可以设置逻辑过期时间
    public void setWithLogicExpire(String key, Object value, Long time,TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    // 根据指定的key查询缓存,并反序列化为指定类型,利用缓存空值的方法解决缓存穿透问题
    public  <R,ID> R queryWithPassThrough(String keyPrefix
            ,ID id, Class<R> type,Function<ID,R> dbFallback
            ,Long time,TimeUnit unit) {
        String key = keyPrefix+ id;
        // 从redis中获取数据
        String json = stringRedisTemplate.opsForValue().get(key);
        // 数据存在直接返回
        if(StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        // 判断数据是否是空值
        if(json != null){
            return null;
        }

        // 数据不存在,查询数据库
        R r = dbFallback.apply(id);
        // 数据库中数据不存在,直接返回
        if(r == null){
            // 写入空值,避免缓存穿透
            stringRedisTemplate.opsForValue()
                    .set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 数据库中数据存在,返回给redis和客户端
        this.set(key,r,time,unit);
        return r;
    }

    // 根据指定的key查询缓存,并反序列化为指定类型,利用逻辑过期的方法解决缓存穿透问题
    public  <R,ID> R queryWithLogicExpire(String keyPrefix
            ,ID id,Class<R> type,Function<ID,R> dbFallback
            ,Long time,TimeUnit unit) {
        String key = keyPrefix+ id;
        // 从redis中获取数据
        String json = stringRedisTemplate.opsForValue().get(key);
        // 数据存在直接返回
        if(StrUtil.isBlank(json)) {
            return null;
        }

        // 命中,判断过期时间
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject jsonObject = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(jsonObject, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 未过期,直接返回数据
        if(expireTime.isAfter(LocalDateTime.now())){
            return r;
        }
        // 过期,缓存重建,判断是否获取到锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 未获取到,直接返回数据
        // 获取到锁,重新启一个线程去缓存重建,返回数据
        // 这里获没获取锁都要返回shop对象
        if(isLock){
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    // 查询数据库
                    R r1 = dbFallback.apply(id);
                    // 写入redis
                    this.setWithLogicExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        // 数据不存在,查询数据库
        return r;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(
                key, "1", 10, TimeUnit.SECONDS);

        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
}
