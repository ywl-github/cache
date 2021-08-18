package com.example.cache.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.sun.istack.internal.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.support.AbstractValueAdaptingCache;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.StringUtils;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 集成 AbstractValueAdaptingCache 实现 Cache 对缓存的曾删改查   第二步
 */
public class RedisCaffeineCache extends AbstractValueAdaptingCache {
    private final Logger logger = LoggerFactory.getLogger(RedisCaffeineCache.class);
    private String name;
    private RedisTemplate<String, Object> redisTemplate;
    private Cache<Object, Object> caffeineCache;
    private String cachePrefix;
    private long defaultExpiration = 0;
    private Map<String, Long> expires;
    private String topic = "cache:redis:caffeine:topic";
    protected RedisCaffeineCache(boolean allowNullValues) {
        super(allowNullValues);
    }

    public RedisCaffeineCache(String name, RedisTemplate<String, Object> redisTemplate, Cache<Object, Object> caffeineCache, CacheRedisCaffeineProperties cacheRedisCaffeineProperties) {
        super(cacheRedisCaffeineProperties.isCacheNullValues());
        this.name = name;
        this.redisTemplate = redisTemplate;
        this.caffeineCache = caffeineCache;
        this.cachePrefix = cacheRedisCaffeineProperties.getCachePrefix();
        this.defaultExpiration = cacheRedisCaffeineProperties.getRedis().getDefaultExpiration();
        this.expires = cacheRedisCaffeineProperties.getRedis().getExpires();
        this.topic = cacheRedisCaffeineProperties.getRedis().getTopic();
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Object getNativeCache() {
        return this;
    }

    // 通过key获取缓存值，可以使用valueLoader.call()来调使用@Cacheable注解的方法。当@Cacheable注解的sync属性配置为true时使用此方法。因此方法内需要保证回源到数据库的同步性。避免在缓存失效时大量请求回源到数据库
    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        Object value = lookup(key);
        if(value != null) {
            return (T) value;
        }

        ReentrantLock lock = new ReentrantLock();
        try {
            lock.lock();
            value = lookup(key);
            if(value != null) {
                return (T) value;
            }
            value = valueLoader.call();
            Object storeValue = toStoreValue(valueLoader.call());
            put(key, storeValue);
            return (T) value;
        } catch (Exception e) {
            try {
                Class<?> c = Class.forName("org.springframework.cache.Cache$ValueRetrievalException");
                Constructor<?> constructor = c.getConstructor(Object.class, Callable.class, Throwable.class);
                RuntimeException exception = (RuntimeException) constructor.newInstance(key, valueLoader, e.getCause());
                throw exception;
            } catch (Exception e1) {
                throw new IllegalStateException(e1);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(Object key, Object value) {
        if (!super.isAllowNullValues() && value == null) {
            this.evict(key);
            return;
        }
        long expire = getExpire();
        if(expire > 0) {
            redisTemplate.opsForValue().set(getKey(key), toStoreValue(value), expire, TimeUnit.MILLISECONDS);
        } else {
            redisTemplate.opsForValue().set(getKey(key), toStoreValue(value));
        }

        push(new CacheMessage(this.name, key));

        caffeineCache.put(key, value);
    }
    // 当缓存中不存在key时才放入缓存。返回值是当key存在时原有的数据
    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        Object cacheKey = getKey(key);
        Object prevValue = null;
        // 考虑使用分布式锁，或者将redis的setIfAbsent改为原子性操作
        synchronized (key) {
            prevValue = redisTemplate.opsForValue().get(cacheKey);
            if(prevValue == null) {
                long expire = getExpire();
                if(expire > 0) {
                    redisTemplate.opsForValue().set(getKey(key), toStoreValue(value), expire, TimeUnit.MILLISECONDS);
                } else {
                    redisTemplate.opsForValue().set(getKey(key), toStoreValue(value));
                }

                push(new CacheMessage(this.name, key));

                caffeineCache.put(key, toStoreValue(value));
            }
        }
        return toValueWrapper(prevValue);
    }

    @Override
    public void evict(Object key) {
        // 先清除redis中缓存数据，然后清除caffeine中的缓存，避免短时间内如果先清除caffeine缓存后其他请求会再从redis里加载到caffeine中
        redisTemplate.delete(getKey(key));

        push(new CacheMessage(this.name, key));

        caffeineCache.invalidate(key);
    }

    @Override
    public void clear() {
        // 先清除redis中缓存数据，然后清除caffeine中的缓存，避免短时间内如果先清除caffeine缓存后其他请求会再从redis里加载到caffeine中
        //Set<String> keys = redisTemplate.keys(this.name.concat(":*"));
        /*for(String key : keys) {
            redisTemplate.delete(key);
        }*/
        Set<String> keys = scanAll(this.name.concat(":*"));
        redisTemplate.delete(keys);

        push(new CacheMessage(this.name, null));

        caffeineCache.invalidateAll();
    }

    @Override
    protected Object lookup(Object key) {
        Object cacheKey = getKey(key);
        Object value = caffeineCache.getIfPresent(key);
        if(value != null) {
            logger.info("get cache from caffeine, the key is : {}", cacheKey);
            return value;
        }

        value = redisTemplate.opsForValue().get(cacheKey);

        if(value != null) {
            logger.info("get cache from redis and put in caffeine, the key is : {}", cacheKey);
            caffeineCache.put(key, value);
        }
        return value;
    }

    private String getKey(Object key) {
        return this.name.concat(":").concat(StringUtils.isEmpty(cachePrefix) ? key.toString() : cachePrefix.concat(":").concat(key.toString()));
    }

    private long getExpire() {
        long expire = defaultExpiration;
        Long cacheNameExpire = expires.get(this.name);
        return cacheNameExpire == null ? expire : cacheNameExpire.longValue();
    }

    /**
     * @description 缓存变更时通知其他节点清理本地缓存
     * @param message
     */
    private void push(CacheMessage message) {
        redisTemplate.convertAndSend(topic, message);
    }

    /**
     * @description 清理本地缓存
     * @param key
     */
    public void clearLocal(Object key) {
        logger.info("clear local cache, the key is : {}", key);
        if(key == null) {
            caffeineCache.invalidateAll();
        } else {
            caffeineCache.invalidate(key);
        }
    }

    @SuppressWarnings("rawtypes")
    public Set<String> scanAll(@NotNull String pattern) {
        List execList;
        Set<String> keys = new HashSet();
        // 每次遍历3000个key
        String count = "3000";
        String cursor = "0";
        // 构建RedisScript对象时省略了‘eval’
        // 根据命令执行后的返回值确定resultType，有默认值，不指定可能出现返回值无法正常解析的情况
        // @see org.springframework.data.redis.connection.ReturnType
        RedisScript<List> redisScript = RedisScript.of(
                "return redis.call('scan',KEYS[1],'MATCH',ARGV[1],'count',ARGV[2])", List.class);

        // 默认使用redisTemplate针对key和value的序列化方式，这种方式序列化后的args参数（pattern、count）会导致脚本无效（命令解析不正确）
        // 可以尝试修改为其他序列化方式并观察效果
        RedisSerializer serializer = redisTemplate.getStringSerializer();
        do {
            execList = redisTemplate.execute(redisScript, serializer,
                    serializer, Collections.singletonList(cursor), pattern, count);
            assert execList != null;
            // 返回值参考执行的redis命令，也可以通过调试来确定
            // 如count命令：返回值的1）表示下次要开始的游标位置，返回值2）表示满足正则表达式的key值集合
            cursor = String.valueOf(execList.get(0));
            keys.addAll((List<String>)execList.get(1));
            // 游标值返回0，表示 整个数据集（collection）已经被完整遍历过了，称这个过程为一次完整遍历（full iteration）
        } while (!"0".equals(cursor));
        return keys;
    }

    @Autowired(required = false)
    public void setRedisTemplate(RedisTemplate redisTemplate) {
        RedisSerializer stringSerializer = new StringRedisSerializer();
        redisTemplate.setKeySerializer(stringSerializer);
        redisTemplate.setValueSerializer(stringSerializer);
        redisTemplate.setHashKeySerializer(stringSerializer);
        redisTemplate.setHashValueSerializer(stringSerializer);
        this.redisTemplate = redisTemplate;
    }
}