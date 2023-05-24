package com.qcc.es;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class ConnectRedis {

    private ConnectRedis() {
        throw new IllegalStateException("ConnectRedis class");
    }

    public static JedisPool returnJedisPool(int database) {
        JedisPoolConfig config = new JedisPoolConfig();
        RedisServer redisServer = RedisServer.REDIS_1 ;
        return new JedisPool(config, redisServer.getHost(), redisServer.getPort(), 2000,
            redisServer.getPassword(), database);
    }
}
