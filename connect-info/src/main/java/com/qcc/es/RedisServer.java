package com.qcc.es;

import lombok.Getter;

@Getter
public enum RedisServer {

    REDIS_1("r-bp1ae4e98928e5d4.redis.rds.aliyuncs.com",6379,"");


    private final String host;

    private final int port ;

    private final String password ;

    RedisServer(String host,int port,String password) {
        this.host = host ;
        this.password = password ;
        this.port = port ;
    }
}
