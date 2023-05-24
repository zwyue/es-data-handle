package com.qcc.es;

import lombok.Getter;

@Getter
public enum MysqlServer {

    BUILDING("qcc-search-sync-read-b4b6a9f7.rwlb.rds.aliyuncs.com:3306","search_sync_industry",
        "ind_sync_read",""),

    DATA_CENTER( "qcc-search-sync-all-ba9f76b4.rwlb.rds.aliyuncs.com:3306","es_data_center",
        "es_data_user",""),
    ;

    private final String url;

    private final String database ;

    private final String username ;

    private final String password ;


    MysqlServer(String url,String database, String username,  String password) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
    }
}
