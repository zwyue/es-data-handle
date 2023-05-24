package com.qcc.es;


public enum EsServer {

    NONE("",0,"","",0),
    INFO_COMPANY(
        "so.greatld.com",9620,"companynew_writer","",7
    ),
    CRM_ANALYSIS(
        "so.greatld.com",9420,"search_write","",7
    ),
    BUILDING("es-cn-7pp2au9bt0020a4ho.elasticsearch.aliyuncs.com",9200,"elastic", "",6),
    ;

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getVersion() {
        return version;
    }

    private final String host;

    private final Integer port ;

    private final String username ;

    private final String password ;

    private final int version ;

    EsServer(String host, Integer port, String username, String password, int version) {
        this.host = host ;
        this.port = port ;
        this.username = username ;
        this.password = password ;
        this.version = version ;
    }

    public static EsServer getByName(String serverName) {
        for (EsServer server : EsServer.values()) {
            if(serverName.equalsIgnoreCase(server.name())) {
                return server ;
            }
        }
        return NONE ;
    }
}
