package com.qcc.es;

import java.util.List;
import java.util.Map;

public record ExecuteParams(MysqlServer mysqlServer,
                            String table,
                            String whereSql,
                            List<Map<String,Object>> list,
                            List<String> list1,
                            Integer from,
                            Integer size,
                            String method) {

}
