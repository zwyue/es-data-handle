package com.qcc.es;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class CompanyHonorContrast implements StartMiddleWare{

    public void start() {
        fromMysql2ES(null);
//        String ids = fromEs2Mysql();
//        fromMysql2ES(" id in (" + ids+")");

        System.exit(0);
    }

    public static void fromMysql2ES(String whereSql) {
        whereSql = whereSql == null ?
            " data_status = 1 and title_area = 'SX' " : whereSql;
        Object obj = ConnectUtil.execute(MysqlServer.COMPANY, whereSql, null, "10");

        JSONArray jsonArray = JSON.parseArray(JSON.toJSONString(obj));

        List<String> ids = new ArrayList<>();
        for (Object o : jsonArray) {
            JSONObject jo = (JSONObject) o ;

//            if(jo.getString("data_status").equals("1")) {
//
//            }else {
//                System.out.println(jo);
//            }
            ids.add(((JSONObject) o).getString("id"));
        }
        RestHighLevelClient restClient = ClientFactory.restClient(EsServer.BUILDING);
        ContrastData.contrast("company_honor_query",ids, restClient);
    }

    public static String fromEs2Mysql() {
        String keyNo = "90f1eaa45324e7bc7b4782d6c816daef";
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("compkeywords", keyNo);
        TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery("datastatus", 1);

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter().add(termQueryBuilder);
        boolQueryBuilder.filter().add(termQueryBuilder1);


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(boolQueryBuilder);

        SearchRequest searchRequest = new SearchRequest("in_building_rating_query");
        searchSourceBuilder.fetchSource(false);
        searchSourceBuilder.size(1000);
        searchRequest.source(searchSourceBuilder);


        String whereSql = "";
        try {
            RestHighLevelClient restClient = ClientFactory.restClient(EsServer.BUILDING);
            SearchResponse response = restClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits searchHits = response.getHits();
            SearchHit[] hits = searchHits.getHits();
            for (int i = 0; i < hits.length; i++) {
                String id = hits[i].getId() ;
                whereSql = whereSql.concat("'").concat(id).concat("'").concat(",");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return whereSql.substring(0, whereSql.lastIndexOf(","));
    }
}
