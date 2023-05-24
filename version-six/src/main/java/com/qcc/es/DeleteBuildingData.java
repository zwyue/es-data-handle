package com.qcc.es;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class DeleteBuildingData {

    public static void main(String[] args) {
        RestHighLevelClient restClient =
            ClientFactory.restClient(EsServer.BUILDING);
        createScroll(restClient);
        System.exit(0);
    }

    record RegisterInfo(String esId, String route, Long time) {
    }

    private static void createScroll(RestHighLevelClient restClient) {
        try {

            SearchRequest searchRequest = new SearchRequest("in_type_building_write_new");
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1));
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            TermQueryBuilder
                termQuery = QueryBuilders.termQuery("relationship", "register_v2");
            searchSourceBuilder.query(termQuery);
            searchSourceBuilder.fetchSource(new String[] {"register_v2.perid", "essynctime"}, null);
            searchSourceBuilder.size(10000);

            searchRequest.source(searchSourceBuilder);
            SearchResponse response = restClient.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = response.getScrollId();
            dataProcess(scrollId, restClient, response);

        } catch (IOException var12) {
            var12.printStackTrace();
        }
    }

    private static Set<String> findPerIds(SearchHits searchHits,
                                          Map<String, List<RegisterInfo>> map) {
        SearchHit[] hits = searchHits.getHits();

        Set<String> perIds = new HashSet<>();

        for (SearchHit hit : hits) {
            String id = hit.getId();
            Map<String, DocumentField> field = hit.getFields();
            DocumentField routingDoc = field.get("_routing");
            String rounting = (String) routingDoc.getValues().get(0);
            Map<String, Object> sourceMap = hit.getSourceAsMap();
            String perId = (String) sourceMap.get("register_v2.perid");
            perIds.add(perId);
            Long essynctime = Long.valueOf(String.valueOf(sourceMap.get("essynctime")));

            List<RegisterInfo> list = map.get(rounting);

            if (list == null) {
                list = new ArrayList<>();
            }
            RegisterInfo registerInfo = new RegisterInfo(id, rounting, essynctime);
            list.add(registerInfo);
            map.put(perId, list);
        }

        return perIds;
    }

    private static List<RegisterInfo> findInfoInMysql(Set<String> perIds,
                                                Map<String, List<RegisterInfo>> map) {
        StringBuilder perIdStr = new StringBuilder();

        perIds.forEach(id -> perIdStr.append("'").append(id).append("',"));

        String whereSql = " id in ( " + perIdStr.substring(0, perIdStr.lastIndexOf(",")) + " )";
        Object obj = ConnectUtil.execute(MysqlServer.BUILDING, whereSql, null, "8");

        JSONArray jsonArray = JSON.parseArray(JSON.toJSONString(obj));

        List<String> ids = new ArrayList<>();
        List<RegisterInfo> lostIds = new ArrayList<>();

        jsonArray.forEach(o -> {
            JSONObject jsonObject = (JSONObject) o;
            String id = jsonObject.getString("id");

            if (jsonArray.size() != perIds.size()) {
                ids.add(id);
            }

            String compKeywords = jsonObject.getString("comp_keywords");
            List<RegisterInfo> list = map.get(id);
            list.forEach(rt -> {
                if (!compKeywords.contains(rt.route)) {
                    lostIds.add(rt);
                }
            });
        });

        if (jsonArray.size() != perIds.size()) {
            perIds.forEach(perId -> {
                if (!ids.contains(perId)) {
                    List<RegisterInfo> list = map.get(perId);
                    lostIds.addAll(list);
                }
            });
        }

        return lostIds;
    }

    private static void doDel(RestHighLevelClient restClient, List<RegisterInfo> ids) throws IOException {

        BulkRequest bulkRequest = new BulkRequest();

        for (RegisterInfo id : ids) {
            DeleteRequest deleteRequest =
                new DeleteRequest("in_type_building_write_new", "doc", id.esId);
            deleteRequest.version(id.time) ;
            deleteRequest.versionType(VersionType.EXTERNAL_GTE) ;
            deleteRequest.routing(id.route) ;
            bulkRequest.add(deleteRequest);
        }
        BulkResponse response = restClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    private static void queryByScrollId(String scrollId, RestHighLevelClient restClient) {
        try {
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll(TimeValue.timeValueMinutes(1)) ;
            SearchResponse response =
                restClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
            dataProcess(scrollId, restClient, response);

        } catch (IOException var13) {
            var13.printStackTrace();
        }
    }

    private static void dataProcess(String scrollId, RestHighLevelClient restClient,
                                    SearchResponse response) {

        try {
            SearchHits searchHits = response.getHits();
            Map<String, List<RegisterInfo>> map = new HashMap<>();
            Set<String> perIds = findPerIds(searchHits, map);
            List<RegisterInfo> ids = findInfoInMysql(perIds, map);


            if(!ids.isEmpty()) {
                doDel(restClient, ids);
            }

            while (!perIds.isEmpty()) {
                queryByScrollId(scrollId, restClient);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
