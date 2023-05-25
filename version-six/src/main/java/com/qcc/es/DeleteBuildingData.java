package com.qcc.es;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.qcc.es.records.EsDataInfo;
import com.qcc.es.records.EsKeyInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.log4j.Log4j2;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

@Log4j2
public class DeleteBuildingData implements StartMiddleWare{

    private static int forCycle = 0 ;

    public void start() {
        RestHighLevelClient restClient = ClientFactory.restClient(EsServer.BUILDING);
        createScroll(restClient);
        forCycle = 0;
        System.exit(0);
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
            String[] includes =
                new String[] {"register_v2.perid", "essynctime", "p1time", "p2time"};
            searchSourceBuilder.fetchSource(includes, null);
            searchSourceBuilder.size(5000);

            searchRequest.source(searchSourceBuilder);
            SearchResponse response = restClient.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = response.getScrollId();
            dataProcess(scrollId, restClient, response.getHits().getHits());

        } catch (IOException var12) {
            var12.printStackTrace();
        }
    }

    private static Set<String> findPerIds(SearchHit[] hits, Map<String, List<EsKeyInfo>> map) {
        Set<String> perIds = new HashSet<>();

        for (SearchHit hit : hits) {
            EsDataInfo esDataInfo = ClientFactory.getEsDataInfo(hit);
            Map<String, Object> sourceMap = esDataInfo.sourceMap();
            String perId = (String) sourceMap.get("register_v2.perid");
            perIds.add(perId);
            Long essynctime = Long.valueOf(String.valueOf(sourceMap.get("essynctime")));

            List<EsKeyInfo> list = map.get(esDataInfo.route());

            if (list == null) {
                list = new ArrayList<>();
            }
            EsKeyInfo registerInfo =
                new EsKeyInfo(esDataInfo.esId(), esDataInfo.route(), essynctime);
            list.add(registerInfo);
            map.put(perId, list);
        }

        return perIds;
    }

    private static List<EsKeyInfo> findInfoInMysql(Set<String> perIds,
                                                   Map<String, List<EsKeyInfo>> map) {
        StringBuilder perIdStr = new StringBuilder();

        perIds.forEach(id -> perIdStr.append("'").append(id).append("',"));

        String whereSql = " id in ( " + perIdStr.substring(0, perIdStr.lastIndexOf(",")) + " )";
        Object obj = ConnectUtil.execute(MysqlServer.BUILDING, whereSql, null, "8");

        JSONArray jsonArray = JSON.parseArray(JSON.toJSONString(obj));

        List<String> ids = new ArrayList<>();
        List<EsKeyInfo> lostIds = new ArrayList<>();

        jsonArray.forEach(o -> {
            JSONObject jsonObject = (JSONObject) o;
            String id = jsonObject.getString("id");

            if (jsonArray.size() != perIds.size()) {
                ids.add(id);
            }

            String compKeywords = jsonObject.getString("comp_keywords");
            List<EsKeyInfo> list = map.get(id);
            list.forEach(rt -> {
                if (!compKeywords.contains(rt.route())) {
                    lostIds.add(rt);
                }
            });
        });

        if (jsonArray.size() != perIds.size()) {
            perIds.forEach(perId -> {
                if (!ids.contains(perId)) {
                    List<EsKeyInfo> list = map.get(perId);
                    lostIds.addAll(list);
                }
            });
        }

        return lostIds;
    }

    private static void dataProcess(String scrollId, RestHighLevelClient restClient,
                                    SearchHit[] hits) {
        try {
            if (hits.length == 0) {
                return;
            }
            log.info("...... for cycle : {}",forCycle);
            forCycle ++ ;
            Map<String, List<EsKeyInfo>> map = new HashMap<>();
            Set<String> perIds = findPerIds(hits, map);

            List<EsKeyInfo> ids = findInfoInMysql(perIds, map);

            if (!ids.isEmpty()) {
                ClientFactory.doDel(restClient, ids, "in_type_building_write_new");
            }

            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll(TimeValue.timeValueMinutes(1));
            SearchResponse response =
                restClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                dataProcess(scrollId, restClient, response.getHits().getHits());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
