package com.qcc.es;

import com.qcc.es.records.EsDataInfo;
import com.qcc.es.records.EsKeyInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class DeleteBuildingRegisterData implements StartMiddleWare {

    public void start() {
        RestHighLevelClient restClient = ClientFactory.restClient(EsServer.BUILDING);
        createScroll(restClient);
        System.exit(0);
    }

    private static void createScroll(RestHighLevelClient restClient) {
        try {

            SearchRequest searchRequest = new SearchRequest("in_type_building_write_new");
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1));
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            TermQueryBuilder
                termQuery = QueryBuilders.termQuery("relationship", "register");
            searchSourceBuilder.query(termQuery);
            searchSourceBuilder.fetchSource("essynctime", null);
            searchSourceBuilder.size(10000);

            searchRequest.source(searchSourceBuilder);
            SearchResponse response = restClient.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = response.getScrollId();
            dataProcess(scrollId, restClient, response);

        } catch (IOException var12) {
            var12.printStackTrace();
        }
    }

    private static void findIdAndRouting(SearchHits searchHits, List<EsKeyInfo> list) {
        SearchHit[] hits = searchHits.getHits();

        for (SearchHit hit : hits) {
            EsDataInfo esDataInfo = ClientFactory.getEsDataInfo(hit);
            Long essynctime =
                Long.valueOf(String.valueOf(esDataInfo.sourceMap().get("essynctime")));
            EsKeyInfo registerInfo =
                new EsKeyInfo(esDataInfo.esId(), esDataInfo.route(), essynctime);
            list.add(registerInfo);
        }
    }

    private static void dataProcess(String scrollId, RestHighLevelClient restClient,
                                    SearchResponse response) {

        try {
            SearchHits searchHits = response.getHits();
            List<EsKeyInfo> list = new ArrayList<>();
            findIdAndRouting(searchHits, list);

            while (!list.isEmpty()) {
                ClientFactory.doDel(restClient, list, "in_type_building_write_new");
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scroll(TimeValue.timeValueMinutes(1));
                response = restClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                dataProcess(scrollId, restClient, response);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
