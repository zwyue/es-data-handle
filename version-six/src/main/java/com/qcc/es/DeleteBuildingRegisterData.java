package com.qcc.es;

import com.qcc.es.records.EsKeyInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class DeleteBuildingRegisterData {

    public static void main(String[] args) {
        RestHighLevelClient restClient =
            ClientFactory.restClient(EsServer.BUILDING);
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

    private static void findIdAndRouting(SearchHits searchHits,
                                         List<EsKeyInfo> list) {
        SearchHit[] hits = searchHits.getHits();

        for (SearchHit hit : hits) {
            String id = hit.getId();
            Map<String, DocumentField> field = hit.getFields();
            DocumentField routingDoc = field.get("_routing");
            String rounting = (String) routingDoc.getValues().get(0);
            Map<String, Object> sourceMap = hit.getSourceAsMap();
            Long essynctime = Long.valueOf(String.valueOf(sourceMap.get("essynctime")));
            EsKeyInfo registerInfo = new EsKeyInfo(id, rounting, essynctime);
            list.add(registerInfo);
        }
    }

    private static void queryByScrollId(String scrollId, RestHighLevelClient restClient) {
        try {
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll(TimeValue.timeValueMinutes(1));
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
            List<EsKeyInfo> list = new ArrayList<>();
            findIdAndRouting(searchHits, list);

            while (!list.isEmpty()) {
                ClientFactory.doDel(restClient, list);
                queryByScrollId(scrollId, restClient);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
