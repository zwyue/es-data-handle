package com.qcc.es;

import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class ContrastData {

    public static void contrast(List<String> ids, RestHighLevelClient restClient) {

        List<String> lostIds = new ArrayList<>();

        ids.forEach(id -> {
            try {
                TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("id", id);

                TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery("datastatus", "1");

                BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
                boolQueryBuilder.filter().add(termQueryBuilder);
                boolQueryBuilder.filter().add(termQueryBuilder1);


                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(boolQueryBuilder);

                CountRequest searchRequest = new CountRequest("in_building_register_query");
                searchRequest.source(searchSourceBuilder);

                CountResponse response = restClient.count(searchRequest, RequestOptions.DEFAULT);

                if (response.getCount() == 0) {
                    System.out.println("...... this id: {" + id + "} does not exit .......");
                    System.out.println(response);
                    lostIds.add(id);
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        System.out.println(lostIds);
    }
}
