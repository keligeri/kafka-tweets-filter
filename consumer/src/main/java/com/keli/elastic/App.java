package com.keli.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class App {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        IndexRequest request = new IndexRequest("hello", "doc", "1");
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "keli");
        jsonMap.put("age", "25");
        jsonMap.put("date", new Date().toString());
        request.source(jsonMap);

        client.index(request, RequestOptions.DEFAULT);
        client.close();
    }
}
