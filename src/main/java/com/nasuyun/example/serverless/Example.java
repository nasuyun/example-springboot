package com.nasuyun.example.serverless;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Component
public class Example {

    @Value("${elasticsearch.serverless.endpoint}")
    String endpoint;
    @Value("${elasticsearch.serverless.username}")
    String username;
    @Value("${elasticsearch.serverless.password}")
    String password;

    final String index = "myindex";

    @PostConstruct
    public void onCreated() {
        log.info("connection info : {}  username[{}] password[{}]", endpoint, username, password);
        createIndex();
        bulk();
        search();
    }

    // 构建ES客户端
    private RestHighLevelClient client() {
        Header[] headers = new Header[]{new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")};
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder builder = RestClient.builder(HttpHost.create(endpoint))
                .setDefaultHeaders(headers)
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(builder);
    }

    /**
     * 创建索引
     */
    public void createIndex() {
        try (RestHighLevelClient client = client()) {
            boolean exists = client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
            if (exists == false) {
                CreateIndexRequest request = new CreateIndexRequest(index);
                request.settings(Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                );
                Map<String, Object> message = new HashMap<>();
                message.put("type", "text");
                Map<String, Object> properties = new HashMap<>();
                properties.put("message", message);
                Map<String, Object> mapping = new HashMap<>();
                mapping.put("properties", properties);
                request.mapping(mapping);
                CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
                log.info("create index[{}], result[{}]", createIndexResponse.index(), createIndexResponse.isAcknowledged());
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    /**
     * 删除索引
     */
    public void deleteIndex() {
        try (RestHighLevelClient client = client()) {
            boolean exists = client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
            if (exists) {
                DeleteIndexRequest request = new DeleteIndexRequest(index);
                AcknowledgedResponse Response = client.indices().delete(request, RequestOptions.DEFAULT);
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    /**
     * 写入索引
     */
    public void bulk() {
        try (RestHighLevelClient client = client()) {
            BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < 10; i++) {
                bulkRequest.add(new IndexRequest(index).type("_doc").source(XContentType.JSON, "message", UUID.randomUUID().toString()));
            }
            bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            log.info("bulk took {}", bulkResponse.getTook());
        } catch (Exception e) {
            log.error("", e);
        }
    }

    /**
     * 查询索引
     */
    public void search() {
        try (RestHighLevelClient client = client()) {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(10);
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.source(searchSourceBuilder);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            log.info("{} {} {}", response.getTook(), response.getHits().totalHits, response.getHits().getHits());
        } catch (Exception e) {
            log.error("", e);
        }
    }

}
