package com.midsizemango.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/company")
@Slf4j
public class CompanyController {
    @Autowired
    private RestHighLevelClient client;

    private static final String INDEX_COMPANY = "company";

    @GetMapping("/test")
    public ResponseEntity<String> testCall() {
        return new ResponseEntity<>("Successfully Called Endpoint", HttpStatus.OK);
    }

    @PutMapping("/index")
    public void index() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(INDEX_COMPANY);
        boolean indexExists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if(!indexExists) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_COMPANY);
            Map<String, Object> textType = new HashMap<>() {{
                put("type", "text");
            }};

            Map<String, Object> intType = new HashMap<>() {{
                put("type", "integer");
            }};

            Map<String, Object> longType = new HashMap<>() {{
                put("type", "long");
            }};

            Map<String, Object> properties = new HashMap<>() {{
                put("name", textType);
                put("age", intType);
                put("designation", textType);
                put("experience", intType);
                put("salary", longType);
            }};

            Map<String, Object> mapping = new HashMap<>() {{
                put("properties", properties);
            }};

            createIndexRequest.mapping(mapping);
            CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("Response: {}", createIndexResponse.index());
        }
    }

    @PostMapping("/save")
    public ResponseEntity<String> save(@RequestBody Company company) throws IOException {
        List<Company> companies = new ArrayList<>();
        Company company1 = new Company();
        company1.setName("Name1");
        company1.setAge(24);
        company1.setDesignation("Designation1");
        company1.setExperience(2);
        company1.setSalary(120000);

        Company company2 = new Company();
        company2.setName("Name2");
        company2.setAge(27);
        company2.setDesignation("Designation2");
        company2.setExperience(7);
        company2.setSalary(180000);

        companies.add(company1);
        companies.add(company2);

        BulkRequest bulkRequest = new BulkRequest();
        companies.forEach(com -> {
            IndexRequest indexRequest = new IndexRequest(INDEX_COMPANY);
            try {
                indexRequest.source(new ObjectMapper().writeValueAsString(company), XContentType.JSON);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            bulkRequest.add(indexRequest);
        });

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        /*IndexRequest indexRequest = new IndexRequest(INDEX_COMPANY);
        indexRequest.source(new ObjectMapper().writeValueAsString(company), XContentType.JSON);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);*/
        //System.out.println("Response: "+indexResponse.getResult().name());

        for(BulkItemResponse response: bulkResponse.getItems()) {
            log.info("Bulk Response ID: {}", response.getId());
        }
        return new ResponseEntity<>( "Success", HttpStatus.OK);
    }

    @PostMapping("/update/{id}")
    public ResponseEntity<String> update(@RequestBody Company company, @PathVariable final String id) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(INDEX_COMPANY, id);
        updateRequest.doc(new ObjectMapper().writeValueAsString(company), XContentType.JSON);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        log.info("Response: {}", updateResponse.getResult().name());
        return new ResponseEntity<>( updateResponse.getId(), HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<String> read(@PathVariable final String id) throws IOException {
        GetRequest getRequest = new GetRequest(INDEX_COMPANY, id);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        log.info("Response: {}", getResponse.getId());
        return new ResponseEntity<>(getResponse.getSourceAsString(), HttpStatus.OK);
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<String> delete(@PathVariable final String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX_COMPANY, id);
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        log.info("Response: {}", deleteResponse.toString());
        return new ResponseEntity<>(deleteResponse.getResult().name(), HttpStatus.OK);
    }

    @GetMapping("/searchAll")
    public ResponseEntity<List<Company>> getAllRecords() throws IOException {
        List<Company> companies = new ArrayList<>();

        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.timeout(new TimeValue(30, TimeUnit.SECONDS));

        SearchRequest searchRequest = new SearchRequest(INDEX_COMPANY);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        for(SearchHit searchHit: searchResponse.getHits().getHits()) {
            Company company = new ObjectMapper().readValue(searchHit.getSourceAsString(), Company.class);
            companies.add(company);
        }
        return new ResponseEntity<>(companies, HttpStatus.OK);
    }

    @GetMapping("/search/{from}/{to}")
    public ResponseEntity<List<Company>> search(@PathVariable final int from, @PathVariable final int to) throws IOException {
        List<Company> companies = new ArrayList<>();

        /*QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", searchName)
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(2)
                .maxExpansions(10);*/
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age").from(from).to(to);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        searchSourceBuilder.timeout(new TimeValue(30, TimeUnit.SECONDS));

        SearchRequest searchRequest = new SearchRequest(INDEX_COMPANY);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        for(SearchHit searchHit: searchResponse.getHits().getHits()) {
            Company company = new ObjectMapper().readValue(searchHit.getSourceAsString(), Company.class);
            companies.add(company);
        }
        return new ResponseEntity<>(companies, HttpStatus.OK);
    }

    @GetMapping("/filter")
    public ResponseEntity<List<Company>> filter(
            @PathVariable("age") int age, @PathVariable("designation") String designation) throws IOException {
        List<Company> companies = new ArrayList<>();

        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery())
                .filter(QueryBuilders.termQuery("age", age))
                .filter(QueryBuilders.termQuery("designation", designation));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        searchSourceBuilder.timeout(new TimeValue(30, TimeUnit.SECONDS));

        SearchRequest searchRequest = new SearchRequest(INDEX_COMPANY);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        for(SearchHit searchHit: searchResponse.getHits().getHits()) {
            Company company = new ObjectMapper().readValue(searchHit.getSourceAsString(), Company.class);
            companies.add(company);
        }
        return new ResponseEntity<>(companies, HttpStatus.OK);
    }
}
