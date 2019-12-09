package com.mikasaco.syncdata.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.mikasaco.syncdata.model.Page;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * elasticSearh 操作类
 *
 * @author Stephen
 * @version 1.0
 * @date 2018/09/17 11:12:08
 */
@Component
@Slf4j
public class EsClient<T, E> {

    private static final String INDEX_KEY = "index";
    private static final String INDEX = "spider";
    private static final String TIMESTAMP = "timestamp";

    private static final Logger LOGGER = LoggerFactory.getLogger(EsClient.class);

    protected static RestHighLevelClient client;


    /**
     * 初始化配置
     */
    @PostConstruct
    public RestHighLevelClient restHighLevelClient() {
        client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("localhost", 9200, "http")));
        return client;
    }


    public boolean checkIndexExists(String indexName) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        return exists;
    }

    public void createIndex(String indexName) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }


    /**
     * 增加文档,传入id
     *
     * @param indexName
     * @param id
     * @param jsonString
     */
    public void addDocByJson(String indexName, String id, String jsonString) throws Exception {

        if (!checkIndexExists(indexName)) {
//            throw new Exception("索引不存在");
        }
        IndexRequest request = new IndexRequest(indexName).id(id).source(jsonString, XContentType.JSON);
        // request的opType默认是INDEX(传入相同id会覆盖原document，CREATE则会将旧的删除)
        // request.opType(DocWriteRequest.OpType.CREATE)
        IndexResponse response = null;
        try {
            response = client.index(request, RequestOptions.DEFAULT);
            String index = response.getIndex();
            String documentId = response.getId();
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                LOGGER.info("新增文档成功！ index: {}, id: {}", index, documentId);
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                LOGGER.info("修改文档成功！ index: {}, id: {}", index, documentId);
            }
            // 分片处理信息
            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                LOGGER.error("文档未写入全部分片副本！");
            }
            // 如果有分片副本失败，可以获得失败原因信息
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    LOGGER.error("副本失败原因：{}", reason);
                }
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                LOGGER.error("版本异常！");
            }
            LOGGER.error("文档新增失败！");
        }
    }

    /**
     * 根据id查找文档
     *
     * @param index
     * @param id
     * @return
     * @throws IOException
     */
    public Map<String, Object> getDocument(String index, String id) throws IOException {
        Map<String, Object> resultMap = new HashMap<>();
        GetRequest request = new GetRequest(index, id);
        // 实时(否)
        request.realtime(false);
        // 检索之前执行刷新(是)
        request.refresh(true);

        GetResponse response = null;
        try {
            response = client.get(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                LOGGER.error("文档未找到，请检查参数！");
            }
            if (e.status() == RestStatus.CONFLICT) {
                LOGGER.error("版本冲突！");
            }
            LOGGER.error("查找失败！");
        }

        if (Objects.nonNull(response)) {
            if (response.isExists()) { // 文档存在
                resultMap = response.getSourceAsMap();
            } else {
                // 处理未找到文档的方案。 请注意，虽然返回的响应具有404状态代码，但仍返回有效的GetResponse而不是抛出异常。
                // 此时此类响应不持有任何源文档，并且其isExists方法返回false。
                LOGGER.error("文档未找到，请检查参数！");
            }
        }
        return resultMap;
    }


    public Page<E> search(String indexName, T t, E e) throws IOException {
        Map<String, String> map = JSONObject.parseObject(JSONObject.toJSONString(t), new TypeReference<Map<String, String>>() {
        });
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (map.containsKey("offset")) {
            searchSourceBuilder.from(Integer.parseInt(map.get("offset")));
        }
        if (map.containsKey("pageSize")) {
            searchSourceBuilder.size(Integer.parseInt(map.get("pageSize")));
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        List<QueryBuilder> queryBuilders = boolQueryBuilder.must();
        for (String key : map.keySet()) {
            String value = map.get(key);
            if (map.containsKey("total") || map.containsKey("pageNo") || map.containsKey("pageSize") || map.containsKey("offset")) {
                continue;
            }
            MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(key, value);
            queryBuilders.add(matchQueryBuilder);
        }
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();

        ArrayList<E> list = new ArrayList<>();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            E item = (E) JSONObject.parseObject(sourceAsString, e.getClass());
            list.add(item);

        }
        Page<E> page = new Page<>();
        Long total = hits.getTotalHits().value;
        page.setTotal(total.intValue());
        page.setData(list);

        return page;

    }

    /**
     * 删除文档
     *
     * @param index
     * @param id
     * @throws IOException
     */
    public void deleteDocument(String index, String id) throws IOException {
        DeleteRequest request = new DeleteRequest(index, id);
        DeleteResponse response = null;
        try {
            response = client.delete(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                LOGGER.error("版本冲突！");
            }
            LOGGER.error("删除失败!");
        }
        if (Objects.nonNull(response)) {
            if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                LOGGER.error("不存在该文档，请检查参数！");
            }
            LOGGER.info("文档已删除！");
            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                LOGGER.error("部分分片副本未处理");
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    LOGGER.error("失败原因：{}", reason);
                }
            }
        }
    }


    /**
     * 通过一个JSON字符串更新文档(如果该文档不存在，则根据参数创建这个文档)
     *
     * @param index
     * @param id
     * @param jsonString
     * @throws IOException
     */
    public void updateDocByJson(String index, String id, String jsonString) throws IOException {

        if (!checkIndexExists(index)) {
            createIndex(index);
        }
        UpdateRequest request = new UpdateRequest(index, id);
        request.doc(jsonString, XContentType.JSON);
        // 如果要更新的文档不存在，则根据传入的参数新建一个文档
        request.docAsUpsert(true);
        try {
            UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
            String indexName = response.getIndex();
            String documentId = response.getId();
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                LOGGER.info("文档新增成功！index: {}, id: {}", indexName, documentId);
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                LOGGER.info("文档更新成功！");
            } else if (response.getResult() == DocWriteResponse.Result.DELETED) {
                LOGGER.error("\"index={},id={}\"的文档已被删除，无法更新！", indexName, documentId);
            } else if (response.getResult() == DocWriteResponse.Result.NOOP) {
                LOGGER.error("操作没有被执行！");
            }

            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                LOGGER.error("分片副本未全部处理");
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    LOGGER.error("未处理原因：{}", reason);
                }
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                LOGGER.error("不存在这个文档，请检查参数！");
            } else if (e.status() == RestStatus.CONFLICT) {
                LOGGER.error("版本冲突异常！");
            }
            LOGGER.error("更新失败！");
        }
    }

    /**
     * 批量增加文档
     *
     * @param params
     * @throws IOException
     */
    public void bulkAdd(List<Map<String, String>> params) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, String> dataMap : params) {
            String index = dataMap.getOrDefault(INDEX_KEY, INDEX);
            String id = dataMap.get("id");
            String jsonString = dataMap.get("json");
            if (StringUtils.isNotBlank(id)) {
                IndexRequest request = new IndexRequest(index, id).source(jsonString, XContentType.JSON);
                bulkRequest.add(request);
            }
        }
        // 超时时间(2分钟)
        bulkRequest.timeout(TimeValue.timeValueMinutes(2L));
        // 刷新策略
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

        if (bulkRequest.numberOfActions() == 0) {
            LOGGER.error("参数错误，批量增加操作失败！");
            return;
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        // 全部操作成功
        if (!bulkResponse.hasFailures()) {
            LOGGER.info("批量增加操作成功！");
        } else {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    LOGGER.error("\"index={}, type={}, id={}\"的文档增加失败！", failure.getIndex(), failure.getType(), failure.getId());
                    LOGGER.error("增加失败详情: {}", failure.getMessage());
                } else {
                    LOGGER.info("\"index={}, type={}, id={}\"的文档增加成功！", bulkItemResponse.getIndex(), bulkItemResponse.getType(), bulkItemResponse.getId());
                }
            }
        }
    }

    /**
     * 批量更新文档
     *
     * @param params
     * @throws IOException
     */
    public void bulkUpdate(List<Map<String, String>> params) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, String> dataMap : params) {
            String index = dataMap.getOrDefault(INDEX_KEY, INDEX);
            String id = dataMap.get("id");
            String jsonString = dataMap.get("json");
            if (StringUtils.isNotBlank(id)) {
                UpdateRequest request = new UpdateRequest(index, id).doc(jsonString, XContentType.JSON);
                request.docAsUpsert(true);
                bulkRequest.add(request);
            }
        }
        if (bulkRequest.numberOfActions() == 0) {
            LOGGER.error("参数错误，批量更新操作失败！");
            return;
        }
        bulkRequest.timeout(TimeValue.timeValueMinutes(2L));
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (!bulkResponse.hasFailures()) {
            LOGGER.info("批量更新操作成功！");
        } else {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    LOGGER.error("\"index={}, type={}, id={}\"的文档更新失败！", failure.getIndex(), failure.getType(), failure.getId());
                    LOGGER.error("更新失败详情: {}", failure.getMessage());
                } else {
                    LOGGER.info("\"index={}, type={}, id={}\"的文档更新成功！", bulkItemResponse.getIndex(), bulkItemResponse.getType(), bulkItemResponse.getId());
                }
            }
        }
    }

    /**
     * 批量删除文档
     *
     * @param params
     * @throws IOException
     */
    public void bulkDelete(List<Map<String, String>> params) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, String> dataMap : params) {
            String index = dataMap.getOrDefault(INDEX_KEY, INDEX);
            String id = dataMap.get("id");
            if (StringUtils.isNotBlank(id)) {
                DeleteRequest request = new DeleteRequest(index, id);
                bulkRequest.add(request);
            }
        }
        if (bulkRequest.numberOfActions() == 0) {
            LOGGER.error("操作失败，请检查参数！");
            return;
        }
        bulkRequest.timeout(TimeValue.timeValueMinutes(2L));
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (!bulkResponse.hasFailures()) {
            LOGGER.info("批量删除操作成功！");
        } else {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    LOGGER.error("\"index={}, type={}, id={}\"的文档删除失败！", failure.getIndex(), failure.getType(), failure.getId());
                    LOGGER.error("删除失败详情: {}", failure.getMessage());
                } else {
                    LOGGER.info("\"index={}, type={}, id={}\"的文档删除成功！", bulkItemResponse.getIndex(), bulkItemResponse.getType(), bulkItemResponse.getId());
                }
            }
        }
    }

    /**
     * 批量查找文档
     *
     * @param params
     * @return
     * @throws IOException
     */
    public List<Map<String, Object>> multiGet(List<Map<String, String>> params) throws IOException {
        List<Map<String, Object>> resultList = new ArrayList<>();

        MultiGetRequest request = new MultiGetRequest();
        for (Map<String, String> dataMap : params) {
            String index = dataMap.getOrDefault(INDEX_KEY, INDEX);
            String id = dataMap.get("id");
            if (StringUtils.isNotBlank(id)) {
                request.add(new MultiGetRequest.Item(index, id));
            }
        }
        request.realtime(false);
        request.refresh(true);
        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
        List<Map<String, Object>> list = parseMGetResponse(response);
        if (!list.isEmpty()) {
            resultList.addAll(list);
        }
        return resultList;
    }

    private List<Map<String, Object>> parseMGetResponse(MultiGetResponse response) {
        List<Map<String, Object>> list = new ArrayList<>();
        MultiGetItemResponse[] responses = response.getResponses();
        for (MultiGetItemResponse item : responses) {
            GetResponse getResponse = item.getResponse();
            if (Objects.nonNull(getResponse)) {
                if (!getResponse.isExists()) {
                    LOGGER.error("\"index={}, type={}, id={}\"的文档查找失败，请检查参数！", getResponse.getIndex(), getResponse.getType(), getResponse.getId());
                } else {
                    list.add(getResponse.getSourceAsMap());
                }
            } else {
                MultiGetResponse.Failure failure = item.getFailure();
                ElasticsearchException e = (ElasticsearchException) failure.getFailure();
                if (e.status() == RestStatus.NOT_FOUND) {
                    LOGGER.error("\"index={}, type={}, id={}\"的文档不存在！", failure.getIndex(), failure.getType(), failure.getId());
                } else if (e.status() == RestStatus.CONFLICT) {
                    LOGGER.error("\"index={}, type={}, id={}\"的文档版本冲突！", failure.getIndex(), failure.getType(), failure.getId());
                }
            }
        }
        return list;
    }


    private List<Map<String, Object>> parseSearchResponse(SearchResponse response) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            resultList.add(hit.getSourceAsMap());
        }
        return resultList;
    }


}