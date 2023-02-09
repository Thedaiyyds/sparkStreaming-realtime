package com.thedai.realtime.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import java.util
import scala.collection.mutable.ListBuffer

object MyEsUtils {
  private val esClient:RestHighLevelClient = build()
  def build(): RestHighLevelClient = {
    val host:String = MyPropertiesUtils("es.host")
    val port:String = MyPropertiesUtils("es.port")
    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost(host, port.toInt))
    val client: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)
    client
  }

  /**
   * 关闭es对象
   */
  def close(): Unit = {
    if(esClient != null) esClient.close()
  }

  /**
   * 批量幂等写
   */
  def bulkSave(indexName:String,docs:List[(String,AnyRef)]): Unit = {
    val bulkRequest = new BulkRequest()
    for ((docId,docObj) <- docs){
      val indexRequest = new IndexRequest(indexName)
      val dataJson: String = JSON.toJSONString(docObj, new SerializeConfig(true))
      indexRequest.source(dataJson,XContentType.JSON)
      indexRequest.id(docId)
      bulkRequest.add(indexRequest)
    }
    esClient.bulk(bulkRequest,RequestOptions.DEFAULT)
  }

  /**
   * 查询指定字段
   * @param indexName
   * @param fieldName
   * @return
   */
  def searchField(indexName:String,fieldName:String):List[String]={
    //判断索引是否存在
    val getIndexRequest = new GetIndexRequest(indexName)
    val isExists: Boolean = esClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT)
    if(!isExists){
      return null
    }

    //从ES中提取指定的字段
    val mids: ListBuffer[String] = ListBuffer[String]()
    val searchRequest = new SearchRequest(indexName)
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.fetchSource(fieldName,null)
    searchSourceBuilder.size(100000)
    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT)
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      val sourceAsMap: util.Map[String, AnyRef] = hit.getSourceAsMap
      val mid: String = sourceAsMap.get("source").toString
      mids.append(mid)
    }
    mids.toList
  }
}
