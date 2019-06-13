package scouter.plugin.server.elasticsearch;

import lombok.Builder;
import lombok.Setter;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import scouter.server.Configure;
import scouter.server.Logger;
import scouter.util.DateUtil;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * @author Heo Yeo Song (yosong.heo@gmail.com) on 2019. 6. 13.
 */
@Builder
@Setter
public class HttpClient {

    final Configure conf = Configure.getInstance();

    String address;
    String user;
    String password;
    boolean isHttps;

    RestHighLevelClient highLevelClient;
    BulkProcessor bulkProcessor;

    public void init(){
        build();

    }

    private void build() {
        HttpHost[] httpHosts = Arrays.stream(address.split(","))
                .map((node) -> {
                    String host = node.split(":")[0];
                    String port = node.split(":")[1];
                    return new HttpHost(host, Integer.valueOf(port),isHttps ? "https" : "http");
                })
                .toArray(size -> new HttpHost[size]);

        this.highLevelClient = new RestHighLevelClient(
                RestClient.builder(httpHosts)
                          .setHttpClientConfigCallback((builder)->{
                            if(!user.isEmpty() && !password.isEmpty()){
                                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
                                builder.setDefaultCredentialsProvider(credentialsProvider);
                            }

                            try {
                                SSLContextBuilder ssl = SSLContexts.custom();
                                ssl.loadTrustMaterial(null,(chain, authType) -> true);

                                builder.setSSLContext(ssl.build())
                                        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);

                            }catch (Exception e){
                                if(conf._trace) {
                                    Logger.printStackTrace("ES-INIT-ERROR", e);
                                }else{
                                    Logger.println("ES-INIT-ERROR ",e.getMessage());
                                }
                            }
                    return builder;
                })
        );
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {

                if (response.hasFailures()) {
                    Logger.println("Bulk executed with failures  :", response.buildFailureMessage());
                }
                if( conf._trace){
                    Logger.println("Bulk completed in milliseconds",  response.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                failure.printStackTrace();
            }
        };

        this.bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) ->
                        highLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener)
                .setBulkActions(2000)
                .setBulkSize(new ByteSizeValue(4, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5L))
                .setConcurrentRequests(3)
                .build();

    }

    private void close() {
        try {
            this.highLevelClient.close();
        } catch (IOException e) {
            if(conf._trace) {
                Logger.printStackTrace("ES-CLOSE-ERROR", e);
            }else{
                Logger.println("ES-CLOSE-ERROR",e.getMessage());
            }
        }
    }

    public void reload(){
        this.close();
        build();
    }

    public void put(String indexName, String id, Map<String, Object> source) {
        this.bulkProcessor.add(new IndexRequest(indexName).id(id).source(source));
    }

    public void flush() {
        this.bulkProcessor.flush();
    }

    public void deleteIndex(List<String> indexPattern, int esIndexDuration) {


        final int _start = esIndexDuration * 2; // before endDay
        final int _end   = esIndexDuration; // total counter;
        final Calendar cal = GregorianCalendar.getInstance();
        IntStream.iterate(_start, n -> n-1 )
                .limit(_end)
                .mapToObj(x ->{
                    // 현재 기준 이전 날짜를 가져오기
                    cal.add(5, x > 0 ? (x * -1) : x);
                   return cal.getTime().getTime();
                })
                .map(_ts -> DateUtil.format(_ts,"yyyy-MM-dd"))
                .collect(Collectors.toList())
                .stream()
                .map(_day -> {
                    List<String> pattern = new ArrayList<>();
                    for(String pt :  indexPattern) {
                        pattern.add(String.join("", pt, "*", _day, "*"));
                    }
                    return pattern;
                } )
                .flatMap(List::stream)
                .forEach(_indexName ->{
                    try {

                        GetIndexRequest getRequest = new GetIndexRequest(_indexName);
                        DeleteIndexRequest delRequest = new DeleteIndexRequest(_indexName);

                        if(this.highLevelClient.indices().exists(getRequest, RequestOptions.DEFAULT)) {
                            AcknowledgedResponse acknowledgedResponse= this.highLevelClient.indices().delete(delRequest,RequestOptions.DEFAULT);
                            if(acknowledgedResponse.isAcknowledged()){
                                Logger.println("index pattern delete : success",_indexName);
                            }

                        }
                    }catch (Throwable e){
                        Logger.printStackTrace("ES-DELETE-ERROR", e);
                    }
                });
    }


}
