import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * es获取client，以及通过scroll获取全量数据
 *
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-6-21 下午6:30
 */
public class SearchScroll {

    public static void main(String[] args) throws Exception {

        //查询的条件
        String query = "{\n" +
            "    \"match\": {\n" +
            "      \"field\":\"value\"\n" +
            "    }\n" +
            "  }";
        String hosts = "localhost:9200";
        String username = "username";
        String password = "password";
        HttpHost[] httpHosts = Stream.of(hosts.split(","))
                                     .map(host -> {
                                         String[] hostPort = host.split(":");
                                         return new HttpHost(hostPort[0], Integer.parseInt(hostPort[1]), "http");
                                     })
                                     .toArray(HttpHost[]::new);

        RestClientBuilder builder = RestClient.builder(httpHosts);

        //下面这段es有帐号密码才需要
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        //获取es的client
        RestHighLevelClient client = new RestHighLevelClient(builder);



        //游标，设置游标超时的时间
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(2));
        SearchRequest request = new SearchRequest().indices("index") //设置index
                                                   .types("type") //设置type
                                                   .scroll(scroll) //设置以游标方式读取
                                                   .source(new SearchSourceBuilder().query(QueryBuilders.wrapperQuery(query))
                                                                                    .size(1000)); //设置查询条件，如果设置了以游标方式读取，这里的size是每次读游标时返回的大小
        //第一次搜索
        SearchResponse searchResponse = client.search(request);

        List<SearchHit> hits = new ArrayList<>(10000);

        while (true) {

            //获取游标的id
            String scrollId = searchResponse.getScrollId();

            //如下即获取本次请求的所有的hits
            searchResponse.getHits().iterator().forEachRemaining(hits::add);

            //新建通过游标搜索的请求，这里es内部实际已经维护了一个第一次查询的快照，后续以如下的方式每次通过游标获取一定数量的数据，直到全部获取
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(scroll);
            searchResponse = client.searchScroll(scrollRequest);

            if (searchResponse.getHits().getHits().length  == 0) {
                break;
            }
        }

        hits.forEach(hit-> System.out.println(hit.getSourceAsString()));

    }
}
