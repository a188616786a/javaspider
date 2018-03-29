import org.apache.log4j.BasicConfigurator;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import us.codecraft.webmagic.*;
import us.codecraft.webmagic.pipeline.Pipeline;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.scheduler.RedisScheduler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class JianshuPageProcessor implements PageProcessor {

    private Site site = Site.me().setRetryTimes(2).setSleepTime(1000).setTimeOut(1500);
    private static final String PAGE_URL_RE = "https://www\\.jianshu\\.com/p/";
    private static final String PAGE_URL = "https://www.jianshu.com/p/";

    private String getRecommandUrl(String pageid) {
        return "https://www.jianshu.com/notes/" + pageid + "/recommendations";
    }

    @Override
    public void process(Page page) {
        if (page.getUrl().regex(PAGE_URL_RE).match()) {
            page.putField("pageid", page.getUrl().regex(PAGE_URL_RE+"([\\w\\d]+)").toString());
            page.putField("content", page.getHtml().css("div.show-content-free").get());
        } else {
            page.setSkip(true);
            List<String> pageids = page.getJson().jsonPath("[*].slug").all();
            for (String pageid: pageids) {
                page.addTargetRequest(PAGE_URL + pageid);
                page.addTargetRequest(getRecommandUrl(pageid));
            }
        }
    }

    @Override
    public Site getSite() {
        return site;
    }

    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();
        Properties redisprop = new Properties();
        FileInputStream in = new FileInputStream("config/redis.properties");
        //        HttpClientDownloader httpClientDownloader = new HttpClientDownloader();
        redisprop.load(in);
        in.close();
//        httpClientDownloader.setProxyProvider(SimpleProxyProvider.from(new Proxy("localhost", 1080)));
        Spider.create(new JianshuPageProcessor()).
//                setDownloader(httpClientDownloader).
                setScheduler(new RedisScheduler(new JedisPool(new JedisPoolConfig(), redisprop.getProperty("host"), Integer.parseInt(redisprop.getProperty("port")), Integer.parseInt(redisprop.getProperty("timeout")), redisprop.getProperty("auth")))
//                .setDuplicateRemover(new BloomFilterDuplicateRemover(1000000))
                ).

                addUrl("https://www.jianshu.com/notes/25814983/recommendations").
                addPipeline(new MysqlPipline()).
                thread(3).run();
    }
}

class MysqlPipline implements Pipeline {
    @Override
    public void process(ResultItems resultItems, Task task) {
        System.out.println(resultItems.get("pageid").toString());
        Connection conn = Dbcp.getConnection();
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(
                    "insert into jianshu (pageid, content) VALUES (?, ?)");
            preparedStatement.setString(1, resultItems.get("pageid").toString());
            preparedStatement.setString(2, resultItems.get("content").toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            System.out.println(resultItems.get("content"));
            e.printStackTrace();
        }
    }
}