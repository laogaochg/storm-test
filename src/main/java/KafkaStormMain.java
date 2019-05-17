import function.OutFunction;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StaticHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;

/**
 * @Author: LaoGaoChuang
 * @Date : 2019/5/16 16:07
 */
public class KafkaStormMain {
    public static void main(String[] args) throws InterruptedException {
        BrokerHosts zk = new ZkHosts("localhost:2181");
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(zk, "testTopic");
        //用于设置kafka的连接属性
        //当发送信息的主线程退出太快会发生消息没有发送出去的bug
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(kafkaConfig);
        TridentTopology builder = new TridentTopology();
        builder.newStream("kafka-stream", spout)
                //str的来由是StringScheme声明的输出Fields
                .each(new Fields("str"), new Split(), new Fields("item"))
                .each(new Fields("item"), new OutFunction(), new Fields("OutFunction-out"));
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        cluster.submitTopology("Word-counts", conf, builder.build());
        Thread.sleep(20000);
    }
}
