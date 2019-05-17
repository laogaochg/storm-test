
import bolts.WordCounter;
import bolts.WordNormalizer;
import grouping.ModuleGrouping;
import org.apache.log4j.LogManager;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spouts.WordReader;

public class WordCountTopologyMain {
    private static final Logger logger = LoggerFactory.getLogger(WordCountTopologyMain.class);

    @Test
    public void test() {
        logger.info("-------");
    }

    public static void main(String[] args) throws InterruptedException {
        //定义拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
//        builder.setBolt("word-normalizer", new WordNormalizer()).customGrouping("word-reader",new ModuleGrouping());
        builder.setBolt("word-counter", new WordCounter(), 2).customGrouping("word-normalizer", new ModuleGrouping());
//        builder.setBolt("word-counter", new WordCounter(),2).fieldsGrouping("word-normalizer", new Fields("word"));
//todo 在spout和bolts之间通过shuffleGrouping方法连接。这种分组方式决定了Storm会以随机分配方式从源节点向目标节点发送消息。
//        builder.setBolt("word-counter", new WordCounter(),2).shuffleGrouping("word-normalizer");
        //配置
        Config conf = new Config();
        conf.put("wordsFile", "D:\\workspace\\storm-test\\src\\main\\resources\\words.txt");
//        conf.put("test", "test");

        //运行拓扑
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
        Thread.sleep(20000);
        cluster.shutdown();
//        todo 提交拓扑到集群里面
//StormSubmitter.submitTopology("Count-Word-Topology-With_Refresh-Cache", conf,builder.createTopology());
//storm jar target/Topologies-0.0.1-SNAPSHOT.jar countword.TopologyMain src/main/resources/words.txt
//       todo 停止或杀死集群里面的拓扑，运行：
//storm kill Count-Word-Topology-With-Refresh-Cach
    }
}