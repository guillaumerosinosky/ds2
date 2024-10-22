package be.uclouvain.gepiciad;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Query2 {
    public static void main(String[] args){
        ParameterTool pt = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment settings = StreamExecutionEnvironment.getExecutionEnvironment();
        settings.setParallelism(pt.getInt("parallelism", 2));
        final StreamTableEnvironment env = StreamTableEnvironment.create(settings);
        String bootstrap = pt.get("bootstrapServers", "kafka-edge1:9092");
        String bidTopic = pt.get("bidTopic", "bid");
        String offset = pt.get("offset", "earliest");
        String fetchMinBytes = pt.get("fetchMinBytes", "10000");
    }
}
