package be.uclouvain.gepiciad;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class QueryExecutor {
    public static void main(String[] args){
        ParameterTool pt = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment settings = StreamExecutionEnvironment.getExecutionEnvironment();
        settings.setParallelism(pt.getInt("parallelism", 2));
        final StreamTableEnvironment env = StreamTableEnvironment.create(settings);
        env.executePlan(PlanReference.fromJsonString(pt.get("queryPlan")));
    }
}
