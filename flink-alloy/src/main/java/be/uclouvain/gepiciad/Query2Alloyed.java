package be.uclouvain.gepiciad;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Query2Alloyed {
    public static void main(String[] args){
        ParameterTool pt = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment settings = StreamExecutionEnvironment.getExecutionEnvironment();
        settings.setParallelism(pt.getInt("parallelism", 2));
        final StreamTableEnvironment env = StreamTableEnvironment.create(settings);
        String bootstrap = pt.get("bootstrapServers", "kafka-edge1:9092");
        String bidTopic = pt.get("bidTopic", "bid");
        String offset = pt.get("offset", "earliest");
        String fetchMinBytes = pt.get("fetchMinBytes", "10000");
        env.executePlan(PlanReference.fromJsonString("{\"flinkVersion\": \"1.16\", \"nodes\": [{\"id\": 4, \"type\": \"stream-exec-table-source-scan_1\", \"scanTableSource\": {\"table\": {\"identifier\": \"`default_catalog`.`default_database`.`bid`\", \"resolvedTable\": {\"schema\": {\"columns\": [{\"name\": \"auction\", \"dataType\": \"BIGINT\"}, {\"name\": \"price\", \"dataType\": \"BIGINT\"}], \"watermarkSpecs\": []}, \"partitionKeys\": [], \"options\": {\"properties.bootstrap.servers\": \"envoy1:9093\", \"properties.fetch.max.wait.ms\": \"500\", \"properties.fetch.min.bytes\": \"" + fetchMinBytes + "\", \"connector\": \"kafka\", \"format\": \"json\", \"topic\": \"" + bidTopic + "\", \"properties.group.id\": \"nexmark\", \"scan.startup.mode\": \"" + offset + "-offset\", \"sink.partitioner\": \"round-robin\"}}}}, \"outputType\": {\"type\": \"ROW\", \"fields\": [{\"name\": \"auction\", \"fieldType\": \"BIGINT\"}, {\"name\": \"price\", \"fieldType\": \"BIGINT\"}]}, \"description\": \"TableSourceScan(table=[[default_catalog, default_database, bid, watermark=[-(dateTime, 4000:INTERVAL SECOND)]]], fields=[auction, bidder, price, dateTime, extra])\", \"inputProperties\": []}, {\"id\": 5, \"type\": \"stream-exec-calc_1\", \"projection\": [{\"kind\": \"INPUT_REF\", \"inputIndex\": 0, \"type\": \"BIGINT\"}, {\"kind\": \"CALL\", \"syntax\": \"SPECIAL\", \"internalName\": \"$CAST$1\", \"operands\": [{\"kind\": \"INPUT_REF\", \"inputIndex\": 1, \"type\": \"BIGINT\"}], \"type\": \"DECIMAL(23, 3)\"}], \"condition\": null, \"inputProperties\": [{\"requiredDistribution\": {\"type\": \"UNKNOWN\"}, \"damBehavior\": \"PIPELINED\", \"priority\": 0}], \"outputType\": \"ROW<`auction` BIGINT, `price` DECIMAL(23, 3)>\", \"description\": \"Calc(select=[auction, CAST(price AS DECIMAL(23, 3)) AS price], where=[SEARCH(auction, Sarg[1007, 1020, 2001, 2019, 2087])])\"}, {\"id\": 6, \"type\": \"stream-exec-sink_1\", \"configuration\": {\"table.exec.sink.keyed-shuffle\": \"AUTO\", \"table.exec.sink.not-null-enforcer\": \"ERROR\", \"table.exec.sink.type-length-enforcer\": \"IGNORE\", \"table.exec.sink.upsert-materialize\": \"AUTO\"}, \"dynamicTableSink\": {\"table\": {\"identifier\": \"`default_catalog`.`default_database`.`nexmark_q2`\", \"resolvedTable\": {\"schema\": {\"columns\": [{\"name\": \"auction\", \"dataType\": \"BIGINT\"}, {\"name\": \"price\", \"dataType\": \"DECIMAL(23, 3)\"}], \"watermarkSpecs\": []}, \"partitionKeys\": [], \"options\": {\"connector\": \"blackhole\"}}}}, \"inputChangelogMode\": [\"INSERT\"], \"inputProperties\": [{\"requiredDistribution\": {\"type\": \"UNKNOWN\"}, \"damBehavior\": \"PIPELINED\", \"priority\": 0}], \"outputType\": \"ROW<`auction` BIGINT, `price` DECIMAL(23, 3)>\", \"description\": \"Sink(table=[default_catalog.default_database.nexmark_q2], fields=[auction, price])\"}], \"edges\": [{\"source\": 4, \"target\": 5, \"shuffle\": {\"type\": \"FORWARD\"}, \"shuffleMode\": \"PIPELINED\"}, {\"source\": 5, \"target\": 6, \"shuffle\": {\"type\": \"FORWARD\"}, \"shuffleMode\": \"PIPELINED\"}]}"));
    }
}
