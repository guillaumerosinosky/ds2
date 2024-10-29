package be.uclouvain.gepiciad;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class QueryMotivationProjectionAlloyed {
    public static void main(String[] args){
        ParameterTool pt = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment settings = StreamExecutionEnvironment.getExecutionEnvironment();
        settings.setParallelism(pt.getInt("parallelism", 2));
        final StreamTableEnvironment env = StreamTableEnvironment.create(settings);
        String bootstrap = pt.get("bootstrapServers", "kafka-edge1:9092");
        String auctionTopic = pt.get("auctionTopic", "bid");
        String offset = pt.get("offset", "earliest");
        String fetchMinBytes = pt.get("fetchMinBytes", "10000");
        env.executePlan(PlanReference.fromJsonString("{\"flinkVersion\": \"1.16\", \"nodes\": [{\"id\": 1, \"type\": \"stream-exec-table-source-scan_1\", \"scanTableSource\": {\"table\": {\"identifier\": \"`default_catalog`.`default_database`.`auction`\", \"resolvedTable\": {\"schema\": {\"columns\": [{\"name\": \"id\", \"dataType\": \"BIGINT\"}], \"watermarkSpecs\": []}, \"partitionKeys\": [], \"options\": {\"properties.bootstrap.servers\": \"" + bootstrap + "\", \"properties.fetch.max.wait.ms\": \"500\", \"properties.fetch.min.bytes\": \"" + fetchMinBytes + "\", \"connector\": \"kafka\", \"format\": \"json\", \"topic\": \"" + auctionTopic + "\", \"properties.group.id\": \"nexmark\", \"scan.startup.mode\": \"" + offset + "-offset\", \"sink.partitioner\": \"round-robin\"}}}}, \"outputType\": {\"type\": \"ROW\", \"fields\": [{\"name\": \"id\", \"fieldType\": \"BIGINT\"}]}, \"description\": \"TableSourceScan(table=[[default_catalog, default_database, auction, watermark=[-(dateTime, 4000:INTERVAL SECOND)]]], fields=[id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra])\", \"inputProperties\": []}, {\"id\": 3, \"type\": \"stream-exec-sink_1\", \"configuration\": {\"table.exec.sink.keyed-shuffle\": \"AUTO\", \"table.exec.sink.not-null-enforcer\": \"ERROR\", \"table.exec.sink.type-length-enforcer\": \"IGNORE\", \"table.exec.sink.upsert-materialize\": \"AUTO\"}, \"dynamicTableSink\": {\"table\": {\"identifier\": \"`default_catalog`.`default_database`.`motivation_projection`\", \"resolvedTable\": {\"schema\": {\"columns\": [{\"name\": \"id\", \"dataType\": \"BIGINT\"}], \"watermarkSpecs\": []}, \"partitionKeys\": [], \"options\": {\"connector\": \"blackhole\"}}}}, \"inputChangelogMode\": [\"INSERT\"], \"inputProperties\": [{\"requiredDistribution\": {\"type\": \"UNKNOWN\"}, \"damBehavior\": \"PIPELINED\", \"priority\": 0}], \"outputType\": \"ROW<`id` BIGINT>\", \"description\": \"Sink(table=[default_catalog.default_database.motivation_projection], fields=[id])\"}], \"edges\": [{\"source\": 1, \"target\": 3, \"shuffle\": {\"type\": \"FORWARD\"}, \"shuffleMode\": \"PIPELINED\"}]}"));
    }
}
