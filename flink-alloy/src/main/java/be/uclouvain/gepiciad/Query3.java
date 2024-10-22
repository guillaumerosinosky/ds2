package be.uclouvain.gepiciad;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Query3 {
    public static void main(String[] args){
        ParameterTool pt = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment settings = StreamExecutionEnvironment.getExecutionEnvironment();
        settings.setParallelism(pt.getInt("parallelism", 2));
        final StreamTableEnvironment env = StreamTableEnvironment.create(settings);
        String bootstrap = pt.get("bootstrapServers", "kafka-edge1:9092");
        String auctionTopic = pt.get("auctionTopic", "auction");
        String personTopic = pt.get("personTopic", "person");
        String offset = pt.get("offset", "earliest");
        String fetchMinBytes = pt.get("fetchMinBytes", "10000");
        env.executePlan(PlanReference.fromJsonString("{\"flinkVersion\": \"1.16\", \"nodes\": [{\"id\": 7, \"type\": \"stream-exec-table-source-scan_1\", \"scanTableSource\": {\"table\": {\"identifier\": \"`default_catalog`.`default_database`.`auction`\", \"resolvedTable\": {\"schema\": {\"columns\": [{\"name\": \"id\", \"dataType\": \"BIGINT\"}, {\"name\": \"itemName\", \"dataType\": \"VARCHAR(2147483647)\"}, {\"name\": \"description\", \"dataType\": \"VARCHAR(2147483647)\"}, {\"name\": \"initialBid\", \"dataType\": \"BIGINT\"}, {\"name\": \"reserve\", \"dataType\": \"BIGINT\"}, {\"name\": \"dateTime\", \"dataType\": {\"type\": \"TIMESTAMP_WITHOUT_TIME_ZONE\", \"precision\": 3, \"kind\": \"ROWTIME\"}}, {\"name\": \"expires\", \"dataType\": \"TIMESTAMP(3)\"}, {\"name\": \"seller\", \"dataType\": \"BIGINT\"}, {\"name\": \"category\", \"dataType\": \"BIGINT\"}, {\"name\": \"extra\", \"dataType\": \"VARCHAR(2147483647)\"}], \"watermarkSpecs\": [{\"rowtimeAttribute\": \"dateTime\", \"expression\": {\"rexNode\": {\"kind\": \"CALL\", \"syntax\": \"SPECIAL\", \"internalName\": \"$-$1\", \"operands\": [{\"kind\": \"INPUT_REF\", \"inputIndex\": 5, \"type\": \"TIMESTAMP(3)\"}, {\"kind\": \"LITERAL\", \"value\": \"4000\", \"type\": \"INTERVAL SECOND(6) NOT NULL\"}], \"type\": \"TIMESTAMP(3)\"}, \"serializableString\": \"`dateTime` - INTERVAL '4' SECOND\"}}]}, \"partitionKeys\": [], \"options\": {\"properties.bootstrap.servers\": \"" + bootstrap +"\", \"properties.fetch.max.wait.ms\": \"500\", \"properties.fetch.min.bytes\": \"" + fetchMinBytes + "\", \"connector\": \"kafka\", \"format\": \"json\", \"topic\": \"" + auctionTopic + "\", \"properties.group.id\": \"" + auctionTopic + "\", \"scan.startup.mode\": \"" + offset + "-offset\", \"sink.partitioner\": \"round-robin\"}}}, \"abilities\": [{\"type\": \"WatermarkPushDown\", \"watermarkExpr\": {\"kind\": \"CALL\", \"syntax\": \"SPECIAL\", \"internalName\": \"$-$1\", \"operands\": [{\"kind\": \"INPUT_REF\", \"inputIndex\": 5, \"type\": \"TIMESTAMP(3)\"}, {\"kind\": \"LITERAL\", \"value\": \"4000\", \"type\": \"INTERVAL SECOND(6) NOT NULL\"}], \"type\": \"TIMESTAMP(3)\"}, \"idleTimeoutMillis\": -1, \"producedType\": {\"type\": \"ROW\", \"nullable\": false, \"fields\": [{\"name\": \"id\", \"fieldType\": \"BIGINT\"}, {\"name\": \"itemName\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"description\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"initialBid\", \"fieldType\": \"BIGINT\"}, {\"name\": \"reserve\", \"fieldType\": \"BIGINT\"}, {\"name\": \"dateTime\", \"fieldType\": {\"type\": \"TIMESTAMP_WITHOUT_TIME_ZONE\", \"precision\": 3, \"kind\": \"ROWTIME\"}}, {\"name\": \"expires\", \"fieldType\": \"TIMESTAMP(3)\"}, {\"name\": \"seller\", \"fieldType\": \"BIGINT\"}, {\"name\": \"category\", \"fieldType\": \"BIGINT\"}, {\"name\": \"extra\", \"fieldType\": \"VARCHAR(2147483647)\"}]}}]}, \"outputType\": {\"type\": \"ROW\", \"fields\": [{\"name\": \"id\", \"fieldType\": \"BIGINT\"}, {\"name\": \"itemName\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"description\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"initialBid\", \"fieldType\": \"BIGINT\"}, {\"name\": \"reserve\", \"fieldType\": \"BIGINT\"}, {\"name\": \"dateTime\", \"fieldType\": {\"type\": \"TIMESTAMP_WITHOUT_TIME_ZONE\", \"precision\": 3, \"kind\": \"ROWTIME\"}}, {\"name\": \"expires\", \"fieldType\": \"TIMESTAMP(3)\"}, {\"name\": \"seller\", \"fieldType\": \"BIGINT\"}, {\"name\": \"category\", \"fieldType\": \"BIGINT\"}, {\"name\": \"extra\", \"fieldType\": \"VARCHAR(2147483647)\"}]}, \"description\": \"TableSourceScan(table=[[default_catalog, default_database, auction, watermark=[-(dateTime, 4000:INTERVAL SECOND)]]], fields=[id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra])\", \"inputProperties\": []}, {\"id\": 8, \"type\": \"stream-exec-calc_1\", \"projection\": [{\"kind\": \"INPUT_REF\", \"inputIndex\": 0, \"type\": \"BIGINT\"}, {\"kind\": \"INPUT_REF\", \"inputIndex\": 7, \"type\": \"BIGINT\"}], \"condition\": {\"kind\": \"CALL\", \"syntax\": \"INTERNAL\", \"internalName\": \"$SEARCH$1\", \"operands\": [{\"kind\": \"INPUT_REF\", \"inputIndex\": 8, \"type\": \"BIGINT\"}, {\"kind\": \"LITERAL\", \"sarg\": {\"ranges\": [{\"lower\": {\"value\": 10, \"boundType\": \"CLOSED\"}, \"upper\": {\"value\": 10, \"boundType\": \"CLOSED\"}}], \"containsNull\": false}, \"type\": \"BIGINT NOT NULL\"}], \"type\": \"BOOLEAN\"}, \"inputProperties\": [{\"requiredDistribution\": {\"type\": \"UNKNOWN\"}, \"damBehavior\": \"PIPELINED\", \"priority\": 0}], \"outputType\": \"ROW<`id` BIGINT, `seller` BIGINT>\", \"description\": \"Calc(select=[id, seller], where=[SEARCH(category, Sarg[10])])\"}, {\"id\": 9, \"type\": \"stream-exec-exchange_1\", \"inputProperties\": [{\"requiredDistribution\": {\"type\": \"HASH\", \"keys\": [1]}, \"damBehavior\": \"PIPELINED\", \"priority\": 0}], \"outputType\": \"ROW<`id` BIGINT, `seller` BIGINT>\", \"description\": \"Exchange(distribution=[hash[seller]])\"}, {\"id\": 10, \"type\": \"stream-exec-table-source-scan_1\", \"scanTableSource\": {\"table\": {\"identifier\": \"`default_catalog`.`default_database`.`person`\", \"resolvedTable\": {\"schema\": {\"columns\": [{\"name\": \"id\", \"dataType\": \"BIGINT\"}, {\"name\": \"name\", \"dataType\": \"VARCHAR(2147483647)\"}, {\"name\": \"emailAddress\", \"dataType\": \"VARCHAR(2147483647)\"}, {\"name\": \"creditCard\", \"dataType\": \"VARCHAR(2147483647)\"}, {\"name\": \"city\", \"dataType\": \"VARCHAR(2147483647)\"}, {\"name\": \"state\", \"dataType\": \"VARCHAR(2147483647)\"}, {\"name\": \"dateTime\", \"dataType\": {\"type\": \"TIMESTAMP_WITHOUT_TIME_ZONE\", \"precision\": 3, \"kind\": \"ROWTIME\"}}, {\"name\": \"extra\", \"dataType\": \"VARCHAR(2147483647)\"}], \"watermarkSpecs\": [{\"rowtimeAttribute\": \"dateTime\", \"expression\": {\"rexNode\": {\"kind\": \"CALL\", \"syntax\": \"SPECIAL\", \"internalName\": \"$-$1\", \"operands\": [{\"kind\": \"INPUT_REF\", \"inputIndex\": 6, \"type\": \"TIMESTAMP(3)\"}, {\"kind\": \"LITERAL\", \"value\": \"4000\", \"type\": \"INTERVAL SECOND(6) NOT NULL\"}], \"type\": \"TIMESTAMP(3)\"}, \"serializableString\": \"`dateTime` - INTERVAL '4' SECOND\"}}]}, \"partitionKeys\": [], \"options\": {\"properties.bootstrap.servers\": \"" + bootstrap + "\", \"properties.fetch.max.wait.ms\": \"500\", \"properties.fetch.min.bytes\": \"" + fetchMinBytes + "\", \"connector\": \"kafka\", \"format\": \"json\", \"topic\": \"" + personTopic + "\", \"properties.group.id\": \"" + personTopic + "\", \"scan.startup.mode\": \"" + offset + "-offset\", \"sink.partitioner\": \"round-robin\"}}}, \"abilities\": [{\"type\": \"WatermarkPushDown\", \"watermarkExpr\": {\"kind\": \"CALL\", \"syntax\": \"SPECIAL\", \"internalName\": \"$-$1\", \"operands\": [{\"kind\": \"INPUT_REF\", \"inputIndex\": 6, \"type\": \"TIMESTAMP(3)\"}, {\"kind\": \"LITERAL\", \"value\": \"4000\", \"type\": \"INTERVAL SECOND(6) NOT NULL\"}], \"type\": \"TIMESTAMP(3)\"}, \"idleTimeoutMillis\": -1, \"producedType\": {\"type\": \"ROW\", \"nullable\": false, \"fields\": [{\"name\": \"id\", \"fieldType\": \"BIGINT\"}, {\"name\": \"name\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"emailAddress\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"creditCard\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"city\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"state\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"dateTime\", \"fieldType\": {\"type\": \"TIMESTAMP_WITHOUT_TIME_ZONE\", \"precision\": 3, \"kind\": \"ROWTIME\"}}, {\"name\": \"extra\", \"fieldType\": \"VARCHAR(2147483647)\"}]}}]}, \"outputType\": {\"type\": \"ROW\", \"fields\": [{\"name\": \"id\", \"fieldType\": \"BIGINT\"}, {\"name\": \"name\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"emailAddress\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"creditCard\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"city\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"state\", \"fieldType\": \"VARCHAR(2147483647)\"}, {\"name\": \"dateTime\", \"fieldType\": {\"type\": \"TIMESTAMP_WITHOUT_TIME_ZONE\", \"precision\": 3, \"kind\": \"ROWTIME\"}}, {\"name\": \"extra\", \"fieldType\": \"VARCHAR(2147483647)\"}]}, \"description\": \"TableSourceScan(table=[[default_catalog, default_database, person, watermark=[-(dateTime, 4000:INTERVAL SECOND)]]], fields=[id, name, emailAddress, creditCard, city, state, dateTime, extra])\", \"inputProperties\": []}, {\"id\": 11, \"type\": \"stream-exec-calc_1\", \"projection\": [{\"kind\": \"INPUT_REF\", \"inputIndex\": 0, \"type\": \"BIGINT\"}, {\"kind\": \"INPUT_REF\", \"inputIndex\": 1, \"type\": \"VARCHAR(2147483647)\"}, {\"kind\": \"INPUT_REF\", \"inputIndex\": 4, \"type\": \"VARCHAR(2147483647)\"}, {\"kind\": \"INPUT_REF\", \"inputIndex\": 5, \"type\": \"VARCHAR(2147483647)\"}], \"condition\": {\"kind\": \"CALL\", \"syntax\": \"INTERNAL\", \"internalName\": \"$SEARCH$1\", \"operands\": [{\"kind\": \"INPUT_REF\", \"inputIndex\": 5, \"type\": \"VARCHAR(2147483647)\"}, {\"kind\": \"LITERAL\", \"sarg\": {\"ranges\": [{\"lower\": {\"value\": \"CA\", \"boundType\": \"CLOSED\"}, \"upper\": {\"value\": \"CA\", \"boundType\": \"CLOSED\"}}, {\"lower\": {\"value\": \"ID\", \"boundType\": \"CLOSED\"}, \"upper\": {\"value\": \"ID\", \"boundType\": \"CLOSED\"}}, {\"lower\": {\"value\": \"OR\", \"boundType\": \"CLOSED\"}, \"upper\": {\"value\": \"OR\", \"boundType\": \"CLOSED\"}}], \"containsNull\": false}, \"type\": \"VARCHAR(2147483647) NOT NULL\"}], \"type\": \"BOOLEAN\"}, \"inputProperties\": [{\"requiredDistribution\": {\"type\": \"UNKNOWN\"}, \"damBehavior\": \"PIPELINED\", \"priority\": 0}], \"outputType\": \"ROW<`id` BIGINT, `name` VARCHAR(2147483647), `city` VARCHAR(2147483647), `state` VARCHAR(2147483647)>\", \"description\": \"Calc(select=[id, name, city, state], where=[SEARCH(state, Sarg[_UTF-16LE'CA', _UTF-16LE'ID', _UTF-16LE'OR'])])\"}, {\"id\": 12, \"type\": \"stream-exec-exchange_1\", \"inputProperties\": [{\"requiredDistribution\": {\"type\": \"HASH\", \"keys\": [0]}, \"damBehavior\": \"PIPELINED\", \"priority\": 0}], \"outputType\": \"ROW<`id` BIGINT, `name` VARCHAR(2147483647), `city` VARCHAR(2147483647), `state` VARCHAR(2147483647)>\", \"description\": \"Exchange(distribution=[hash[id]])\"}, {\"id\": 13, \"type\": \"stream-exec-join_1\", \"joinSpec\": {\"joinType\": \"INNER\", \"leftKeys\": [1], \"rightKeys\": [0], \"filterNulls\": [true], \"nonEquiCondition\": null}, \"inputProperties\": [{\"requiredDistribution\": {\"type\": \"UNKNOWN\"}, \"damBehavior\": \"PIPELINED\", \"priority\": 0}, {\"requiredDistribution\": {\"type\": \"UNKNOWN\"}, \"damBehavior\": \"PIPELINED\", \"priority\": 0}], \"outputType\": \"ROW<`id` BIGINT, `seller` BIGINT, `id0` BIGINT, `name` VARCHAR(2147483647), `city` VARCHAR(2147483647), `state` VARCHAR(2147483647)>\", \"description\": \"Join(joinType=[InnerJoin], where=[(seller = id0)], select=[id, seller, id0, name, city, state], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])\"}, {\"id\": 14, \"type\": \"stream-exec-calc_1\", \"projection\": [{\"kind\": \"INPUT_REF\", \"inputIndex\": 3, \"type\": \"VARCHAR(2147483647)\"}, {\"kind\": \"INPUT_REF\", \"inputIndex\": 4, \"type\": \"VARCHAR(2147483647)\"}, {\"kind\": \"INPUT_REF\", \"inputIndex\": 5, \"type\": \"VARCHAR(2147483647)\"}, {\"kind\": \"INPUT_REF\", \"inputIndex\": 0, \"type\": \"BIGINT\"}], \"condition\": null, \"inputProperties\": [{\"requiredDistribution\": {\"type\": \"UNKNOWN\"}, \"damBehavior\": \"PIPELINED\", \"priority\": 0}], \"outputType\": \"ROW<`name` VARCHAR(2147483647), `city` VARCHAR(2147483647), `state` VARCHAR(2147483647), `id` BIGINT>\", \"description\": \"Calc(select=[name, city, state, id])\"}, {\"id\": 15, \"type\": \"stream-exec-sink_1\", \"configuration\": {\"table.exec.sink.keyed-shuffle\": \"AUTO\", \"table.exec.sink.not-null-enforcer\": \"ERROR\", \"table.exec.sink.type-length-enforcer\": \"IGNORE\", \"table.exec.sink.upsert-materialize\": \"AUTO\"}, \"dynamicTableSink\": {\"table\": {\"identifier\": \"`default_catalog`.`default_database`.`nexmark_q3`\", \"resolvedTable\": {\"schema\": {\"columns\": [{\"name\": \"name\", \"dataType\": \"VARCHAR(2147483647)\"}, {\"name\": \"city\", \"dataType\": \"VARCHAR(2147483647)\"}, {\"name\": \"state\", \"dataType\": \"VARCHAR(2147483647)\"}, {\"name\": \"id\", \"dataType\": \"BIGINT\"}], \"watermarkSpecs\": []}, \"partitionKeys\": [], \"options\": {\"connector\": \"blackhole\"}}}}, \"inputChangelogMode\": [\"INSERT\"], \"inputProperties\": [{\"requiredDistribution\": {\"type\": \"UNKNOWN\"}, \"damBehavior\": \"PIPELINED\", \"priority\": 0}], \"outputType\": \"ROW<`name` VARCHAR(2147483647), `city` VARCHAR(2147483647), `state` VARCHAR(2147483647), `id` BIGINT>\", \"description\": \"Sink(table=[default_catalog.default_database.nexmark_q3], fields=[name, city, state, id])\"}], \"edges\": [{\"source\": 7, \"target\": 8, \"shuffle\": {\"type\": \"FORWARD\"}, \"shuffleMode\": \"PIPELINED\"}, {\"source\": 8, \"target\": 9, \"shuffle\": {\"type\": \"FORWARD\"}, \"shuffleMode\": \"PIPELINED\"}, {\"source\": 10, \"target\": 11, \"shuffle\": {\"type\": \"FORWARD\"}, \"shuffleMode\": \"PIPELINED\"}, {\"source\": 11, \"target\": 12, \"shuffle\": {\"type\": \"FORWARD\"}, \"shuffleMode\": \"PIPELINED\"}, {\"source\": 9, \"target\": 13, \"shuffle\": {\"type\": \"FORWARD\"}, \"shuffleMode\": \"PIPELINED\"}, {\"source\": 12, \"target\": 13, \"shuffle\": {\"type\": \"FORWARD\"}, \"shuffleMode\": \"PIPELINED\"}, {\"source\": 13, \"target\": 14, \"shuffle\": {\"type\": \"FORWARD\"}, \"shuffleMode\": \"PIPELINED\"}, {\"source\": 14, \"target\": 15, \"shuffle\": {\"type\": \"FORWARD\"}, \"shuffleMode\": \"PIPELINED\"}]}"));
    }
}
