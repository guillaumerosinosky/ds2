package be.uclouvain.gepiciad;

import be.uclouvain.gepiciad.schemas.Column;
import be.uclouvain.gepiciad.schemas.Schema;
import be.uclouvain.gepiciad.schemas.SchemaProvider;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Tpcds {
    private static final List<String> TPCDS_TABLES =
            Arrays.asList(
                    "catalog_sales",
                    "catalog_returns",
                    "inventory",
                    "store_sales",
                    "store_returns",
                    "web_sales",
                    "web_returns",
                    "call_center",
                    "catalog_page",
                    "customer",
                    "customer_address",
                    "customer_demographics",
                    "date_dim",
                    "household_demographics",
                    "income_band",
                    "item",
                    "promotion",
                    "reason",
                    "ship_mode",
                    "store",
                    "time_dim",
                    "warehouse",
                    "web_page",
                    "web_site");

    private static final String QUERY_PREFIX = "query";
    private static final String QUERY_SUFFIX = ".sql";
    private static final String DATA_SUFFIX = ".dat";
    private static final String RESULT_SUFFIX = ".ans";
    private static final String COL_DELIMITER = "|";
    private static final String FILE_SEPARATOR = "/";

    public static void main(String[] args) {
        StreamTableEnvironment tableEnvironment = prepareTableEnv();
        tableEnvironment.executeSql("CREATE TABLE motivation(\n" +
                "    c_customer_id  VARCHAR\n" +
                ") WITH (\n" +
                "    'connector' = 'blackhole'\n" +
                ");");
        System.out.println(tableEnvironment.compilePlanSql("INSERT INTO motivation with customer_total_return as\n" +
                "(select sr_customer_sk as ctr_customer_sk\n" +
                ",sr_store_sk as ctr_store_sk\n" +
                ",sum(sr_return_amt) as ctr_total_return\n" +
                "from store_returns\n" +
                ",date_dim\n" +
                "where sr_returned_date_sk = d_date_sk\n" +
                "and d_year = 2000\n" +
                "group by sr_customer_sk\n" +
                ",sr_store_sk)\n" +
                " select  c_customer_id\n" +
                "from customer_total_return ctr1\n" +
                ",store\n" +
                ",customer\n" +
                "where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2\n" +
                "from customer_total_return ctr2\n" +
                "where ctr1.ctr_store_sk = ctr2.ctr_store_sk)\n" +
                "and s_store_sk = ctr1.ctr_store_sk\n" +
                "and s_state = 'TN'\n" +
                "and ctr1.ctr_customer_sk = c_customer_sk\n" +
                "order by c_customer_id\n" +
                "limit 100").asJsonString());
    }


    private static StreamTableEnvironment prepareTableEnv() {
        // init Table Env
        StreamExecutionEnvironment settings = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(settings);

        // config Optimizer parameters
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD,
                        10 * 1024 * 1024L);
        tEnv.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);

        // register TPC-DS tables
        TPCDS_TABLES.forEach(
                table -> {
                    tEnv.executeSql(KakfaTableCreationStatement(table));
                });
        return tEnv;
    }

    private static String KakfaTableCreationStatement(String table) {
        String res = "";
        for (Column c :
                SchemaProvider.getTableSchema(table).columns) {
            res = res.concat(c.toString());
        }
        return "CREATE TABLE " +
                table +
                "(\n" +
                res.substring(0, res.length()-2) +
                "\n)" +
                "WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '"+ table + "',\n" +
                "    'properties.bootstrap.servers' = 'kafka-edge1:9092',\n" +
                "    'properties.group.id' = 'nexmark',\n" +
                "    'properties.fetch.max.wait.ms' = '500',\n" +
                "    'properties.fetch.min.bytes' = '100000',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'sink.partitioner' = 'round-robin',\n" +
                "    'format' = 'json'\n" +
                ");";
    }
}
