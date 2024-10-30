package org.example.type;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.time.ZoneId;

public class TypeTest {

    //Flink中的基础数据类型
    // 字符串: CHAR, VARCHAR, STRING
    // 二进制: BINARY, VARBINARY, BYTES
    // 数值: DECIMAL, NUMERIC, TINYINT, SMALLINT, INT\INTEGER, BIGINT, FLOAT, DOUBLE
    // 布尔: BOOLEAN
    // 空值: NULL
    // 日期时间: DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ, INTERVAL(INTERVAL YEAR TO MONTH)
    // INTERVAL DAY TO SECOND

    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.getConfig().set(CoreOptions.DEFAULT_PARALLELISM.key(),"1");
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        String tableSql = "create table test(" +
                " d_date DATE," +  // 日期类型 : 输出  2024-10-30
                " d_time TIME," + // 时间类型 : 输出 08:20:10
                " d_timestamp timestamp," +
                " d_timestamp2 timestamp(3)," +
                " d_timestamp_ltz timestamp_ltz," +
                " d_timestamp2_ltz timestamp_ltz(3)" +
                ") " +
                "with (" +
                "   'connector' = 'datagen'" + "," +
                "   'rows-per-second' = '1'" +
                ")";

        tEnv.executeSql(tableSql);
        tEnv.executeSql("select * from test").print();

    }



}
