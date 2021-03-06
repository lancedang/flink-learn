package com.dangdang.flink;


import com.dangdang.flink.datasource.JdbcReader;
import com.dangdang.flink.model.Student;
import com.dangdang.flink.sink.MySQLSink2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 通过DataStream API读取数据使用RichSourceFunction做DataSource实现
 */
public class Main2 {

    private static final Logger log = LoggerFactory.getLogger(Main2.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<List<Student>> studentDataStream = env.addSource(new JdbcReader());
        studentDataStream.addSink(new MySQLSink2()).setParallelism(1);
        env.execute();
    }


}