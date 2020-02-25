// Copyright (C) 2020 Meituan
// All rights reserved
package com.dangdang.flink;

import com.dangdang.flink.datasource.JdbcReader;
import com.dangdang.flink.datasource.JdbcReader2;
import com.dangdang.flink.datasource.MySQLDataSource;
import com.dangdang.flink.model.Student;
import com.dangdang.flink.sink.MySQLSink2;
import com.dangdang.flink.util.FileUtil;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 通过TableAPI读取数据
 * @author qiankai07
 * @version 1.0
 * Created on 2/25/20 11:17 PM
 **/
public class Main3 {

    private static final Logger log = LoggerFactory.getLogger(Main3.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Student> studentDataStream = env.addSource(new JdbcReader2());
        //DataStream<Student> studentDataStream = MySQLDataSource.readFromDb(env);


        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                //.useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, settings);

        //从DataStream获取数据
        Table table = streamTableEnvironment.fromDataStream(studentDataStream);

        streamTableEnvironment.createTemporaryView("temp_table", table);

        /*
        Table sorted = streamTableEnvironment.sqlQuery("select id from tempTable");

        DataStream<Row> rowDataStream = streamTableEnvironment.toAppendStream(sorted, Row.class);
        rowDataStream.print();
        */
        //创建sink内部Table
        String destSql = FileUtil.readSourceFile("destination.sql");
        streamTableEnvironment.sqlUpdate(destSql);

        //将内部Table插入到outer system
        String insertSql = FileUtil.readSourceFile("insert.sql");

        streamTableEnvironment.sqlUpdate(insertSql);


        env.execute("sort-streaming-data");

        log.info("end");

    }
}