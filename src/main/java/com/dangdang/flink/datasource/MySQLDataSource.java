// Copyright (C) 2020 Meituan
// All rights reserved
package com.dangdang.flink.datasource;

import com.dangdang.flink.constant.PropertiesConstants;
import com.dangdang.flink.model.Student;
import com.dangdang.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author qiankai07
 * @version 1.0
 * Created on 2/25/20 12:37 PM
 **/
public class MySQLDataSource {

    private static final Logger log = LoggerFactory.getLogger(MySQLDataSource.class);

    public static DataStream<Student> readFromDb(StreamExecutionEnvironment env) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;

        TypeInformation[] fieldTypes = new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO};

        String[] fieldNames = new String[]{"id", "name", "password", "age"};

        String jdbcUrl = "jdbc:mysql://" + parameterTool.get(PropertiesConstants.MYSQL_HOST)
                + ":"
                + parameterTool.getInt(PropertiesConstants.MYSQL_PORT) +
                "/"
                + parameterTool.get(PropertiesConstants.MYSQL_DATABASE)
                + "?characterEncoding=utf8";

        log.info(jdbcUrl);

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat().setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl(jdbcUrl)
                .setUsername(parameterTool.get(PropertiesConstants.MYSQL_USERNAME)).setPassword(parameterTool.get(PropertiesConstants.MYSQL_PASSWORD))
                .setQuery("select id, name, password, age from student").setRowTypeInfo(rowTypeInfo)
                .finish();

        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> dataStreamSourceRow = env.createInput(jdbcInputFormat);

        dataStreamSourceRow.print();

        DataStream<Student> dataStream = dataStreamSourceRow.map(new RichMapFunction<Row, Student>() {
            @Override
            public Student map(Row value) throws Exception {
                Student s = new Student();
                s.setId((Integer) value.getField(0));
                s.setName((String) value.getField(1));
                s.setPassword((String) value.getField(2));
                s.setAge((Integer) value.getField(3));
                return s;
            }
        });

        log.info("read datasource end");
        return dataStream;
    }


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        readFromDb(env);
    }

}