package com.dangdang.flink;


import com.dangdang.flink.datasource.MySQLDataSource;
import com.dangdang.flink.model.Student;
import com.dangdang.flink.sink.MySQLSink;
import com.dangdang.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dangdang.flink.constant.PropertiesConstants.STREAM_SINK_PARALLELISM;


public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;

        DataStream<Student> studentDataStream = MySQLDataSource.readFromDb(env);

        studentDataStream.addSink(new MySQLSink()).setParallelism(parameterTool.getInt(STREAM_SINK_PARALLELISM, 1));

        env.execute();
    }
}