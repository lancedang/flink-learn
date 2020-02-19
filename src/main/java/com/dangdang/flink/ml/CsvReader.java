// Copyright (C) 2020 Meituan
// All rights reserved
package com.dangdang.flink.ml;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @author qiankai07
 * @version 1.0
 * Created on 2/18/20 4:25 PM
 **/
public class CsvReader {
    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "/Users/qiankai07/IdeaProjects/flink-learn/src/main/resources/csvData.csv";

        DataSource<CsvData> csvDataDataSource = env.readCsvFile(filePath)
                .fieldDelimiter(",")
                .ignoreFirstLine()
                .pojoType(CsvData.class, "x", "y");

        csvDataDataSource.print();
    }
}