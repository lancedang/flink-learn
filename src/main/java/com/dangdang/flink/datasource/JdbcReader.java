package com.dangdang.flink.datasource;

import com.dangdang.flink.constant.PropertiesConstants;
import com.dangdang.flink.model.Student;
import com.dangdang.flink.util.ExecutionEnvUtil;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

//import org.apache.flink.runtime.operators.Driver;


public class JdbcReader extends RichSourceFunction<List<Student>> {
    private static final Logger logger = LoggerFactory.getLogger(JdbcReader.class);

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;

    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        DriverManager.registerDriver(new Driver());
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        String jdbcUrl = parameterTool.get(PropertiesConstants.MYSQL_JDBC_URL);
        connection = DriverManager.getConnection(jdbcUrl, "root", "abc123456");//获取连接
        ps = connection.prepareStatement("select id, name, password, age, flag from student where flag=false");
    }

    //执行查询并获取结果
    @Override
    public void run(SourceContext<List<Student>> ctx) throws Exception {

        List<Student> students = new ArrayList<>();

        try {
            while (isRunning) {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {

                    Student student = new Student();
                    student.setId(resultSet.getInt(1));
                    student.setName(resultSet.getString(2));
                    student.setPassword(resultSet.getString(3));
                    student.setAge(resultSet.getInt(4));

                    String flag = resultSet.getString(5);
                    student.setFlag(flag);

                    if (Boolean.parseBoolean(flag)) {
                        students.add(student);
                    }
                }
                System.out.println("students>>>>>>" + students);
                ctx.collect(students);//发送结果
                students.clear();
                Thread.sleep(1000 * 10);
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }

    //关闭数据库连接
    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
        isRunning = false;
    }
}
