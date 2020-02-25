CREATE TABLE student_dest (
                              id INT,
                              name VARCHAR,
                              password VARCHAR,
                              age INT
) WITH (
    'connector.type' = 'jdbc', -- 使用 jdbc connector
    'connector.url' = 'jdbc:mysql://localhost:3306/flink_demo', -- jdbc url
    'connector.table' = 'student_2', -- 表名
    'connector.username' = 'root', -- 用户名
    'connector.password' = 'abc123456', -- 密码
    'connector.write.flush.max-rows' = '1' -- 默认5000条，为了演示改为1条
)

