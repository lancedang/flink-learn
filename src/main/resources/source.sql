CREATE TABLE view_student (
    id INT,
    name VARCHAR,
    password VARCHAR,
    age INT,
    flag VARCHAR
) WITH (
    'connector.type' = 'jdbc', -- 使用 jdbc connector
    'connector.url' = 'jdbc:mysql://localhost:3306/flink_demo', -- jdbc url
    'connector.table' = 'student', -- 表名
    'connector.username' = 'root', -- 用户名
    'connector.password' = 'abc123456', -- 密码
    'connector.write.flush.max-rows' = '1' -- 默认5000条，为了演示改为1条
)