INSERT INTO student_dest
SELECT
    id,
    name,
    password,
    age
FROM temp_table
