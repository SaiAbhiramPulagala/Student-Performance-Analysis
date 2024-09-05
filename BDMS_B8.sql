CREATE DATABASE Student;
USE Student;
CREATE TABLE student_info (
    student_id INT PRIMARY KEY AUTO_INCREMENT,
    school VARCHAR(2),
    sex VARCHAR(1),
    age INT,
    address VARCHAR(1),
    famsize VARCHAR(3),
    Pstatus VARCHAR(1),
    Medu INT,
    Fedu INT,
    Mjob VARCHAR(15),
    Fjob VARCHAR(15),
    reason VARCHAR(10),
    guardian VARCHAR(6),
    traveltime INT,
    studytime INT,
    failures INT,
    schoolsup VARCHAR(3),
    famsup VARCHAR(3),
    paid VARCHAR(3),
    activities VARCHAR(3),
    nursery VARCHAR(3),
    higher VARCHAR(3),
    internet VARCHAR(3),
    romantic VARCHAR(3),
    famrel INT,
    freetime INT,
    goout INT,
    Dalc INT,
    Walc INT,
    health INT,
    absences INT
);
CREATE TABLE grades (
    student_id INT PRIMARY KEY,
    G1 INT,
    G2 INT,
    G3 INT,
    FOREIGN KEY (student_id) REFERENCES student_info(student_id)
);
SHOW VARIABLES LIKE 'secure_file_priv';

CREATE TEMPORARY TABLE temp_table (
    student_id INT PRIMARY KEY , school VARCHAR(2),sex VARCHAR(1),age INT,
    address VARCHAR(1),
    famsize VARCHAR(3),
    Pstatus VARCHAR(1),
    Medu INT,
    Fedu INT,
    Mjob VARCHAR(15),
    Fjob VARCHAR(15),
    reason VARCHAR(10),
    guardian VARCHAR(6),
    traveltime INT,
    studytime INT,
    failures INT,
    schoolsup VARCHAR(3),
    famsup VARCHAR(3),
    paid VARCHAR(3),
    activities VARCHAR(3),
    nursery VARCHAR(3),
    higher VARCHAR(3),
    internet VARCHAR(3),
    romantic VARCHAR(3),
    famrel INT,
    freetime INT,
    goout INT,
    Dalc INT,
    Walc INT,
    health INT,
    absences INT,
    G1 INT,
    G2 INT,
    G3 INT
);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\student_data.csv'
INTO TABLE temp_table
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
INSERT INTO student_info (school, sex, age, address, famsize, Pstatus, Medu, Fedu, Mjob, Fjob, reason, guardian, traveltime, studytime, failures, schoolsup, famsup, paid, activities, nursery, higher, internet, romantic, famrel, freetime, goout, Dalc, Walc, health, absences)
SELECT school, sex, age, address, famsize, Pstatus, Medu, Fedu, Mjob, Fjob, reason, guardian, traveltime, studytime, failures, schoolsup, famsup, paid, activities, nursery, higher, internet, romantic, famrel, freetime, goout, Dalc, Walc, health, absences
FROM temp_table;

INSERT INTO grades (student_id, G1, G2, G3)
SELECT student_id, G1, G2, G3
FROM temp_table;

SELECT DISTINCT student_id
FROM temp_table
WHERE student_id NOT IN (SELECT student_id FROM student_info);

INSERT INTO student_info (student_id,school,sex,age,address,famsize,Pstatus,Medu,Fedu,Mjob,Fjob,reason,guardian,traveltime,studytime,failures,schoolsup,famsup,paid,activities,nursery,higher,internet,romantic,famrel,freetime,goout,Dalc,Walc,health,absences)
VALUES ('1','GP','F','18','U','GT3','A','4','4','at_home','teacher','course','mother','2','2','0','yes','no','no','no','yes','yes','no','no','4','3','4','1','1','3','6');