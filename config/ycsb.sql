DROP TABLE IF EXISTS USERTABLE;
DROP TABLE IF EXISTS ycsb_conflict;
CREATE TABLE USERTABLE (
                           YCSB_KEY INT PRIMARY KEY,
                           VID BIGINT,
                           FIELD1 VARCHAR(100),
                           FIELD2 VARCHAR(100),
                           FIELD3 VARCHAR(100),
                           FIELD4 VARCHAR(100),
                           FIELD5 VARCHAR(100),
                           FIELD6 VARCHAR(100),
                           FIELD7 VARCHAR(100),
                           FIELD8 VARCHAR(100),
                           FIELD9 VARCHAR(100),
                           FIELD10 VARCHAR(100)
);
