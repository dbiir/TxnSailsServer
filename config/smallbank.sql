DROP TABLE IF EXISTS CHECKING;
DROP TABLE IF EXISTS SAVINGS;
DROP TABLE IF EXISTS ACCOUNTS;

CREATE TABLE ACCOUNTS (
                          custid      BIGINT      NOT NULL,
                          name        VARCHAR(64) NOT NULL,
                          CONSTRAINT pk_accounts PRIMARY KEY (custid)
);
CREATE INDEX IDX_ACCOUNTS_NAME ON ACCOUNTS (name);

CREATE TABLE SAVINGS (
                         custid      BIGINT      NOT NULL,
                         bal         FLOAT       NOT NULL,
                         vid         BIGINT      NOT NULL,
                         CONSTRAINT pk_savings PRIMARY KEY (custid),
                         FOREIGN KEY (custid) REFERENCES ACCOUNTS (custid)
);

CREATE TABLE CHECKING (
                          custid      BIGINT      NOT NULL,
                          bal         FLOAT       NOT NULL,
                          vid         BIGINT      NOT NULL,
                          CONSTRAINT pk_checking PRIMARY KEY (custid),
                          FOREIGN KEY (custid) REFERENCES ACCOUNTS (custid)
);
