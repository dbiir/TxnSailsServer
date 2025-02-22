DROP TABLE IF EXISTS order_line CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS district CASCADE;
DROP TABLE IF EXISTS stock CASCADE;
DROP TABLE IF EXISTS warehouse CASCADE;

CREATE TABLE warehouse (
    -- WarehouseID
                           w_id       int            NOT NULL,
    -- WarehouseYTD
                           w_ytd      decimal(12, 2) NOT NULL,
    -- WarehouseInfo
                           w_info VARCHAR(255) NULL DEFAULT NULL,
    -- vid
                           vid      int            NOT NULL,
    /*
    w_tax      decimal(4, 4)  NOT NULL,
    w_name     varchar(10)    NOT NULL,
    w_street_1 varchar(20)    NOT NULL,
    w_street_2 varchar(20)    NOT NULL,
    w_city     varchar(20)    NOT NULL,
    w_state    char(2)        NOT NULL,
    w_zip      char(9)        NOT NULL,
     */
                           PRIMARY KEY (w_id)
);

CREATE TABLE stock (
    -- Stock WarehouseID
                       s_w_id       int           NOT NULL,
    -- Stock ItemID
                       s_i_id       int           NOT NULL,
    -- Stock Quantity
                       s_quantity   int           NOT NULL,
    -- vid
                       vid        int           NOT NULL,
    /*
    s_ytd        decimal(8, 2) NOT NULL,
    s_order_cnt  int           NOT NULL,
    s_remote_cnt int           NOT NULL,
    s_data       varchar(50)   NOT NULL,
    s_dist_01    char(24)      NOT NULL,
    s_dist_02    char(24)      NOT NULL,
    s_dist_03    char(24)      NOT NULL,
    s_dist_04    char(24)      NOT NULL,
    s_dist_05    char(24)      NOT NULL,
    s_dist_06    char(24)      NOT NULL,
    s_dist_07    char(24)      NOT NULL,
    s_dist_08    char(24)      NOT NULL,
    s_dist_09    char(24)      NOT NULL,
    s_dist_10    char(24)      NOT NULL,
    */
                       FOREIGN KEY (s_w_id) REFERENCES warehouse (w_id) ON DELETE CASCADE,
                       PRIMARY KEY (s_w_id, s_i_id)
);

CREATE TABLE district (
    -- District WarehouseID
                          d_w_id      int            NOT NULL,
    -- District ID
                          d_id        int            NOT NULL,
    -- District YTD
                          d_ytd       decimal(12, 2) NOT NULL,
    -- DistrictID NextOrderID
                          d_next_o_id int            NOT NULL,
    -- District Info
                          d_info      VARCHAR(255)    NULL DEFAULT NULL,
    /*
    d_tax       decimal(4, 4)  NOT NULL,
    d_name      varchar(10)    NOT NULL,
    d_street_1  varchar(20)    NOT NULL,
    d_street_2  varchar(20)    NOT NULL,
    d_city      varchar(20)    NOT NULL,
    d_state     char(2)        NOT NULL,
    d_zip       char(9)        NOT NULL,
     */
                          FOREIGN KEY (d_w_id) REFERENCES warehouse (w_id) ON DELETE CASCADE,
                          PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE customer (
    -- warehouse
                          c_w_id         int            NOT NULL,
    -- district
                          c_d_id         int            NOT NULL,
    -- customer
                          c_id           int            NOT NULL,
    -- balance
                          c_balance      decimal(12, 2) NOT NULL,
    -- info
                          c_info         VARCHAR(255)     NULL DEFAULT NULL,
    -- vid
                          vid          int            NOT NULL,
    /*
    c_discount     decimal(4, 4)  NOT NULL,
    c_credit       char(2)        NOT NULL,
    c_last         varchar(16)    NOT NULL,
    c_first        varchar(16)    NOT NULL,
    c_credit_lim   decimal(12, 2) NOT NULL,
    c_ytd_payment  float          NOT NULL,
    c_payment_cnt  int            NOT NULL,
    c_delivery_cnt int            NOT NULL,
    c_street_1     varchar(20)    NOT NULL,
    c_street_2     varchar(20)    NOT NULL,
    c_city         varchar(20)    NOT NULL,
    c_state        char(2)        NOT NULL,
    c_zip          char(9)        NOT NULL,
    c_phone        char(16)       NOT NULL,
    c_since        timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    c_middle       char(2)        NOT NULL,
    c_data         varchar(500)   NOT NULL,
     */
                          FOREIGN KEY (c_w_id, c_d_id) REFERENCES district (d_w_id, d_id) ON DELETE CASCADE,
                          PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE TABLE orders (
                        o_w_id       int       NOT NULL,
                        o_d_id       int       NOT NULL,
                        o_id         int       NOT NULL,
                        o_c_id       int       NOT NULL,
                        o_status     VARCHAR(50)    NULL DEFAULT NULL,
                        vid        int       NOT NULL,
    /*
    o_carrier_id int                DEFAULT NULL,
    o_ol_cnt     int       NOT NULL,
    o_all_local  int       NOT NULL,
    o_entry_d    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
     */
                        PRIMARY KEY (o_w_id, o_d_id, o_id),
                        FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer (c_w_id, c_d_id, c_id) ON DELETE CASCADE,
                        UNIQUE (o_w_id, o_d_id, o_c_id, o_id)
);

CREATE TABLE order_line (
                            ol_w_id        int           NOT NULL,
                            ol_d_id        int           NOT NULL,
                            ol_o_id        int           NOT NULL,
                            ol_number      int           NOT NULL,
                            ol_i_id        int           NOT NULL,
                            ol_delivery_info  VARCHAR(255)    NULL DEFAULT NULL,
                            ol_quantity    int           NOT NULL,
                            vid         int           NOT NULL,
    /*
    ol_amount      decimal(6, 2) NOT NULL,
    ol_supply_w_id int           NOT NULL,
    ol_quantity    int           NOT NULL,
    ol_dist_info   char(24)      NOT NULL,
     */
                            FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES orders (o_w_id, o_d_id, o_id) ON DELETE CASCADE,
                            FOREIGN KEY (ol_w_id, ol_i_id) REFERENCES stock (s_w_id, s_i_id) ON DELETE CASCADE,
                            PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id);