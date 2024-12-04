package org.dbiir.common;

public class SQLConstants {
    // SQL 操作类型常量
    public static final int OP_READ = 0; // 读取操作
    public static final int OP_WRITE = 1; // 写入操作

    // 负载常量
    public static final String TPCC = "TPCC";
    public static final String YCSB = "YCSB";
    public static final String SMALLBANK = "SMALLBANK";
    
    // 隔离级别常量
    public static final String ISOLATION_SERIALIZABLE = "SERIALIZABLE";
    public static final String ISOLATION_READ_COMMITTED = "READ COMMITTED";

    // 其他常用常量
    public static final String DEFAULT_TABLE = "default_table";
    public static final String RELATION = "relation";

    // 表名
    // ycsb
    public static final String USERTABLE = "USERTABLE";
    
    // tpcc
    public static final String NEW_ORDER = "NEW_ORDER";
    public static final String ORDER_LINE = "ORDER_LINE";
    public static final String OORDER = "OORDER";
    public static final String CUSTOMER = "CUSTOMER";
    public static final String DISTRICT = "DISTRICT";
    public static final String STOCK = "STOCK";
    public static final String ITEM = "ITEM";
    public static final String WAREHOUSE = "WAREHOUSE";

    // smallbank
    public static final String CHECKING = "CHECKING";
    public static final String SAVINGS = "SAVINGS";
    public static final String ACCOUNTS = "ACCOUNTS";

    // template
    public static final String OrderStatus = "ORDERSTATUS";
    public static final String Payment = "PAYMENT";
    public static final String Delivery = "DELIVERY";
    public static final String NewOrder = "NEWORDER";
    public static final String StockLevel = "STOCKLEVEL";

    public static final String DeleteRecord = "DELETERECORD";
    public static final String InsertRecord = "INSERTRECORD";
    public static final String ReadRecord = "READRECORD";
    public static final String ReadWriteRecord = "READWRITERECORD";
    public static final String ScanRecord = "SCANRECORD";
    public static final String UpdatRecord = "UPDATRECORD";
    public static final String ReadModifyWriteRecord = "READMODIFYWRITERECORD";

    public static final String DepositChecking = "DEPOSITCHECKING";
    public static final String WriteCheck = "WRITECHECK";
    public static final String TransactSavings = "TRANSACTSAVINGS";
    public static final String Amalgamate = "AMALGAMATE";
    public static final String Balance = "BALANCE";
    
    

}
