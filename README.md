# TxnSails
The code base of TxnSails: Achieving Serializable **Transaction** Scheduling with **S**elf-**A**daptive **I**solation **L**evel **S**election.

The technique report is available in `technique\_report.pdf` within this repository or at [arXiv:2502.00991](https://arxiv.org/abs/2502.00991). 

## Brief introduction 
TxnSails works in the middle tier between database and application. It meets three requirements:  
1. It requires minimal modifications to client applications and database kernels, ensuring low implementation overhead.
2. It must be efficient to handle dangerous structures under various lower isolation levels while ensuring SER.
3. It must adaptively select the optimal isolation level to maximize performance in response to dynamic workloads.

The architecture of TxnSails is illustrated in figure below.

<div align=center>
<img alt="Architecture" src="docs/images/architecture.jpg" width="75%"/>
</div>

## Code description
### Code Navigation
Key modules and corresponding source code: 
1. `Analyzer` - src/.../worker/OfflineWorker, src/.../analysis/*
2. `Executor` - src/.../worker/OnlineWorker, src/.../execution/validation/*
3. `Adapter` - src/.../worker/{Adapter, Flusher}, src/.../execution/sample/\*, isolation\_adapter/\* (Python)
    - `isolation\_adapter/graph\_construct`: graph construction
    - `isolation\_adapter/graph\_training`: graph embedding and classfication
    - `isolation\_adapter/services`: offline training service and online prediction service
    - `adapter.py`: connection
4. `Client` - txnSailsClient 

*Note: `...` represents the filepath `main/java/org/dbiir/txnsails`.*

### Implementation
**Analyzer**: We first implemented a *StaticDependencyGraph* class that takes the transaction templates as input and builds a static dependency graph. Then, the graph is fed into the *ChordAbsentCycleFinder* class to detect cycles with characteristics defined in theorem 2.1. At last, it identifies the transaction templates involving static vulnerable dependencies and stores the results in MetaWorker instance.

**Executor**: It invokes *SQLRewrite()* function to rewrite queries, selecting the version of the record if its template is involved in static vulnerable dependencies. It then sends the rewritten query to the database and records the *vid* column. Additionally, we implement a crucial data structure, *ValidationMetaTable*, which is initialized before any transactions are received to perform middle-tier validation in single- or cross-isolation scenarios. It is organized as a hash table, with each bucket representing a list of *ValidationMeta*, including *validation lock*, *latest version*, and *lease* information. A dedicated thread is responsible for garbage collection of expired meta entries by comparing the *lease* and real-time system clock. Furthermore, we implement a *WAIT-DIE* strategy within*ValidationMetaTable* to prevent deadlocks. 

**Adapter**: We first implemented a *TransactionCollector* class that collects the read and write sets for transactions adhering to Monte Carlo sampling. Then, we design a *Flusher* thread to flush the runtime dependency graph. Finally, *Adapter* is implemented with the help of the *torch\_geometric* library. It inputs the runtime dependency group and outputs the optimal isolation level. To ensure cross-platform compatibility and efficiency, the Python and Java components communicate via *sockets*. 



## Client Libs
### Interface description
We provide four apis for clients: 

- `register() -> (status, serverSideIdx)`:
- `analyse() -> (status)`:
- `execute() -> (status, results/errorMsg)`:
- `commit()/rollback() -> (status)`: 

Application developer should rebuild a portion of their code to utilize TxnSails' capabilities and TxnSails can automatically guarantee the serializable.
Note that we do not modify the application workload to achieve serializable, for example, we do not either promote reads to writes or introduce outside lock manager.
We would continue to improve above apis and TxnSails to support serializable transactions for more heterogeneous database systems, 
thereby further reducing application development costs.


### How to use
1. Modify application code according to above interfaces. 
2. Register the transaction templates into TxnSails.

**Online workflow**:
1. Execute transactions by invoking `execute()` and `commit()/rollback()`.

**Offline workflow**:
1. generate random workload configurations
2. run each workload under different isolation
    - sample the runtime dependency graph
3. label the runtime dependency graph with the optimal isolation level according to the performance. 

**The isolation level selection and transition is transparent to clients and applications.**

## Evaluation
### Environment and Configuration
We conducted our experiments on two in-cluster servers, each equipped with an Intel(R) Xeon(R) Platinum 8361HC CPU @ 2.60GHz processor, which includes 24 physical cores, 64 GB of DRAM, and a 500 GB SSD. 
The operating system was CentOS Linux release 7.9. 

We utilize BenchBase as our benchmark simulator, deploying it on a single server. We modify it to interface with TxnSails. By default, the experiments are conducted using 128 client terminals. The sql statement is listed in `BENCHMARK_SQL_Statement.pdf`. 

We deployed PostgreSQL 15.2 as the database engine. For our database configuration, we allocated a buffer pool size of 24GB, limited the maximum number of connections to 2000, and established a lock wait timeout of 100 ms. To eliminate network-related variables from affecting the results, both TxnSails and PostgreSQL were deployed on another server. 

### How to Build
TxnSails requires JDK 21 and Maven 3.9+ for compilation. To run the build scripts, you need to ensure that Python 3.9+ is installed. Meanwhile, the packages used in Python are listed as following.
| Package | Version |
| ------  | ------- |
|torch    |2.1.2+cu121  |
| scipy | 1.12.0|
|torch-geometric | 2.6.1 |
| scikit-learn | 1.6.1 |

You should first build the TxnSails client:

```shell
cd <path_to_your_client> && ./build.py # in your client machine
```

You can run the following command to build TxnSails server:

```shell
mvn clean package -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true -Drat.skip=true -Djacoco.skip=true -DskipITs -DskipTests # in your server machine
```

This command will compile the project and the fat jar can be found in `target` folder. 

### How to Run
We provide python scripts located in the `scripts/` folder to generate the corresponding `.xml` configuration files in both server and client. Before running the tests, you should modify the information in the python script to ensure the generation of configuration files that meet the requirements, including the JDBC connection URL to connect to the database, and the database username and password. Your should generate configuration in both server and client. 

For example, you can run the following command generate your ycsb configuration files:

``` shell
python3 gen_ycsb_config.py
```

After you have completed the compilation and generated necessary configuration files, you can run the benchmark tests using the run_test.py script. TxnSails does not include the data loading part. That means you should load data into the database by yourselves before running the tests. The schemas for all benchmarks (SmallBank, TPC-C, YCSB) are located in /config.

You can run the following command to get help:

``` shell
python3 runTxnSailsServer.py -h

# If you need start adapter , run following shell
python3 adapter.py -w <your workload>
```

The following options are provided:

```shell
  -h, --help            show this help message and exit
  -f {scalability,hotspot-128,skew-128,wc_ratio-256,bal_ratio-128,wc_ratio-128,random-128,no_ratio-128,pa_ratio-128,wr_ratio-128} [{scalability,hotspot-128,skew-128,wc_ratio-256,bal_ratio-128,wc_ratio-
128,random-128,no_ratio-128,pa_ratio-128,wr_ratio-128} ...], --function {scalability,hotspot-128,skew-128,wc_ratio-256,bal_ratio-128,wc_ratio-128,random-128,no_ratio-128,pa_ratio-128,wr_ratio-128} [{scalability,hotspot-128,skew-128,wc_ratio-256,bal_ratio-128,wc_ratio-128,random-128,no_ratio-128,pa_ratio-128,wr_ratio-128} ...]
                        specify the function
  -w {ycsb,tpcc,smallbank}, --workload {ycsb,tpcc,smallbank}
                        specify the workload
  -e {postgresql}, --engine {postgresql}
                        specify the workload
  -n CNT, --cnt CNT     count of execution
  -p PHASE, --phase PHASE
                        online predict or offline training
```

### Running example
You can run the command to execute the hotspot-128 test of the SmallBank benchmark in PostgreSQL:
```shell
python3 runTxnSailsServer.py -w smallbank -f hotspot-256 -e postgresql -p online
```

Note: 
1. You should replace the `prefix_cmd_local`, `prefix_cmd_remote_java`, `remote_client_dir`, `config_prefix`, and `remote_machine_ip` with your own configuration. 
2. Generate the configuration in both client and server.
