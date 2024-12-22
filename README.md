# TxnSails
The code base of TxnSails: _Achieving Serializable **Transaction** Scheduling with **S**elf-**A**daptive **I**solation **L**evel **S**election.

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
3. `Adapter` - src/.../worker/{Adapter, Flusher}, src/.../execution/sample/\*, isolation\_adapter/\*

Note: `...` represents the filepath `main/java/org/dbiir/txnsails`.

## Client Libs
We provide four apis for clients: 

- `register() -> (status, serverSideIdx)`:
- `analyse() -> (status)`:
- `execute() -> (status, results/errorMsg)`:
- `commit()/rollback() -> (status)`: 

Application developer should rebuild a portion of their code to utilize TxnSails' capabilities. 
Note that we do not modify the application workload to achieve serializable, for example, we do not promote reads to writes.
We would continue to enhance above apis to offer JDBC-like functionality. 
This would enable applications to connect to TxnSails via JDBC and TxnSails automatically guarantee the serializable, 
thereby further reducing application development costs.

## Evaluation
### Environment and Configuration

### Instructions

### Running example
