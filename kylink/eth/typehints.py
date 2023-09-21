from dataclasses import dataclass
from typing import List, Optional

# from pydantic import BaseModel
    
@dataclass
class Block:
    hash: str
    number: str 
    parentHash: str
    uncles: List[str]
    sha3Uncles: str
    totalDifficulty: str
    miner: str
    difficulty: str
    nonce: str
    mixHash: str
    baseFeePerGas: Optional[str]
    gasLimit: str
    gasUsed: str
    stateRoot: str
    transactionsRoot: str
    receiptsRoot: str
    logsBloom: str
    withdrawlsRoot: Optional[str]
    extraData: str
    timestamp: str
    size: str

class Event:
    address: str
    blockHash: str
    blockNumber: str
    blockTimestamp: str
    transactionHash: str
    transactionIndex: str
    logIndex: str  
    removed: bool
    topics: List[str]
    data: str

class Trace:
    blockPos: str
    blockNumber: str
    blockTimestamp: str
    blockHash: str
    transactionHash: Optional[str]
    traceAddress: List[str]
    subtraces: str
    transactionPosition: Optional[str]
    error: Optional[str]
    actionType: str
    actionCallFrom: Optional[str]
    actionCallTo: Optional[str]
    actionCallValue: Optional[str]
    actionCallInput: Optional[str]
    actionCallGas: Optional[str]
    actionCallType: str
    actionCreateFrom: Optional[str]
    actionCreateValue: Optional[str]
    actionCreateInit: Optional[str]
    actionCreateGas: Optional[str]
    actionSuicideAddress: Optional[str]
    actionSuicideRefundAddress: Optional[str]
    actionSuicideBalance: Optional[str]
    actionRewardAuthor: Optional[str]
    actionRewardValue: Optional[str]
    actionRewardType: str
    resultType: str
    resultCallGasUsed: Optional[str]
    resultCallOutput: Optional[str]
    resultCreateGasUsed: Optional[str]
    resultCreateCode: Optional[str]
    resultCreateAddress: Optional[str]

class Transaction:
    hash: str
    blockHash: str
    blockNumber: str
    blockTimestamp: str
    transactionIndex: str
    chainId: Optional[str]
    type: Optional[str]
    from_: str
    to: Optional[str]  
    value: str
    nonce: str
    input: str
    gas: str
    gasPrice: Optional[str]
    maxFeePerGas: Optional[str]
    maxPriorityFeePerGas: Optional[str]
    r: str
    s: str
    v: str
    accessList: Optional[str]
    contractAddress: Optional[str]
    cumulativeGasUsed: str
    effectiveGasPrice: Optional[str]
    gasUsed: str
    logsBloom: str
    root: Optional[str]
    status: Optional[str]

class Withdraw:
    blockHash: str
    blockNumber: str
    blockTimestamp: str  
    index: str
    validatorIndex: str
    address: str
    amount: str

class Account:
    address: str
    ens: Optional[str]
    balance: str
    code: Optional[str] 
    transactionCount: str

class BlockFilter:
    hash: Optional[str] = None
    number: Optional[str] = None
    parentHash: Optional[str] = None
    uncles: Optional[List[str]] = None
    sha3Uncles: Optional[str] = None
    totalDifficulty: Optional[str] = None
    miner: Optional[str] = None
    difficulty: Optional[str] = None
    nonce: Optional[str] = None
    mixHash: Optional[str] = None
    baseFeePerGas: Optional[str] = None
    gasLimit: Optional[str] = None
    gasUsed: Optional[str] = None
    stateRoot: Optional[str] = None
    transactionsRoot: Optional[str] = None
    receiptsRoot: Optional[str] = None
    logsBloom: Optional[str] = None
    withdrawlsRoot: Optional[str] = None
    extraData: Optional[str] = None
    timestamp: Optional[str] = None
    size: Optional[str] = None

class EventFilter:
    address: Optional[str] = None
    blockHash: Optional[str] = None
    blockNumber: Optional[str] = None
    blockTimestamp: Optional[str] = None
    transactionHash: Optional[str] = None
    transactionIndex: Optional[str] = None
    logIndex: Optional[str] = None
    removed: Optional[bool] = None
    topics: Optional[List[str]] = None
    data: Optional[str] = None

class TraceFilter:
    blockPos: Optional[str] = None
    blockNumber: Optional[str] = None
    blockTimestamp: Optional[str] = None
    blockHash: Optional[str] = None
    transactionHash: Optional[str] = None
    traceAddress: Optional[List[str]] = None
    subtraces: Optional[str] = None
    transactionPosition: Optional[str] = None
    error: Optional[str] = None
    actionType: Optional[str] = None
    actionCallFrom: Optional[str] = None
    actionCallTo: Optional[str] = None
    actionCallValue: Optional[str] = None
    actionCallInput: Optional[str] = None
    actionCallGas: Optional[str] = None
    actionCallType: Optional[str] = None
    actionCreateFrom: Optional[str] = None
    actionCreateValue: Optional[str] = None
    actionCreateInit: Optional[str] = None
    actionCreateGas: Optional[str] = None
    actionSuicideAddress: Optional[str] = None
    actionSuicideRefundAddress: Optional[str] = None
    actionSuicideBalance: Optional[str] = None
    actionRewardAuthor: Optional[str] = None
    actionRewardValue: Optional[str] = None
    actionRewardType: Optional[str] = None
    resultType: Optional[str] = None
    resultCallGasUsed: Optional[str] = None
    resultCallOutput: Optional[str] = None
    resultCreateGasUsed: Optional[str] = None
    resultCreateCode: Optional[str] = None
    resultCreateAddress: Optional[str] = None

class TransactionFilter:
    hash: Optional[str] = None
    blockHash: Optional[str] = None
    blockNumber: Optional[str] = None
    blockTimestamp: Optional[str] = None
    transactionIndex: Optional[str] = None
    chainId: Optional[str] = None
    type: Optional[str] = None
    from_: Optional[str] = None
    to: Optional[str] = None
    value: Optional[str] = None
    nonce: Optional[str] = None
    input: Optional[str] = None
    gas: Optional[str] = None
    gasPrice: Optional[str] = None
    maxFeePerGas: Optional[str] = None
    maxPriorityFeePerGas: Optional[str] = None
    r: Optional[str] = None
    s: Optional[str] = None
    v: Optional[str] = None
    accessList: Optional[str] = None
    contractAddress: Optional[str] = None
    cumulativeGasUsed: Optional[str] = None
    effectiveGasPrice: Optional[str] = None
    gasUsed: Optional[str] = None
    logsBloom: Optional[str] = None
    root: Optional[str] = None
    status: Optional[str] = None

class WithdrawFilter:
    blockHash: Optional[str] = None
    blockNumber: Optional[str] = None
    blockTimestamp: Optional[str] = None
    index: Optional[str] = None
    validatorIndex: Optional[str] = None
    address: Optional[str] = None
    amount: Optional[str] = None

class BlockSorter:
    hash: Optional[int] = None
    number: Optional[int] = None
    parentHash: Optional[int] = None
    uncles: Optional[int] = None
    sha3Uncles: Optional[int] = None
    totalDifficulty: Optional[int] = None
    miner: Optional[int] = None
    difficulty: Optional[int] = None
    nonce: Optional[int] = None
    mixHash: Optional[int] = None
    baseFeePerGas: Optional[int] = None
    gasLimit: Optional[int] = None
    gasUsed: Optional[int] = None
    stateRoot: Optional[int] = None
    transactionsRoot: Optional[int] = None
    receiptsRoot: Optional[int] = None
    logsBloom: Optional[int] = None
    withdrawlsRoot: Optional[int] = None
    extraData: Optional[int] = None
    timestamp: Optional[int] = None
    size: Optional[int] = None

class EventSorter:
    address: Optional[int] = None
    blockHash: Optional[int] = None
    blockNumber: Optional[int] = None
    blockTimestamp: Optional[int] = None
    transactionHash: Optional[int] = None
    transactionIndex: Optional[int] = None
    logIndex: Optional[int] = None
    removed: Optional[int] = None
    topics: Optional[int] = None
    data: Optional[int] = None

class TraceSorter:
    blockPos: Optional[int] = None
    blockNumber: Optional[int] = None
    blockTimestamp: Optional[int] = None
    blockHash: Optional[int] = None
    transactionHash: Optional[int] = None
    traceAddress: Optional[int] = None
    subtraces: Optional[int] = None
    transactionPosition: Optional[int] = None
    error: Optional[int] = None
    actionType: Optional[int] = None
    actionCallFrom: Optional[int] = None
    actionCallTo: Optional[int] = None
    actionCallValue: Optional[int] = None
    actionCallInput: Optional[int] = None
    actionCallGas: Optional[int] = None
    actionCallType: Optional[int] = None
    actionCreateFrom: Optional[int] = None
    actionCreateValue: Optional[int] = None
    actionCreateInit: Optional[int] = None
    actionCreateGas: Optional[int] = None
    actionSuicideAddress: Optional[int] = None
    actionSuicideRefundAddress: Optional[int] = None
    actionSuicideBalance: Optional[int] = None
    actionRewardAuthor: Optional[int] = None
    actionRewardValue: Optional[int] = None
    actionRewardType: Optional[int] = None
    resultType: Optional[int] = None
    resultCallGasUsed: Optional[int] = None
    resultCallOutput: Optional[int] = None
    resultCreateGasUsed: Optional[int] = None
    resultCreateCode: Optional[int] = None
    resultCreateAddress: Optional[int] = None

class TransactionSorter:
    hash: Optional[int] = None
    blockHash: Optional[int] = None
    blockNumber: Optional[int] = None
    blockTimestamp: Optional[int] = None
    transactionIndex: Optional[int] = None
    chainId: Optional[int] = None
    type: Optional[int] = None
    from_: Optional[int] = None
    to: Optional[int] = None
    value: Optional[int] = None
    nonce: Optional[int] = None
    input: Optional[int] = None
    gas: Optional[int] = None
    gasPrice: Optional[int] = None
    maxFeePerGas: Optional[int] = None
    maxPriorityFeePerGas: Optional[int] = None
    r: Optional[int] = None
    s: Optional[int] = None
    v: Optional[int] = None
    accessList: Optional[int] = None
    contractAddress: Optional[int] = None
    cumulativeGasUsed: Optional[int] = None
    effectiveGasPrice: Optional[int] = None
    gasUsed: Optional[int] = None
    logsBloom: Optional[int] = None
    root: Optional[int] = None
    status: Optional[int] = None

class WithdrawSorter:
    blockHash: Optional[int] = None
    blockNumber: Optional[int] = None
    blockTimestamp: Optional[int] = None
    index: Optional[int] = None
    validatorIndex: Optional[int] = None
    address: Optional[int] = None
    amount: Optional[int] = None

class Pagination:
    limit: Optional[int] = None
    offset: Optional[int] = None

class Query:
    query: str
    blocks: List[Block]
    events: List[Event] 
    traces: List[Trace]
    transactions: List[Transaction]
    withdraws: List[Withdraw]
    accounts: List[Account]