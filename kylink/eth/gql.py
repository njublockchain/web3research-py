from dataclasses import asdict
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

from .typehints import (
    BlockFilter,
    BlockSorter,
    EventFilter,
    EventSorter,
    TraceFilter,
    TraceSorter,
    TransactionFilter,
    TransactionSorter,
    WithdrawFilter,
    WithdrawSorter,
)


class GraphQLProvider:
    def __init__(self, api) -> None:
        self._transport = RequestsHTTPTransport(url="https://eth-graph.kylink.xyz")
        self._client = Client(
            transport=self._transport, fetch_schema_from_transport=True
        )
        self._api = api

    def blocks(self, filter=BlockFilter, sorter=BlockSorter, limit=10, offset=0):
        query = gql(
            """
        query Blocks($filter: BlockFilter, $sorter: BlockSorter, $pagination: Pagination) {
            blocks(filter: $filter, sorter: $sorter, pagination: $pagination) {
                hash
                number
                parentHash
                uncles
                sha3Uncles
                totalDifficulty
                miner
                difficulty
                nonce
                mixHash
                baseFeePerGas
                gasLimit
                gasUsed
                stateRoot
                transactionsRoot
                receiptsRoot
                logsBloom
                withdrawlsRoot
                extraData
                timestamp
                size
            }
        }
        """
        )

        params = {
            "filter": filter,
            "sorter": sorter,
            "pagination": {"offset": offset, "limit": limit},
        }

        return self._client.execute(query, variable_values=params)["blocks"]

    def transactions(
        self, filter=TransactionFilter, sorter=TransactionSorter, limit=10, offset=0
    ):
        setattr(filter, "from_", "from")
        setattr(sorter, "from_", "from")

        query = gql(
            """
        query getTransactions($filter: EventFilter, $sorter: EventSorter, $pagination: Pagination) {
            transactions(filter: $filter, sorter: $sorter, pagination: $pagination) {
                hash
                blockHash
                blockNumber
                blockTimestamp
                transactionIndex
                chainId
                type
                from
                to
                value
                nonce
                input
                gas
                gasPrice
                maxFeePerGas
                maxPriorityFeePerGas
                r
                s
                v
                accessList
                contractAddress
                cumulativeGasUsed
                effectiveGasPrice
                gasUsed
                logsBloom
                root
                status
            }
        }
        """
        )

        params = {
            "filter": asdict(filter),
            "sorter": asdict(sorter),
            "pagination": {"offset": offset, "limit": limit},
        }

        return self._client.execute(query, variable_values=params)["transactions"]

    def events(self, filter=EventFilter, sorter=EventSorter, limit=10, offset=0):
        query = gql(
            """
        query getEvents($filter: EventFilter, $sorter: EventSorter, $pagination: Pagination) {
            events(filter: $filter, sorter: $sorter, pagination: $pagination) {
                address
                blockHash
                blockNumber
                blockTimestamp
                transactionHash
                transactionIndex
                logIndex
                removed
                topics
                data
            }
        }
        """
        )

        params = {
            "filter": filter,
            "sorter": sorter,
            "pagination": {"offset": offset, "limit": limit},
        }

        return self._client.execute(query, variable_values=params)["evemts"]

    def traces(self, filter=TraceFilter, sorter=TraceSorter, limit=10, offset=0):
        query = gql(
            """
        query getTraces($filter: TraceFilter, $limit: Int, $offset: Int) {
            traces(filter: $filter, sorter: $sorter, pagination: $pagination) {
                blockPos
                blockNumber
                blockTimestamp
                blockHash
                transactionHash
                traceAddress
                subtraces
                transactionPosition
                error
                actionType
                actionCallFrom
                actionCallTo
                actionCallValue
                actionCallInput
                actionCallGas
                actionCallType
                actionCreateFrom
                actionCreateValue
                actionCreateInit
                actionCreateGas
                actionSuicideAddress
                actionSuicideRefundAddress
                actionSuicideBalance
                actionRewardAuthor
                actionRewardValue
                actionRewardType
                resultType
                resultCallGasUsed
                resultCallOutput
                resultCreateGasUsed
                resultCreateCode
                resultCreateAddress
            }
        }
        """
        )

        params = {
            "filter": asdict(filter),
            "sorter": asdict(sorter),
            "pagination": {"offset": offset, "limit": limit},
        }

        return self._client.execute(query, variable_values=params)

    def withdraws(self, filter=WithdrawFilter, sorter=WithdrawSorter, limit=10, offset=0):
        query = gql(
            """
        query Withdraws($filter: WithdrawFilter, $sorter: WithdrawSorter, $pagination: Pagination) {
            withdraws(filter: $filter, sorter: $sorter, pagination: $pagination) {
                blockHash
                blockNumber
                blockTimestamp
                index
                validatorIndex
                address
                amount
            }
        }
        """
        )

        params = {
            "filter": filter,
            "sorter": sorter,
            "pagination": {"offset": offset, "limit": limit},
        }

        return self._client.execute(query, variable_values=params)["withdraws"]

    def accounts(self, addresses=[]):
        query = gql(
            """
        query getAccount($addresses: [HexAddress!]!) {
            accounts(addresses: $addresses) {
                address
                ens
                balance
                code
                transactionCount
            }
        }
        """
        )
        params = {"addresses": addresses}

        return self._client.execute(query, variable_values=params)["accounts"]
