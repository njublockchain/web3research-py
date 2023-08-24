from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport


class Kylink:
    def __init__(self) -> None:
        self._transport = RequestsHTTPTransport(url="https://kpi.kylink.xyz")
        self._client = Client(
            transport=self._transport, fetch_schema_from_transport=True
        )

    def blocks(self, filter={}, sorter={}, limit=10, offset=0):
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
        return self._client.execute(query, variable_values=params)

    def transactions(self, filter={}, sorter={}, limit=10, offset=0):
        query = gql(
            """
        query getTransactions($filter: EventFilter, $sorter: EventSorter, $pagination: Pagination) {
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
        return self._client.execute(query, variable_values=params)

    def events(self, filter={}, sorter={}, limit=10, offset=0):
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
        return self._client.execute(query, variable_values=params)

    def traces(self, filter={}, sorter={}, limit=10, offset=0):
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
            "filter": filter,
            "sorter": sorter,
            "pagination": {"offset": offset, "limit": limit},
        }
        return self._client.execute(query, variable_values=params)

    def withdraws(self, filter={}, sorter={}, limit=10, offset=0):
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
        return self._client.execute(query, variable_values=params)

    def account(self, address=""):
        query = gql(
            """
        query getAccount($address: HexAddress) {
            account(address: $address) {
                address
                ens
                balance
                code
                transactionCount
            }
        }
        """
        )
        params = {"address": address}
        return self._client.execute(query, variable_values=params)
