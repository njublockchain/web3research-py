from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport



class Kylink:
    def __init__(self) -> None:
        self._transport = RequestsHTTPTransport(url="https://kpi.kylink.xyz")
        self._client = Client(transport=self._transport, fetch_schema_from_transport=True)


    def blocks(self, filter={}, limit=10, offset=0):
        query = gql(
            """
        query getBlocks($filter: BlockFilter, $limit: Int, $offset: Int) {
            blocks(filter: $filter, limit: $limit, offset: $offset) {
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

        params = {"filter": filter, "limit": limit, "offset": offset}
        return self._client.execute(query, variable_values=params)

    def transactions(self, filter={}, limit=10, offset=0):
        query = gql(
            """
        query getTransactions($filter: EventFilter, $limit: Int, $offset: Int) {
            events(filter: $filter, limit: $limit, offset: $offset) {
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

        params = {"filter": filter, "limit": limit, "offset": offset}
        return self._client.execute(query, variable_values=params)

    def events(self, filter={}, limit=10, offset=0):
        query = gql(
            """
        query getEvents($filter: EventFilter, $limit: Int, $offset: Int) {
            events(filter: $filter, limit: $limit, offset: $offset) {
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

        params = {"filter": filter, "limit": limit, "offset": offset}
        return self._client.execute(query, variable_values=params)

    def traces(self, filter={}, limit=10, offset=0):
        query = gql(
            """
        query getTraces($filter: TraceFilter, $limit: Int, $offset: Int) {
            traces(filter: $filter, limit: $limit, offset: $offset) {
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

        params = {"filter": filter, "limit": limit, "offset": offset}
        return self._client.execute(query, variable_values=params)

    def withdraws(self, filter={}, limit=10, offset=0):
        query = gql(
            """
        query Withdraws($filter: WithdrawFilter, $limit: Int, $offset: Int) {
            withdraws(filter: $filter, limit: $limit, offset: $offset) {
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

        params = {"filter": filter, "limit": limit, "offset": offset}
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
