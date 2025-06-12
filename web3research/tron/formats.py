TRON_ACCOUNT_CREATE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "accountAddress": "str",  # String - hex address  
    "type": "int",
}
TRON_ACCOUNT_PERMISSION_UPDATE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "ownerPermissionType": "int",  # Nullable(Int32)
    "ownerPermissionId": "int",  # Nullable(Int32)
    "ownerPermissionName": "str",  # Nullable(String)
    "ownerThreshold": "int",  # Nullable(Int64)
    "ownerParentId": "int",  # Nullable(Int32)
    "ownerKeys": "dict[str, int]",  # Map(String, Int64)
    "ownerOperations": "str",  # String - hex data
    "witnessPermissionType": "int",  # Nullable(Int32)
    "witnessPermissionId": "int",  # Nullable(Int32)
    "witnessPermissionName": "str",  # Nullable(String)
    "witnessThreshold": "int",  # Nullable(Int64)
    "witnessParentId": "int",  # Nullable(Int32)
    "witnessKeys": "dict[str, int]",  # Map(String, Int64)
    "witnessOperations": "str",  # String - hex data
    "activesPermissionType": "list[int]",  # actives.permissionType
    "activesPermissionId": "list[int]",  # actives.permissionId
    "activesPermissionName": "list[str]",  # actives.permissionName
    "activesThreshold": "list[int]",  # actives.threshold
    "activesParentId": "list[int]",  # actives.parentId
    "activesKeys": "list[dict[str, int]]",  # actives.keys
    "activesOperations": "list[str]",  # actives.operations
}
TRON_ACCOUNT_UPDATE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64) 
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "accountName": "str",  # String - hex data
}
TRON_ASSET_ISSUE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "id": "str",
    "ownerAddress": "str",  # String - hex address
    "name": "str",  # String - hex data
    "abbr": "str",  # String - hex data
    "totalSupply": "int",
    "trxNum": "int",
    "precision": "int",
    "num": "int",
    "startTime": "int",
    "endTime": "int",
    "order": "int",
    "voteScore": "int",
    "description": "str",  # String - hex data
    "url": "str",  # String - hex data
    "freeAssetNetLimit": "int",
    "publicFreeAssetNetLimit": "int",
    "publicFreeAssetNetUsage": "int",
    "publicLatestFreeNetTime": "int",
}
TRON_BLOCK_COLUMN_FORMATS = {
    "hash": "str",  # FixedString(64) - hex string without 0x prefix
    "timestamp": "int",
    "parentHash": "str",  # FixedString(64) - hex string without 0x prefix
    "number": "int",
    "witnessId": "int",
    "witnessAddress": "str",  # String - hex address without 0x prefix
    "version": "int",
    "transactionCount": "int",
}
TRON_CANCEL_ALL_UNFREEZE_V2_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
}
TRON_CLEAR_ABI_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "contractAddress": "str",  # String - hex address
}
TRON_CREATE_SMART_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64) 
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "originAddress": "str",  # Nullable(String) - hex address
    "contractAddress": "str",  # Nullable(String) - hex address
    "abi": "str",  # Nullable(String) - JSON string
    "bytecode": "str",  # Nullable(String) - hex data
    "callValue": "int",  # Nullable(Int64)
    "consumeUserResourcePercent": "int",  # Nullable(Int64)
    "name": "str",  # Nullable(String)
    "originEnergyLimit": "int",  # Nullable(Int64)
    "codeHash": "str",  # Nullable(FixedString(64)) - hex string
    "trxHash": "str",  # Nullable(FixedString(64)) - hex string
    "version": "int",  # Nullable(Int32)
    "callTokenValue": "int",
    "tokenId": "int",
}
TRON_DELEGATE_RESOURCE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "resource": "int",
    "balance": "int",
    "receiverAddress": "str",  # String - hex address
    "lock": "bool",
    "lockPeriod": "int",
}
TRON_EVENT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64) - hex string without 0x prefix
    "logIndex": "int",
    "address": "str",  # String - hex address without 0x prefix
    "topic0": "str",  # Nullable(FixedString(64)) - hex string without 0x prefix
    "topic1": "str",  # Nullable(FixedString(64)) - hex string without 0x prefix
    "topic2": "str",  # Nullable(FixedString(64)) - hex string without 0x prefix
    "topic3": "str",  # Nullable(FixedString(64)) - hex string without 0x prefix
    "data": "str",  # String - hex data
}
TRON_EXCHANGE_CREATE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "firstTokenId": "str",  # String - hex data
    "firstTokenBalance": "int",
    "secondTokenId": "str",  # String - hex data
    "secondTokenBalance": "int",
}
TRON_EXCHANGE_INJECT_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "exchangeId": "int",
    "tokenId": "str",  # String - hex data
    "quant": "int",
}
TRON_EXCHANGE_TRANSACTION_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "exchangeId": "int", 
    "tokenId": "str",  # String - hex data
    "quant": "int",
    "expected": "int",
}
TRON_EXCHANGE_WITHDRAW_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int", 
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "exchangeId": "int",
    "tokenId": "str",  # String - hex data
    "quant": "int",
}
TRON_FREEZE_BALANCE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int", 
    "ownerAddress": "str",  # String - hex address
    "frozenBalance": "int",
    "frozenDuration": "int",
    "resource": "int",
    "receiverAddress": "str",  # String - hex address
}
TRON_FREEZE_BALANCE_V2_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "frozenBalance": "int",
    "resource": "int",
}
TRON_INTERNAL_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64) - hex string without 0x prefix
    "internalIndex": "int",
    "callerAddress": "str",  # String - hex address without 0x prefix
    "transferToAddress": "str",  # String - hex address without 0x prefix
    "callValueInfosTokenId": "list[str]",  # callValueInfos.tokenId
    "callValueInfosCallValue": "list[int]",  # callValueInfos.callValue
    "note": "str",  # String - hex data
    "rejected": "bool",
    "extra": "str",  # String - hex data
}
TRON_MARKET_CANCEL_ORDER_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "orderId": "str",  # String - hex data
}
TRON_MARKET_SELL_ASSET_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "sellTokenId": "str",  # String - hex data
    "sellTokenQuantity": "int",
    "buyTokenId": "str",  # String - hex data
    "buyTokenQuantity": "int",
}
TRON_PARTICIPATE_ASSET_ISSUE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "toAddress": "str",  # String - hex address
    "assetName": "str",  # String - hex data
    "amount": "int",
}
TRON_PROPOSAL_APPROVE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int", 
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "proposalId": "int",
    "isAddApproval": "bool",
}
TRON_PROPOSAL_CREATE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "parameters": "dict[int, int]",  # Map(Int64, Int64)
}
TRON_PROPOSAL_DELETE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "proposalId": "int",
}
TRON_SET_ACCOUNT_ID_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "accountId": "str",  # String - hex data
    "ownerAddress": "str",  # String - hex address
}
TRON_SHIELDED_TRANSFER_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "transparentFromAddress": "str",  # String - hex address
    "fromAmount": "int",
    "spendDescriptionValueCommitment": "list[str]",  # spendDescription.valueCommitment
    "spendDescriptionAnchor": "list[str]",  # spendDescription.anchor
    "spendDescriptionNullifier": "list[str]",  # spendDescription.nullifier
    "spendDescriptionRk": "list[str]",  # spendDescription.rk
    "spendDescriptionZkproof": "list[str]",  # spendDescription.zkproof
    "spendDescriptionAuthoritySignature": "list[str]",  # spendDescription.authoritySignature
    "receiveDescriptionValueCommitment": "list[str]",  # receiveDescription.valueCommitment
    "receiveDescriptionNoteCommitment": "list[str]",  # receiveDescription.noteCommitment
    "receiveDescriptionEpk": "list[str]",  # receiveDescription.epk
    "receiveDescriptionCEnc": "list[str]",  # receiveDescription.cEnc
    "receiveDescriptionCOut": "list[str]",  # receiveDescription.cOut
    "receiveDescriptionZkproof": "list[str]",  # receiveDescription.zkproof
    "bindingSignature": "str",  # FixedString(64) - hex string
    "transparentToAddress": "str",  # String - hex address
    "toAmount": "int",
}
TRON_TRANSACTION_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "index": "int",
    "hash": "str",  # FixedString(64) - hex string without 0x prefix
    "expiration": "int",
    "authorityAccountNames": "list[str]",  # Array(LowCardinality(String))
    "authorityAccountAddresses": "list[str]",  # Array(String)
    "authorityPermissionNames": "list[str]",  # Array(LowCardinality(String))
    "data": "str",  # String - hex data
    "contractType": "str",  # LowCardinality(String)
    "contractProvider": "str",  # Nullable(String)
    "contractName": "str",  # Nullable(String)
    "contractPermissionId": "int",  # Nullable(Int32)
    "scripts": "str",  # String - hex data
    "timestamp": "int",
    "feeLimit": "int",
    "constantResult": "str",  # String - hex data
    "fee": "int",
    "contractResult": "str",  # Nullable(String) - hex data
    "contractAddress": "str",  # Nullable(String) - hex address
    "energyUsage": "int",
    "energyFee": "int",
    "originEnergyUsage": "int",
    "energyUsageTotal": "int",
    "netUsage": "int",
    "netFee": "int",
    "receiptResult": "str",  # LowCardinality(String)
    "result": "str",  # LowCardinality(String)
    "resMessage": "str",  # String - hex data
    "assetIssueId": "str",
    "withdrawAmount": "int",
    "unfreezeAmount": "int",
    "exchangeReceivedAmount": "int",
    "exchangeInjectAnotherAmount": "int",
    "exchangeWithdrawAnotherAmount": "int",
    "exchangeId": "int",
    "shieldedTransactionFee": "int",
    "orderId": "str",  # FixedString(64) - hex string without 0x prefix
    "orderDetailMakerOrderId": "list[str]",  # orderDetails.makerOrderId
    "orderDetailTakerOrderId": "list[str]",  # orderDetails.takerOrderId  
    "orderDetailFillSellQuantity": "list[int]",  # orderDetails.fillSellQuantity
    "orderDetailFillBuyQuantity": "list[int]",  # orderDetails.fillBuyQuantity
    "packingFee": "int",
    "withdrawExpireAmount": "int",
    "cancelUnfreezeV2Amount": "dict[str, int]",  # Map(String, Int64)
}
TRON_TRANSFER_ASSET_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int", 
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "assetName": "str",  # String - hex data
    "ownerAddress": "str",  # String - hex address
    "toAddress": "str",  # String - hex address
    "amount": "int",
}
TRON_TRANSFER_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int", 
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "toAddress": "str",  # String - hex address
    "amount": "int",
}
TRON_TRIGGER_SMART_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int", 
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "contractAddress": "str",  # String - hex address
    "callValue": "int",
    "data": "str",  # String - hex data
    "callTokenValue": "int",
    "tokenId": "int",
}
TRON_UNDELEGATE_RESOURCE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "resource": "int",
    "balance": "int",
    "receiverAddress": "str",  # String - hex address
}
TRON_UNFREEZE_ASSET_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
}
TRON_UNFREEZE_BALANCE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "resource": "int",
    "receiverAddress": "str",  # String - hex address
}
TRON_UNFREEZE_BALANCE_V2_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "unfreezeBalance": "int",
    "resource": "int",
}
TRON_UPDATE_ASSET_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "description": "str",  # String - hex data
    "url": "str",  # String - hex data
    "newLimit": "int",
    "newPublicLimit": "int",
}
TRON_UPDATE_BROKERAGE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "brokerage": "int",
}
TRON_UPDATE_ENERGY_LIMIT_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "contractAddress": "str",  # String - hex address
    "originEnergyLimit": "int",
}
TRON_VOTE_ASSET_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int", 
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "voteAddress": "list[str]",  # Array(String) - hex addresses
    "support": "bool",
    "count": "int",
}
TRON_VOTE_WITNESS_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int", 
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int", 
    "ownerAddress": "str",  # String - hex address
    "votesVoteAddress": "list[str]",  # votes.voteAddress
    "votesVoteCount": "list[int]",  # votes.voteCount
    "support": "bool",
}
TRON_WITNESS_CREATE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "url": "str",  # String - hex data
}
TRON_WITNESS_UPDATE_CONTRACT_COLUMN_FORMATS = {
    "blockNumber": "int", 
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "str",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "str",  # String - hex address
    "updateUrl": "str",  # String - hex data
}