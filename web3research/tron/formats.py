from typing import Dict

QUERY_FORMATS = {
    "FixedString*": "string",
}

TRON_ACCOUNT_CREATE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "accountAddress": "string",  # String - hex address  
    "type": "int",
}
TRON_ACCOUNT_PERMISSION_UPDATE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "ownerPermissionType": "int",  # Nullable(Int32)
    "ownerPermissionId": "int",  # Nullable(Int32)
    "ownerPermissionName": "string",  # Nullable(String)
    "ownerThreshold": "int",  # Nullable(Int64)
    "ownerParentId": "int",  # Nullable(Int32)
    "ownerKeys": "dict[str, int]",  # Map(String, Int64)
    "ownerOperations": "string",  # String - hex data
    "witnessPermissionType": "int",  # Nullable(Int32)
    "witnessPermissionId": "int",  # Nullable(Int32)
    "witnessPermissionName": "string",  # Nullable(String)
    "witnessThreshold": "int",  # Nullable(Int64)
    "witnessParentId": "int",  # Nullable(Int32)
    "witnessKeys": "dict[str, int]",  # Map(String, Int64)
    "witnessOperations": "string",  # String - hex data
    "activesPermissionType": "list[int]",  # actives.permissionType
    "activesPermissionId": "list[int]",  # actives.permissionId
    "activesPermissionName": "list[str]",  # actives.permissionName
    "activesThreshold": "list[int]",  # actives.threshold
    "activesParentId": "list[int]",  # actives.parentId
    "activesKeys": "list[dict[str, int]]",  # actives.keys
    "activesOperations": "list[str]",  # actives.operations
}
TRON_ACCOUNT_UPDATE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64) 
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "accountName": "string",  # String - hex data
}
TRON_ASSET_ISSUE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "id": "string",
    "ownerAddress": "string",  # String - hex address
    "name": "string",  # String - hex data
    "abbr": "string",  # String - hex data
    "totalSupply": "int",
    "trxNum": "int",
    "precision": "int",
    "num": "int",
    "startTime": "int",
    "endTime": "int",
    "order": "int",
    "voteScore": "int",
    "description": "string",  # String - hex data
    "url": "string",  # String - hex data
    "freeAssetNetLimit": "int",
    "publicFreeAssetNetLimit": "int",
    "publicFreeAssetNetUsage": "int",
    "publicLatestFreeNetTime": "int",
}
TRON_BLOCK_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "hash": "string",  # FixedString(64) - hex string without 0x prefix
    "timestamp": "int",
    "parentHash": "string",  # FixedString(64) - hex string without 0x prefix
    "number": "int",
    "witnessId": "int",
    "witnessAddress": "string",  # String - hex address without 0x prefix
    "version": "int",
    "transactionCount": "int",
}
TRON_CANCEL_ALL_UNFREEZE_V2_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
}
TRON_CLEAR_ABI_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "contractAddress": "string",  # String - hex address
}
TRON_CREATE_SMART_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64) 
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "originAddress": "string",  # Nullable(String) - hex address
    "contractAddress": "string",  # Nullable(String) - hex address
    "abi": "string",  # Nullable(String) - JSON string
    "bytecode": "string",  # Nullable(String) - hex data
    "callValue": "int",  # Nullable(Int64)
    "consumeUserResourcePercent": "int",  # Nullable(Int64)
    "name": "string",  # Nullable(String)
    "originEnergyLimit": "int",  # Nullable(Int64)
    "codeHash": "string",  # Nullable(FixedString(64)) - hex string
    "trxHash": "string",  # Nullable(FixedString(64)) - hex string
    "version": "int",  # Nullable(Int32)
    "callTokenValue": "int",
    "tokenId": "int",
}
TRON_DELEGATE_RESOURCE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "resource": "int",
    "balance": "int",
    "receiverAddress": "string",  # String - hex address
    "lock": "bool",
    "lockPeriod": "int",
}
TRON_EVENT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64) - hex string without 0x prefix
    "logIndex": "int",
    "address": "string",  # String - hex address without 0x prefix
    "topic0": "string",  # Nullable(FixedString(64)) - hex string without 0x prefix
    "topic1": "string",  # Nullable(FixedString(64)) - hex string without 0x prefix
    "topic2": "string",  # Nullable(FixedString(64)) - hex string without 0x prefix
    "topic3": "string",  # Nullable(FixedString(64)) - hex string without 0x prefix
    "data": "string",  # String - hex data
}
TRON_EXCHANGE_CREATE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "firstTokenId": "string",  # String - hex data
    "firstTokenBalance": "int",
    "secondTokenId": "string",  # String - hex data
    "secondTokenBalance": "int",
}
TRON_EXCHANGE_INJECT_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "exchangeId": "int",
    "tokenId": "string",  # String - hex data
    "quant": "int",
}
TRON_EXCHANGE_TRANSACTION_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "exchangeId": "int", 
    "tokenId": "string",  # String - hex data
    "quant": "int",
    "expected": "int",
}
TRON_EXCHANGE_WITHDRAW_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int", 
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "exchangeId": "int",
    "tokenId": "string",  # String - hex data
    "quant": "int",
}
TRON_FREEZE_BALANCE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int", 
    "ownerAddress": "string",  # String - hex address
    "frozenBalance": "int",
    "frozenDuration": "int",
    "resource": "int",
    "receiverAddress": "string",  # String - hex address
}
TRON_FREEZE_BALANCE_V2_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "frozenBalance": "int",
    "resource": "int",
}
TRON_INTERNAL_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64) - hex string without 0x prefix
    "internalIndex": "int",
    "callerAddress": "string",  # String - hex address without 0x prefix
    "transferToAddress": "string",  # String - hex address without 0x prefix
    "callValueInfosTokenId": "list[str]",  # callValueInfos.tokenId
    "callValueInfosCallValue": "list[int]",  # callValueInfos.callValue
    "note": "string",  # String - hex data
    "rejected": "bool",
    "extra": "string",  # String - hex data
}
TRON_MARKET_CANCEL_ORDER_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "orderId": "string",  # String - hex data
}
TRON_MARKET_SELL_ASSET_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "sellTokenId": "string",  # String - hex data
    "sellTokenQuantity": "int",
    "buyTokenId": "string",  # String - hex data
    "buyTokenQuantity": "int",
}
TRON_PARTICIPATE_ASSET_ISSUE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "toAddress": "string",  # String - hex address
    "assetName": "string",  # String - hex data
    "amount": "int",
}
TRON_PROPOSAL_APPROVE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int", 
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "proposalId": "int",
    "isAddApproval": "bool",
}
TRON_PROPOSAL_CREATE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "parameters": "dict[int, int]",  # Map(Int64, Int64)
}
TRON_PROPOSAL_DELETE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "proposalId": "int",
}
TRON_SET_ACCOUNT_ID_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "accountId": "string",  # String - hex data
    "ownerAddress": "string",  # String - hex address
}
TRON_SHIELDED_TRANSFER_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "transparentFromAddress": "string",  # String - hex address
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
    "bindingSignature": "string",  # FixedString(64) - hex string
    "transparentToAddress": "string",  # String - hex address
    "toAmount": "int",
}
TRON_TRANSACTION_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "index": "int",
    "hash": "string",  # FixedString(64) - hex string without 0x prefix
    "expiration": "int",
    "authorityAccountNames": "list[str]",  # Array(LowCardinality(String))
    "authorityAccountAddresses": "list[str]",  # Array(String)
    "authorityPermissionNames": "list[str]",  # Array(LowCardinality(String))
    "data": "string",  # String - hex data
    "contractType": "string",  # LowCardinality(String)
    "contractProvider": "string",  # Nullable(String)
    "contractName": "string",  # Nullable(String)
    "contractPermissionId": "int",  # Nullable(Int32)
    "scripts": "string",  # String - hex data
    "timestamp": "int",
    "feeLimit": "int",
    "constantResult": "string",  # String - hex data
    "fee": "int",
    "contractResult": "string",  # Nullable(String) - hex data
    "contractAddress": "string",  # Nullable(String) - hex address
    "energyUsage": "int",
    "energyFee": "int",
    "originEnergyUsage": "int",
    "energyUsageTotal": "int",
    "netUsage": "int",
    "netFee": "int",
    "receiptResult": "string",  # LowCardinality(String)
    "result": "string",  # LowCardinality(String)
    "resMessage": "string",  # String - hex data
    "assetIssueId": "string",
    "withdrawAmount": "int",
    "unfreezeAmount": "int",
    "exchangeReceivedAmount": "int",
    "exchangeInjectAnotherAmount": "int",
    "exchangeWithdrawAnotherAmount": "int",
    "exchangeId": "int",
    "shieldedTransactionFee": "int",
    "orderId": "string",  # FixedString(64) - hex string without 0x prefix
    "orderDetailMakerOrderId": "list[str]",  # orderDetails.makerOrderId
    "orderDetailTakerOrderId": "list[str]",  # orderDetails.takerOrderId  
    "orderDetailFillSellQuantity": "list[int]",  # orderDetails.fillSellQuantity
    "orderDetailFillBuyQuantity": "list[int]",  # orderDetails.fillBuyQuantity
    "packingFee": "int",
    "withdrawExpireAmount": "int",
    "cancelUnfreezeV2Amount": "dict[str, int]",  # Map(String, Int64)
}
TRON_TRANSFER_ASSET_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int", 
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "assetName": "string",  # String - hex data
    "ownerAddress": "string",  # String - hex address
    "toAddress": "string",  # String - hex address
    "amount": "int",
}
TRON_TRANSFER_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int", 
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "toAddress": "string",  # String - hex address
    "amount": "int",
}
TRON_TRIGGER_SMART_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int", 
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "contractAddress": "string",  # String - hex address
    "callValue": "int",
    "data": "string",  # String - hex data
    "callTokenValue": "int",
    "tokenId": "int",
}
TRON_UNDELEGATE_RESOURCE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "resource": "int",
    "balance": "int",
    "receiverAddress": "string",  # String - hex address
}
TRON_UNFREEZE_ASSET_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
}
TRON_UNFREEZE_BALANCE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "resource": "int",
    "receiverAddress": "string",  # String - hex address
}
TRON_UNFREEZE_BALANCE_V2_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "unfreezeBalance": "int",
    "resource": "int",
}
TRON_UPDATE_ASSET_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "description": "string",  # String - hex data
    "url": "string",  # String - hex data
    "newLimit": "int",
    "newPublicLimit": "int",
}
TRON_UPDATE_BROKERAGE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "brokerage": "int",
}
TRON_UPDATE_ENERGY_LIMIT_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "contractAddress": "string",  # String - hex address
    "originEnergyLimit": "int",
}
TRON_UPDATE_SETTING_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "contractAddress": "string",  # String - hex address
    "consumeUserResourcePercent": "int",  # Int64
}
TRON_VOTE_ASSET_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int", 
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "voteAddress": "list[str]",  # Array(String) - hex addresses
    "support": "bool",
    "count": "int",
}
TRON_VOTE_WITNESS_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int", 
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int", 
    "ownerAddress": "string",  # String - hex address
    "votesVoteAddress": "list[str]",  # votes.voteAddress
    "votesVoteCount": "list[int]",  # votes.voteCount
    "support": "bool",
}
TRON_WITHDRAW_BALANCE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
}
TRON_WITHDRAW_EXPIRE_UNFREEZE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
}
TRON_WITNESS_CREATE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int",
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "url": "string",  # String - hex data
}
TRON_WITNESS_UPDATE_CONTRACT_COLUMN_FORMATS: Dict[str, str | Dict[str, str]] = {
    "blockNumber": "int", 
    "blockTimestamp": "int",
    "transactionIndex": "int",
    "transactionHash": "string",  # FixedString(64)
    "contractIndex": "int",
    "ownerAddress": "string",  # String - hex address
    "updateUrl": "string",  # String - hex data
}