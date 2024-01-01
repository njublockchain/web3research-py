# Kylink-py

Kylink-py, means the package for Python Toolkit for Kylink Platform

## Installation

```
pip install kylink
```

## Usage

Example: fetch and parse a USDT Transfer Event

```python
import os
import web3
import kylink
from kylink.evm import SingleEventDecoder

# for internet
ky = kylink.Kylink(api_token=YOUT_APIKEY)

log = ky.eth.events(
    "address = unhex('dac17f958d2ee523a2206206994597c13d831ec7')", limit=1
)[0]

w3 = web3.Web3()

abi = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "from", "type": "address"},
        {"indexed": True, "name": "to", "type": "address"},
        {"indexed": False, "name": "value", "type": "uint256"},
    ],
    "name": "Transfer",
    "type": "event",
}
decoder = SingleEventDecoder(w3, event_abi=abi)
result = decoder.decode(log)
print(result)

```

You can read detailed guide on [our document site](https://doc.kylink.xyz/)
