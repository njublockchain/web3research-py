# Web3Research-py

`web3research-py` is the official python software development kit for all dashboards on [Web3Research Platform](http://web3resear.ch)

## Installation

```
pip install -U web3research
```

## Usage

Example: fetch and parse a USDT Transfer Event

```python
import os
import web3
import web3research
from web3research.evm import SingleEventDecoder

# for internet
w3r = web3research.Web3Research(api_token=YOUT_APIKEY)

log = w3r.eth.events(
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

You can read detailed guide on [our document site](https://doc.web3resear.ch/)
