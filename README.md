# Web3Research Python Toolkit

`web3research-py` is the official python software development kit for leveraging datasets on [Web3Research Platform](http://web3resear.ch)

**WARNING**: This package is not built for Web3Research dashboards, which means that you might not be able to run on the Python Cards.

## Installation

From pypi (stable)
```bash
pip install -U web3research
```

From github (latest)
```bash
pip install -U git+https://github.com/njublockchain/web3research-py
```

## Usage

Example: fetch and parse a USDT Transfer Event

```python
import os
import web3
import web3research
from web3research.evm import SingleEventDecoder
from web3research.common import Address

# for internet
w3r = web3research.Web3Research(api_token=YOUT_APIKEY)

USDT_address = Address('0xdac17f958d2ee523a2206206994597c13d831ec7')

log = w3r.eth.events(
    where=f"address = {USDT_address}", 
    order_by={"blockNumber", True},
    limit=1
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

More practical examples are shown on [web3research-py-examples](http://github.com/njublockchain/web3research-py-examples)

You can read detailed guide on [our document site](https://doc.web3resear.ch/). Auto-generated python API document is available  on https://web3research.readthedocs.io/.
