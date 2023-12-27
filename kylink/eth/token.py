from kylink.db import ClickhouseProvider


class TokenProvider:
    def __init__(self, raw_provider: ClickhouseProvider):
        self.raw_provider = raw_provider
    
