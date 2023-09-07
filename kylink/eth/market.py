from .gql import GraphQLProvider


class MarketProvider:
    def __init__(self, raw_provider: GraphQLProvider):
        self.raw_provider = raw_provider
