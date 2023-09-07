from .gql import GraphQLProvider


class DeFiProvider:
    def __init__(self, raw_provider: GraphQLProvider):
        self.raw_provider = raw_provider
