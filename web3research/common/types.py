class Address:
    def __init__(self, addr: str):
        assert addr.startswith("0x"), f"Address must start with 0x, got {addr}"
        self.addr = addr

    def __str__(self):
        return f"unhex('{self.addr.removeprefix('0x')}')"

    def __repr__(self):
        return self.addr

    def __eq__(self, other):
        return self.addr == other.addr

    def __hash__(self):
        return self.addr


class Hash:
    def __init__(self, hash: str):
        assert hash.startswith("0x"), f"Hash must start with 0x, got {hash}"
        self.hash = hash

    def __str__(self):
        return f"unhex('{self.hash.removeprefix('0x')}')"

    def __repr__(self):
        return self.hash

    def __eq__(self, other):
        return self.hash == other.hash

    def __hash__(self):
        return self.hash
