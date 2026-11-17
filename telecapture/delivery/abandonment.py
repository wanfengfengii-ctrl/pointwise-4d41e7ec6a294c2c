from dataclasses import dataclass, field

@dataclass
class AbandonmentIndex:
    starts: dict
    through: dict = field(default_factory=dict)

    def apply(self, stream, logical_mid, unordered=False):
        start = self.starts[(stream, unordered)]
        if logical_mid < start:
            return
        key = (stream, unordered)
        self.through[key] = max(logical_mid, self.through.get(key, start - 1))

    def contains(self, stream, logical_mid, unordered=False):
        key = (stream, unordered)
        return self.starts[key] <= logical_mid <= self.through.get(key, self.starts[key] - 1)

    def next_unabandoned(self, stream, logical_mid, unordered=False):
        if self.contains(stream, logical_mid, unordered):
            return self.through[(stream, unordered)] + 1
        return logical_mid
