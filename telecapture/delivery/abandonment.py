from dataclasses import dataclass, field

@dataclass
class AbandonmentIndex:
    starts: dict
    through: dict = field(default_factory=dict)

    def apply(self, stream, logical_mid):
        start = self.starts[stream]
        if logical_mid < start:
            return
        self.through[stream] = max(logical_mid, self.through.get(stream, start - 1))

    def contains(self, stream, logical_mid):
        return self.starts[stream] <= logical_mid <= self.through.get(stream, self.starts[stream] - 1)

    def next_unabandoned(self, stream, logical_mid):
        if self.contains(stream, logical_mid):
            return self.through[stream] + 1
        return logical_mid
