"""An in-memory stand-in for a Kafka topic and a confluent_kafka.Consumer, for testing the consumer loop.

It models what the consumer relies on: per-partition logs with offsets, a consumer group's committed offsets,
delivery in small per-partition chunks (so a batch usually ends in the middle of a snapshot), rebalance callbacks,
and restart from the committed offsets.
"""
from __future__ import annotations


class TP:
    def __init__(self, partition: int):
        self.partition = partition


class Msg:
    def __init__(self, topic, partition, offset, key, value, ts_ms):
        self._t, self._p, self._o, self._k, self._v, self._ts = topic, partition, offset, key, value, ts_ms

    def error(self):
        return None

    def topic(self):
        return self._t

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def key(self):
        return self._k

    def value(self):
        return self._v

    def timestamp(self):
        return (1, self._ts)  # (TIMESTAMP_CREATE_TIME, ms)


class Broker:
    def __init__(self, topic: str, partitions: int):
        self.topic = topic
        self.logs = {p: [] for p in range(partitions)}
        self.committed: dict[str, dict[int, int]] = {}

    def produce(self, partition: int, key: bytes | None, value: bytes, ts_ms: int = 0) -> None:
        log = self.logs[partition]
        log.append(Msg(self.topic, partition, len(log), key, value, ts_ms))

    def produce_all(self, msgs, ts_ms: int = 0) -> None:
        for p, k, v in msgs:
            self.produce(p, k, v, ts_ms)


class Consumer:
    """consume() returns up to `chunk` messages from each assigned partition in turn (or, with sequential=True,
    drains the partitions one after another, lowest first)."""

    def __init__(self, broker: Broker, group: str, chunk: int = 7, partitions: list[int] | None = None,
                 sequential: bool = False):
        self.broker, self.group, self.chunk, self.sequential = broker, group, chunk, sequential
        self.initial = sorted(broker.logs) if partitions is None else partitions
        self.assigned: list[int] = []
        self.position: dict[int, int] = {}
        self.on_assign = self.on_revoke = None
        self.closed = False
        self.commits = 0

    def subscribe(self, topics, on_assign=None, on_revoke=None):
        assert topics == [self.broker.topic]
        self.on_assign, self.on_revoke = on_assign, on_revoke

    def assign(self, partitions: list[int]) -> None:
        committed = self.broker.committed.get(self.group, {})
        for p in partitions:
            self.position[p] = committed.get(p, 0)
        self.assigned = sorted(set(self.assigned) | set(partitions))
        if self.on_assign:
            self.on_assign(self, [TP(p) for p in partitions])

    def revoke(self, partitions: list[int]) -> None:
        if self.on_revoke:
            self.on_revoke(self, [TP(p) for p in partitions])
        self.assigned = [p for p in self.assigned if p not in partitions]
        for p in partitions:
            self.position.pop(p, None)

    def consume(self, num_messages: int = 1, timeout: float = 0.0):
        if not self.assigned and self.initial:
            self.assign(self.initial)
            self.initial = []
        out = []
        for p in self.assigned:
            log = self.broker.logs[p]
            take = len(log) if self.sequential else self.chunk
            batch = log[self.position[p]: self.position[p] + take]
            out += batch
            self.position[p] += len(batch)
            if self.sequential and batch:
                break
        return out

    def commit(self, asynchronous: bool = False):
        assert not asynchronous
        self.broker.committed.setdefault(self.group, {}).update({p: self.position[p] for p in self.assigned})
        self.commits += 1

    def close(self):
        self.closed = True
