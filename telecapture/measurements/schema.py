import sqlite3

SCHEMA_VERSION = 1

class ArchiveError(ValueError):
    def __init__(self, reason, message):
        self.reason = reason
        super().__init__(f'{reason}: {message}')

DDL = {
    'recordings': """CREATE TABLE recordings (
        registration_key TEXT PRIMARY KEY NOT NULL,
        canonical_header TEXT NOT NULL)""",
    'messages': """CREATE TABLE messages (
        delivery_key TEXT PRIMARY KEY NOT NULL,
        registration_key TEXT NOT NULL REFERENCES recordings(registration_key),
        recording_id TEXT NOT NULL, gateway_id TEXT NOT NULL, session_id TEXT NOT NULL,
        stream_id INTEGER NOT NULL, logical_mid TEXT NOT NULL, raw_mid INTEGER NOT NULL,
        body BLOB NOT NULL, digest TEXT NOT NULL, provenance TEXT NOT NULL)""",
    'measurements': """CREATE TABLE measurements (
        delivery_key TEXT NOT NULL REFERENCES messages(delivery_key),
        row_number INTEGER NOT NULL, sensor_id TEXT NOT NULL, time_us TEXT NOT NULL,
        quantity TEXT NOT NULL, unit TEXT NOT NULL, value_micro TEXT, quality TEXT NOT NULL,
        PRIMARY KEY (delivery_key, row_number))""",
}

def _signature(connection, name):
    columns = tuple(connection.execute(f'PRAGMA table_info({name})'))
    foreign = tuple(connection.execute(f'PRAGMA foreign_key_list({name})'))
    unique = []
    for row in connection.execute(f'PRAGMA index_list({name})'):
        if row[2]:
            # Names of SQLite's automatic indexes are implementation details.
            index = row[1].replace('"', '""')
            unique.append(tuple(x[2] for x in connection.execute(f'PRAGMA index_info("{index}")')))
    return columns, foreign, tuple(sorted(unique))

def validate_schema(connection, allow_missing=False):
    version = connection.execute('PRAGMA user_version').fetchone()[0]
    if version not in (0, SCHEMA_VERSION):
        raise ArchiveError('schema', f'unsupported schema version {version}')
    reference = sqlite3.connect(':memory:')
    try:
        for statement in DDL.values():
            reference.execute(statement)
        for name in DDL:
            kind = connection.execute('SELECT type FROM sqlite_master WHERE name=?', (name,)).fetchone()
            if kind is None and allow_missing:
                continue
            if kind != ('table',) or _signature(connection, name) != _signature(reference, name):
                raise ArchiveError('schema', f'incompatible structure: {name}')
        triggers = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name IN ('recordings','messages','measurements')").fetchall()
        if triggers:
            raise ArchiveError('schema', 'unsupported triggers on archive tables')
        if not allow_missing and connection.execute('PRAGMA foreign_key_check').fetchone():
            raise ArchiveError('schema', 'broken evidence references')
    finally:
        reference.close()

def initialize_archive(connection):
    try:
        validate_schema(connection, allow_missing=True)
        connection.execute('BEGIN IMMEDIATE')
        for name, statement in DDL.items():
            if connection.execute('SELECT 1 FROM sqlite_master WHERE name=?', (name,)).fetchone() is None:
                connection.execute(statement)
        connection.execute(f'PRAGMA user_version={SCHEMA_VERSION}')
        validate_schema(connection)
        connection.execute('COMMIT')
    except BaseException:
        if connection.in_transaction:
            connection.execute('ROLLBACK')
        raise
