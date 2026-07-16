import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
aws_secrets = None  # AWS Secrets Manager optional; use env

# mysql2pg Phase 5.5 (W2, 2026-07-16): dialect-aware connection.
# ARBIMON_DB_DIALECT=mysql (DEFAULT - byte-for-byte today's behavior) or
# postgres. The PG path activates ONLY via env at the coordinated jobs-plane
# flip; nothing changes for existing deploys. Env names stay the same for
# both dialects (host/port/schema/tm_driver_pwd/ARBIMON_DB_USER; the flip
# changes env VALUES, not names; ARBIMON_DB_PORT overrides port when set).
# Credentials are URL-quoted (quote_plus): the string-concat URL breaks on
# any password containing URL-special characters (found by the W1 smoke).

def connect():

    # Get database connection parameters from environment variables
    secret_name = os.environ.get('AWS_SECRET')
    if False: params = None
    params = os.environ
    host     = params.get('host')
    port     = str(os.environ.get('ARBIMON_DB_PORT') or params.get('port', '3306'))
    schema   = params.get('schema')
    user     = os.environ.get('ARBIMON_DB_USER', 'tm_driver')
    password = params.get('tm_driver_pwd')

    if not all(params):
        raise Exception('Connection parameters invalid', [host, port, schema, user, password])

    # Establish connection and return session
    dialect = os.environ.get('ARBIMON_DB_DIALECT', 'mysql').lower()
    if dialect in ('postgres', 'postgresql', 'pg'):
        # executemany_mode: every session.execute(table.insert(), [dicts])
        # bulk path (detections / playlist_aed) becomes multi-row VALUES on
        # PG (worker-port template rule 4) without touching call sites.
        engine = create_engine(
            'postgresql+psycopg2://' + quote_plus(user) + ':'
            + quote_plus(password) + '@' + host + ':' + port + '/' + schema,
            connect_args={'connect_timeout': 10},
            executemany_mode='values_plus_batch')
    else:
        engine = create_engine(
            'mysql+mysqlconnector://' + quote_plus(user) + ':'
            + quote_plus(password) + '@' + host + ':' + port + '/' + schema)
    Session = sessionmaker(bind=engine, autocommit=False)
    metadata = MetaData()
    return Session(), engine, metadata