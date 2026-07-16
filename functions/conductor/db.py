import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from secrets import aws_secrets

# mysql2pg Phase 5.5 (W2, 2026-07-16): dialect-aware connection, kept in
# lockstep with functions/worker/db.py. NOTE: in rfcx-local the conductor
# path is DEAD (no image builds functions/conductor/; arbimon-legacy's
# enqueueAEDClusteringJob inserts the type-8 jobs row directly and the
# jobqueue-dispatcher claims it). Ported anyway so the two db.py copies
# never diverge on dialect handling if this path is ever resurrected.

def connect():

    # Get database connection parameters from environment variables
    secret_name = os.environ.get('AWS_SECRET')
    if secret_name: params = aws_secrets(secret_name)
    else: params = os.environ
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