import os
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
aws_secrets = None  # AWS Secrets Manager optional; use env

def connect():

    # Get database connection parameters from environment variables
    secret_name = os.environ.get('AWS_SECRET')
    if False: params = None
    params = os.environ
    host     = params.get('host')
    port     = str(params.get('port', '3306'))
    schema   = params.get('schema')
    user     = os.environ.get('ARBIMON_DB_USER', 'tm_driver')
    password = params.get('tm_driver_pwd')

    if not all(params):
        raise Exception('Connection parameters invalid', [host, port, schema, user, password])

    # Establish connection and return session
    engine = create_engine('mysql+mysqlconnector://' + user + ':' + password + '@' + host + ':' + port + '/' + schema)
    Session = sessionmaker(bind=engine, autocommit=False)
    metadata = MetaData()
    return Session(), engine, metadata
