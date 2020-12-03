import os
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from secrets import aws_secrets

def connect():

    # Get database connection parameters from environment variables
    secret_name = os.environ.get('AWS_SECRET')
    if secret_name: params = aws_secrets(secret_name)
    else: params = os.environ
    host     = params.get('host')
    port     = str(params.get('port', '3306'))
    schema   = params.get('schema')
    user     = params.get('username')
    password = params.get('password')

    if not all(params):
        raise Exception('Connection parameters invalid', [host, port, schema, user, password])

    # Establish connection and return session
    engine = create_engine('mysql+mysqlconnector://' + user + ':' + password + '@' + host + ':' + port + '/' + schema)
    Session = sessionmaker(bind=engine, autocommit=False)
    metadata = MetaData()
    return Session(), engine, metadata
