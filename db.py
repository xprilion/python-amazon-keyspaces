import os
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement


class Cassandra:
    def __init__(self):
        ssl_context = SSLContext(PROTOCOL_TLSv1_2)
        ssl_context.load_verify_locations('.cassandra/AmazonRootCA1.pem')
        ssl_context.verify_mode = CERT_REQUIRED
        auth_provider = PlainTextAuthProvider(
            username='ServiceUsername',
            password='ServicePassword')
        self.cluster = Cluster(
            ['cassandra.us-east-1.amazonaws.com'],
            ssl_context=ssl_context,
            auth_provider=auth_provider,
            port=9142)
        self.session = self.cluster.connect("test_keyspace")

    def execute(self, query):
        return self.session.execute(SimpleStatement(
            query, consistency_level=ConsistencyLevel.LOCAL_QUORUM))
