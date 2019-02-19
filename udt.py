from cassandra.cluster import Cluster

cluster = Cluster(protocol_version=3)
session = cluster.connect()
session.set_keyspace('office')
session.execute("CREATE TYPE address (street text, zipcode int)")
session.execute("CREATE TABLE users (id int PRIMARY KEY, location frozen<address>)")

# create a class to map to the "address" UDT
class Address(object):

    def __init__(self, street, zipcode):
        self.street = street
        self.zipcode = zipcode

cluster.register_user_type('office', 'address', Address)

# insert a row using an instance of Address
session.execute("INSERT INTO users (id, location) VALUES (%s, %s)",
                (0, Address("123 Main St.", 78723)))

# results will include Address instances
results = session.execute("SELECT * FROM users")
row = results[0]
print (row.id, row.location.street, row.location.zipcode)
