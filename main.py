from db import Cassandra

csql = Cassandra()

# Insert Query
results = csql.execute("INSERT INTO users (id, name, age, city) \
                        VALUES (6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47, \
                       'John', 24, 'Delhi')")
print([x for x in results])

# Read query
results = csql.execute("SELECT * FROM users")
print([x for x in results])
