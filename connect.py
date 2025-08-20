import os, ssl, certifi
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()

ASTRA_HOST = os.getenv("ASTRA_HOST") 
ASTRA_PORT = int(os.getenv("ASTRA_PORT", "29042"))
ASTRA_CLIENT_ID = os.getenv("ASTRA_CLIENT_ID")
ASTRA_CLIENT_SECRET = os.getenv("ASTRA_CLIENT_SECRET")

auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_CLIENT_SECRET)

# TLS with certifi CA bundle
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.load_verify_locations(cafile=certifi.where())
ssl_context.check_hostname = True
ssl_context.verify_mode = ssl.CERT_REQUIRED

print(f"Connecting to {ASTRA_HOST}:{ASTRA_PORT} with TLS …")
cluster = Cluster(
    [ASTRA_HOST],
    port=ASTRA_PORT,
    auth_provider=auth_provider,
    ssl_context=ssl_context,
    protocol_version=4
)
session = cluster.connect()

rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces;")
print("Keyspaces:")
for r in rows:
    print(" -", r.keyspace_name)

cluster.shutdown()
print("Done ✅")
