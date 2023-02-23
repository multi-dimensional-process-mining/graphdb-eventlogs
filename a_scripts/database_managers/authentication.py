from enum import Enum

from a_scripts.database_managers.credentials import Credentials

# from remote_authentication import remote

# if you want a remote connection, store credentials somewhere different (e.g. remote_authentication)
remote = None

# local credentials
local = Credentials(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="1234"
)


class Connections(Enum):
    REMOTE = 1
    LOCAL = 2


connections_map = {
    Connections.REMOTE: remote,
    Connections.LOCAL: local
}
