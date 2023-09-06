import sys

class VersionConfig:
    def getConfig(self):
        return """
version: '3.9'
name: tp0
services:"""

class ServerConfig:
    def getConfig(self):
        return """
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=DEBUG
    networks:
      - testing_net"""

class ClientConfig:
    def getConfig(self, n):
        return """
  client{}:
    container_name: client{}
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID={}
      - CLI_LOG_LEVEL=DEBUG
    networks:
      - testing_net
    depends_on:
      - server""".format(n, n, n)

class NetworksConfig:
    def getConfig(self):
        return """
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24"""



def main():
    args = sys.argv[1:]

    if ( len(args) != 2 ):
        print("Error: wrong number of arguments, must be 2")
        exit(1)
    
    elif ( args[0] != "-n" ):
        print("Error: cant read option {}".format(args[0]))
        exit(1)

    elif ( not args[1].isdecimal() or int(args[1]) <= 0  ):
        print("Error: must have a greaten than zero numeric value")
        exit(1)

    n = int(args[1])
    version = VersionConfig()
    server = ServerConfig()
    networks = NetworksConfig()
    client = ClientConfig()
    

    f = open("docker-compose-dev.yaml", "w")
    f.write(version.getConfig())
    f.write(server.getConfig())

    for i in range(1, n + 1):
        f.write(client.getConfig(i))

    f.write(networks.getConfig())
    f.close()


if __name__ == "__main__":
    main()