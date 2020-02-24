import logging
import configparser
import multiprocessing

import psycopg2

from paramiko import SSHClient, AutoAddPolicy, RSAKey
from paramiko.auth_handler import AuthenticationException, SSHException

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

logging.basicConfig(filename="remote_ssh_client.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class RemoteClient:
    """Client to interact with a remote host via SSH"""

    def __init__(self, hostname: str, username: str):
        self.host = hostname
        self.user = username
        self.client = None
        self.ssh_key = None
        self.key_file_obj = None
        config = configparser.ConfigParser()
        config.read('db_config.ini')
        self.conn = psycopg2.connect(
            host=config.get('DB', 'DB_host'),
            port=config.get('DB', 'DB_PORT'),
            database=config.get('DB', 'DB_DATABASE'),
            user=config.get('DB', 'DB_USERNAME'),
            password=config.get('DB', 'DB_PASSWORD')
        )
        self.cur = self.conn.cursor()

    # Fetch and transfer SSH Keys.
    # ------------------------------------------------------
    def __get_ssh_key(self):
        """Fetch locally stored SSH key."""
        try:
            query_cmd = """SELECT * FROM keys WHERE host='""" + self.host + """'"""
            self.cur.execute(query_cmd)
            key_string = self.cur.fetchone()[1]
            self.key_file_obj = StringIO(key_string)
            self.ssh_key = RSAKey.from_private_key(self.key_file_obj)
        except SSHException as error:
            logger.error(error)
        return self.ssh_key

    # Open and close remote SSH and connections.
    # ------------------------------------------------------
    def __connect(self):
        """Open connection to remote host."""
        try:
            self.client = SSHClient()
            self.client.load_system_host_keys()
            self.client.set_missing_host_key_policy(AutoAddPolicy())
            self.client.connect(self.host,
                                username=self.user,
                                pkey=self.ssh_key,
                                look_for_keys=True,
                                timeout=500)
        except AuthenticationException as error:
            logger.info(
                'Authentication failed: did you remember to create an SSH key?')
            logger.error(error)
            raise error
        finally:
            return self.client

    def disconnect(self):
        """Close ssh connection."""
        self.conn.close()
        self.key_file_obj.close()
        self.client.close()

    # Execute commands on your remote host.
    # ------------------------------------------------------
    def execute_commands(self, commands: [str]):
        """Execute multiple commands in succession."""
        outputs = []
        if self.ssh_key is None:
            self.ssh_key = self.__get_ssh_key()
        if self.client is None:
            self.client = self.__connect()
        for cmd in commands:
            _, stdout, _ = self.client.exec_command(cmd)
            # stdout.channel.recv_exit_status()
            response = stdout.readlines()
            outputs.append(response[0])

        self.disconnect()
        return outputs


def get_cores_from_host(host_user_string: str):
    """
    Summary:

    Takes a single string <hostname>,<username> fashion as input
    and fetches number of cpu cores available on that machine.

    Parameters:
    host_user_string (str): string containing hostname and username
    separated by a comma

    """
    host_user_string = host_user_string.strip()
    host = host_user_string.split(',')[0]
    user = host_user_string.split(',')[1]
    r_client = RemoteClient(host, user)
    outputs = r_client.execute_commands(
        ["grep -c ^processor /proc/cpuinfo"])
    print("{0}\t:\t{1}".format(host, outputs[0].strip()))

if __name__ == "__main__":
    print("Starting remote ssh client...\n")
    print("Computing number of cores...\n")
    print("Hostname\t:\t#cpu_cores")
    with open("hostfile.txt", 'r') as file:
        LINES = file.readlines()

        # creating a pool object
        POOL = multiprocessing.Pool()

        # map list to target function
        POOL.map(get_cores_from_host, LINES)
