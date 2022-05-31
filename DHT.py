import socket
import threading
import os
import time
import hashlib
from json import loads, dumps
import logging
from queue import Queue


class Node:
    def __init__(self, host, port):
        self.stop = False
        self.host = host
        self.port = port
        self.M = 16
        self.N = 2**self.M
        self.key = self.hasher(host+str(port))
        # You will need to kill this thread when leaving, to do so just set self.stop = True
        threading.Thread(target=self.listener).start()
        self.files = []
        self.backUpFiles = []
        if not os.path.exists(host+"_"+str(port)):
            os.mkdir(host+"_"+str(port))
        '''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
        # Set value of the following variables appropriately to pass Intialization test
        self.addr = (self.host, self.port)
        self.successor: tuple = self.addr
        self.predecessor: tuple = self.addr
        # additional state variables
        self.directory = self.host+"_"+str(self.port)
        self.queue = Queue(maxsize=1)

        self.heart_beat = 0.5

        threading.Thread(target=self.ping_predecessor, daemon=True).start()
        threading.Thread(target=self.ping_successor, daemon=True).start()

        self.logger = logging.getLogger(f"{self.port}->{self.key}")
        self.logger.setLevel(logging.ERROR)

        formater = logging.Formatter(
            "%(name)s:%(levelname)s:%(funcName)s:%(message)s")

        sh = logging.StreamHandler()
        sh.setLevel(logging.DEBUG)
        sh.setFormatter(formater)

        self.logger.addHandler(sh)

    def __repr__(self) -> str:
        return f"{self.port} -> {self.key}"

    def hasher(self, key):
        '''
        DO NOT EDIT THIS FUNCTION.
        You can use this function as follow:
                For a node: self.hasher(node.host+str(node.port))
                For a file: self.hasher(file)
        '''
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N

    def server_hash(self, addr):
        return self.hasher(addr[0] + str(addr[1]))

    def handleConnection(self, client, addr):
        '''
         Function to handle each inbound connection, called as a thread from the listener.
        '''

        data = self.recv(client)
        command = data["command"]
        addr = tuple(data["addr"])

        if command == "lookup":
            self.lookup(data["key"], addr)

        elif command == "lookup_node":
            self.lookup_for_node(data["key"], addr)

        elif command == "lookup_response":
            self.queue.put(addr)

        elif command == "get_neighbors":
            self.send_neighbors(client)

        elif command == "update_successor":
            self.successor = addr
            self.logger.debug("successor   updated to: %s", self.successor)

            # backups are stored on successors. a change in successor will cause
            # the backup to change.
            self.send_files_as_backup()

        elif command == "update_predecessor":
            self.predecessor = addr
            self.logger.debug("predecessor updated to: %s", self.predecessor)

        elif command == "migrate_data":
            self.predecessor = addr
            self.migrate_files(self.predecessor)

            self.send_files_as_backup()

        elif command == "accept_inbound_file":
            file_name = data["file_name"]

            self.send_msg_to_sock(client, self.make_msg("init_transfer"))

            self.recieveFile(client, os.path.join(self.directory, file_name))

            self.register_file(file_name)

            # received confirmation
            self.send_msg_to_sock(
                client,
                self.make_msg("file_accepted")
            )

            self.logger.info("received %s", file_name)

        elif command == "accept_inbound_backup_file":
            file_name = data["file_name"]

            self.send_msg_to_sock(client, self.make_msg("init_transfer"))

            self.recieveFile(client, os.path.join(self.directory, file_name))

            self.backUpFiles.append(file_name)

        elif command == "clear_backup":
            self.clear_backup()

            self.send_msg_to_sock(
                client,
                self.make_msg("clear_backup_complete")
            )

        elif command == "request_file":
            self.send_requested_file(client, data["file_name"])

        elif command == "ping":
            pass

    def ping_successor(self):
        while not self.stop:
            time.sleep(self.heart_beat)

            if self.successor != self.addr:
                try:
                    self.send_msg_to_addr(
                        self.successor, self.make_msg("ping"))

                except ConnectionError:
                    # successor down
                    self.logger.warning("successor %s down", self.successor)
                    self.successor = self.addr

    def ping_predecessor(self):
        while not self.stop:
            time.sleep(self.heart_beat)

            if self.predecessor != self.addr:
                try:
                    self.send_msg_to_addr(
                        self.predecessor, self.make_msg("ping"))

                except ConnectionError:
                    # predecessor down

                    # wait for heart_beat time as the pings are not synced and
                    # it could be the case that the predecessor node has not yet
                    # recognized that its succor left

                    time.sleep(self.heart_beat)
                    self.logger.warning(
                        "predecessor %s down", self.predecessor)
                    self.predecessor = self.addr

                    self.restore_backup()
                    self.send_files_as_backup()

                    self.send_msg_to_addr(
                        self.successor,
                        self.make_msg("lookup_node", key=self.key)
                    )

                    self.predecessor = self.queue.get()

                    self.send_msg_to_addr(
                        self.predecessor,
                        self.make_msg("update_successor")
                    )

    def migrate_files(self, to):
        files_to_send = []

        for file in self.files:
            if not self.key_in_domain(self.hasher(file)):
                files_to_send.append(file)

        for file in files_to_send:
            self.send_file_to(to, file)

            self.files.remove(file)

        # all the migrated files will become the backup so no need to delete
        # the file migration and backup will be running at the same time and both
        # will be changing the same directory thus causing errors.
        # Possible fixes:
        # - use a backup subfolder
        # - add a file_type field to messages to distinguish put and migrate
        # - let the files persist

    def send_files_as_backup(self):
        with socket.socket() as sock:
            sock.connect(self.successor)
            self.send_msg_to_sock(
                sock,
                self.make_msg("clear_backup")
            )

            _ = self.recv(sock)

        for file in self.files:
            self.send_backup_file_to(self.successor, file)

    def restore_backup(self):
        for file in self.backUpFiles:
            self.files.append(file)

        self.backUpFiles.clear()

    def clear_backup(self):
        self.backUpFiles.clear()

    def send_neighbors(self, sock: socket.socket):

        self.send_msg_to_sock(
            sock,
            self.make_msg(
                "neighbors",
                predecessor=self.addr,
                successor=self.successor
            )
        )

    def lookup(self, key: int, addr: tuple):
        '''
        Lookup key in the current doamin.

        If found send the lookup_response to the
        enquirer. Else forward to the predecessor.

        key: key to lookup
        addr: enquirer's address
        '''
        if self.key_in_domain(key):
            self.send_msg_to_addr(addr, self.make_msg("lookup_response"))
            return

        self.send_msg_to_addr(
            self.predecessor,
            self.make_msg("lookup", key=key, addr=addr)
        )

    def lookup_for_node(self, key: int, addr: tuple):
        if self.node_in_domain(key):
            self.send_msg_to_addr(addr, self.make_msg("lookup_response"))
            return

        self.send_msg_to_addr(
            self.successor,
            self.make_msg("lookup_node", key=key, addr=addr)
        )

    def node_in_domain(self, key: int) -> bool:
        successor_key = self.server_hash(self.successor)

        if successor_key == self.key:
            return True

        if successor_key < self.key and (key >= self.key or key < successor_key):
            return True

        if successor_key > self.key and successor_key >= key > self.key:
            return True

        return False

    def key_in_domain(self, key: int) -> bool:
        predecessor_key = self.server_hash(self.predecessor)

        if predecessor_key == self.key:
            return True

        if predecessor_key > self.key and (key <= self.key or key > predecessor_key):
            return True

        if predecessor_key < self.key and self.key >= key > predecessor_key:
            return True

        return False

    def listener(self):
        '''
        We have already created a listener for you, any connection made by other nodes will be accepted here.
        For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
        to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
        '''
        listener = socket.socket()
        listener.bind((self.host, self.port))
        listener.listen(10)
        while not self.stop:
            client, addr = listener.accept()
            threading.Thread(target=self.handleConnection,
                             args=(client, addr)).start()
        print("Shutting down node:", self.host, self.port)
        try:
            listener.shutdown(2)
            listener.close()
        except:
            listener.close()

    def join(self, joiningAddr):
        '''
        This function handles the logic of a node joining. This function should do a lot of things such as:
        Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.

        lookup_node ->get_neighbors -> update_successor -> update_predecessor -> migrate_data
        '''
        if joiningAddr == "":
            return

        self.logger.debug("Req %s for neighbors", joiningAddr)

        self.send_msg_to_addr(joiningAddr, self.make_msg(
            "lookup_node", key=self.key))

        addr = self.queue.get()

        with socket.socket() as sock:
            sock.connect(addr)

            self.send_msg_to_sock(
                sock,
                self.make_msg("get_neighbors")
            )

            data = self.recv(sock)

            self.predecessor = tuple(data["predecessor"])
            self.successor = tuple(data["successor"])

        self.send_msg_to_addr(
            self.predecessor,
            self.make_msg("update_successor")
        )

        self.send_msg_to_addr(
            self.successor,
            self.make_msg("update_predecessor")
        )

        # get rid of the old backup files and make room for the new backup files
        with socket.socket() as sock:
            sock.connect(self.successor)
            self.send_msg_to_sock(
                sock,
                self.make_msg("clear_backup")
            )

            _ = self.recv(sock)

        # migration will handle backups too
        self.send_msg_to_addr(self.successor, self.make_msg("migrate_data"))

    def put(self, fileName):
        '''
        This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
        Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
        in directory given by host_port e.g. "localhost_20007/file.py".

        Utilizes commands:
        lookup -> accept_inbound_file -> (receive) init_transfer ->  sendFile
        '''
        key = self.hasher(fileName)

        if self.key_in_domain(key):
            self.save_file_to_dir(fileName)
            self.register_file(fileName)
            return

        self.send_msg_to_addr(self.successor, self.make_msg("lookup", key=key))

        receiver = self.queue.get()

        with socket.socket() as sock:
            sock.connect(receiver)
            self.send_msg_to_sock(
                sock,
                self.make_msg("accept_inbound_file", file_name=fileName)
            )

            # wait for init_transfer msg
            _ = self.recv(sock)

            self.sendFile(sock, fileName)

    def register_file(self, file_name: str):
        '''
        formally register file into the node

        assumption: file_name is already saved in the node's directory
        '''
        self.files.append(file_name)

        # send backup
        self.send_backup_file_to(self.successor, file_name)

    def get(self, fileName):
        '''
        This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
        i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.

        Utilized commands:
        lookup -> request_file -> file_search -> receiveFile()
        '''
        key = self.hasher(fileName)

        if self.key_in_domain(key):
            if fileName in self.files:
                return fileName

            return None

        self.send_msg_to_addr(self.successor, self.make_msg("lookup", key=key))

        addr = self.queue.get()

        with socket.socket() as sock:
            sock.connect(addr)
            self.send_msg_to_sock(
                sock,
                self.make_msg("request_file", file_name=fileName)
            )

            found = self.recv(sock)["found"]

            if found:
                self.send_msg_to_sock(
                    sock,
                    self.make_msg("init_transfer")
                )

                self.recieveFile(sock, fileName)
                return fileName

        return None

    def send_requested_file(self, sock: socket.socket, file_name: str):
        '''
        Responds: request_file

        If present sends requested file to the sender.

        sock: socket object of the instance with sender
        file_name: name of the file to send
        '''
        found = file_name in self.files

        self.send_msg_to_sock(
            sock,
            self.make_msg("file_search", found=found)
        )

        # wait for init_transfer msg

        if found:
            _ = self.recv(sock)

            self.sendFile(
                sock,
                os.path.join(self.directory, file_name)
            )

    def leave(self):
        '''
        When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
        it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
        by setting self.stop flag to True
        '''
        # sequence of the following commands is important
        for file in self.files:
            self.send_file_to(self.successor, file)

        self.send_msg_to_addr(
            self.successor,
            self.make_msg("update_predecessor", addr=self.predecessor)
        )

        self.send_msg_to_addr(
            self.predecessor,
            self.make_msg("update_successor", addr=self.successor)
        )

        self.stop = True

    def sendFile(self, soc, fileName):
        ''' 
        Utility function to send a file over a socket
                Arguments:	soc => a socket object
                                        fileName => file's name including its path e.g. NetCen/PA3/file.py
        '''
        fileSize = os.path.getsize(fileName)
        soc.send(str(fileSize).encode('utf-8'))
        soc.recv(1024).decode('utf-8')
        with open(fileName, "rb") as file:
            contentChunk = file.read(1024)
            while contentChunk != "".encode('utf-8'):
                soc.send(contentChunk)
                contentChunk = file.read(1024)

    def recieveFile(self, soc, fileName):
        '''
        Utility function to recieve a file over a socket
                Arguments:	soc => a socket object
                                        fileName => file's name including its path e.g. NetCen/PA3/file.py
        '''
        fileSize = int(soc.recv(1024).decode('utf-8'))
        soc.send("ok".encode('utf-8'))
        contentRecieved = 0
        file = open(fileName, "wb")
        while contentRecieved < fileSize:
            contentChunk = soc.recv(1024)
            contentRecieved += len(contentChunk)
            file.write(contentChunk)
        file.close()

    def kill(self):
        # DO NOT EDIT THIS, used for code testing
        self.stop = True

    def make_msg(self, command, **kwargs):
        '''
        make message dict

        default addr: self.addr
        '''
        ret_dict = {"command": command, "addr": self.addr}
        ret_dict.update(kwargs)

        return ret_dict

    def send_msg_to_addr(self, to: tuple, msg: dict):
        '''
        sends message to the provided address

        default addr: self.addr
        '''
        with socket.socket() as sock:
            sock.connect(to)
            self.send_msg_to_sock(sock, msg)

    def send_file_to(self, to: tuple, file_name: str):
        with socket.socket() as sock:
            sock.connect(to)

            self.send_msg_to_sock(
                sock,
                self.make_msg("accept_inbound_file", file_name=file_name)
            )

            # wait for init_transfer msg
            _ = self.recv(sock)

            self.sendFile(sock, os.path.join(self.directory, file_name))

            # wait for file_accepted
            _ = self.recv(sock)

    def send_backup_file_to(self, to: tuple, file_name: str):
        '''
        send file as backup
        '''
        if to == self.addr:
            return

        self.logger.debug("backup: %s to %s", file_name, to)
        with socket.socket() as sock:
            sock.connect(to)

            self.send_msg_to_sock(
                sock,
                self.make_msg("accept_inbound_backup_file",
                              file_name=file_name)
            )

            # wait for init_transfer msg
            _ = self.recv(sock)

            self.sendFile(sock, os.path.join(self.directory, file_name))

    def send_msg_to_sock(self, sock: socket.socket, msg: dict):
        sock.send(dumps(msg).encode("utf-8"))

    def recv(self, conn: socket.socket) -> dict:
        data = conn.recv(1024).decode()
        return loads(data)

    def save_file_to_dir(self, file_name):
        with open(file_name, "r", encoding="utf-8") as file:
            data = file.read()

        with open(os.path.join(self.directory, file_name), "w", encoding="utf-8") as file:
            file.write(data)
