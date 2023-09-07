from .utils import Bet
from .utils import process_bet
from .utils import generate_winners_message
from .utils import do_lottery
from threading import Lock
import socket
import logging
import threading

CANT_AGENCIES = 5

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._sigterm_recv = False
        self._threads = []
        self._lock = Lock()


    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        # TODO: Modify this program to handle signal to graceful shutdown
        # the server
        while not self._sigterm_recv:
            try:
                self.__accept_new_connection()
                if ( len(self._threads) == CANT_AGENCIES ):
                    for t in self._threads:
                        t.start()
                    
                    for t in self._threads:
                        t.join()   
                # self.__handle_client_connection(client_sock)
            except OSError as e:
                break

        logging.info('action: exit_gracefully_done | result: server_closed')

    def __handle_client_connection(self, client_sock, lock):
        """
        Read N messages from a specific client socket, it does not close the connection yet

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            client_sock.send(self.send_res("READY").encode('utf-8'))
            n = 1
            
            while True: 
                msg = self.recv_msg(client_sock).decode('utf-8').rstrip("X")
                addr = client_sock.getpeername()
                if msg != "DONE":
                    logging.info(f'action: receive_batch_message | result: success | ip: {addr[0]} | message number: {n}')
                    logging.info(f'action: processing_batch_message | result: pending | ip: {addr[0]} | message number: {n}')
                    lock.acquire()
                    result = process_bet(msg)
                    lock.release()
                    if result == None:
                        self._terminate_all_connections()
                        break
                    else:
                        logging.info(f'action: processing_batch_message | result: success')

                else:
                    break
        
        except OSError as e:
            logging.error("action: receive_bet_message | result: fail | error: {e}")
            self._terminate_all_connections()

    """receive message from client socket"""
    def recv_msg(self, sock):
        result = b''
        remaining = 8192
        while remaining > 0:
            data = sock.recv(remaining)
            result += data
            remaining -= len(data)
        return result

    """creates message for client socket"""
    def send_res(self, message):
        return message.ljust(8192, 'X') + "\n"

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        t = threading.Thread(target=self.process_client, args=(c,self._lock))
        self._threads.append(t)



    """exit gracefully if SIGTERM is recieved"""
    def exit_gracefully(self, signum, frame):
        logging.info('action: exit_gracefully_receive | result: prepare_exit_gracefully')
        self._sigterm_recv = True
        self._server_socket.close()

    """apply protocol to all clients"""
    def process_client(self, c, lock):
        self.__handle_client_connection(c, lock)
        c.send(self.send_res("START_LOTTERY").encode('utf-8'))
        logging.info('action: lottery | result: pending')
        
        lock.acquire()
        lottery_result = do_lottery()
        lock.release()

        logging.info('action: lottery | result: success')
        self.__handle_client_winners(c, lottery_result)
        logging.info('action: closing | result: pending')
        c.close()        
        logging.info('action: closed | result: success')

    """signal that the winners can be requested and send the to its  agency"""
    def __handle_client_winners(self, client_sock, lottery_result):
        try:
            addr = client_sock.getpeername()
            client_sock.send(self.send_res("READY").encode('utf-8'))
            msg = self.recv_msg(client_sock).decode('utf-8').rstrip("X")
            logging.info(f'action: ready_message_sent | result: success | ip: {addr[0]}')
            agency = int(msg[0])
            winners = lottery_result.get_agency_winners(agency)
            client_sock.send(self.send_res(generate_winners_message(winners)).encode('utf-8'))
        
        except OSError as e:
            logging.error("action: receive_bet_message | result: fail | error: {e}")


    def _terminate_all_connections(self):
        for c in self._connections:
            c.send(self.send_res("ERROR").encode('utf-8'))
            c.close()

        self._connections = []