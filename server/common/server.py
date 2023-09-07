from .utils import Bet
from .utils import process_bet
from .utils import generate_winners_message
from .utils import do_lottery
import socket
import logging

CANT_AGENCIES = 5

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._sigterm_recv = False
        self._connections = []
        self._lotery_result = None


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
                if ( len(self._connections) == CANT_AGENCIES ):
                    self.process_clients()
                # self.__handle_client_connection(client_sock)
            except OSError as e:
                break

        logging.info('action: exit_gracefully_done | result: server_closed')

    def __handle_client_connection(self, client_sock):
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
                    result = process_bet(msg)
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
        self._connections.append(c)
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')

    """exit gracefully if SIGTERM is recieved"""
    def exit_gracefully(self, signum, frame):
        logging.info('action: exit_gracefully_receive | result: prepare_exit_gracefully')
        self._sigterm_recv = True
        self._server_socket.close()

    """apply protocol to all clients"""
    def process_clients(self):
        for c in self._connections:
            self.__handle_client_connection(c)
        
        for c in self._connections:
            c.send(self.send_res("START_LOTTERY").encode('utf-8'))

        logging.info('action: lottery | result: pending')
        
        self._lottery_result = do_lottery()
        
        logging.info('action: lottery | result: success')

        for c in self._connections:
            self.__handle_client_winners(c)
        
        logging.info('action: closing | result: pending')

        for c in self._connections:
            c.close()
        
        logging.info('action: closed | result: success')

    """signal that the winners can be requested and send the to its  agency"""
    def __handle_client_winners(self, client_sock):
        try:
            addr = client_sock.getpeername()
            client_sock.send(self.send_res("READY").encode('utf-8'))
            msg = self.recv_msg(client_sock).decode('utf-8').rstrip("X")
            logging.info(f'action: ready_message_sent | result: success | ip: {addr[0]}')
            agency = int(msg[0])
            winners = self._lottery_result.get_agency_winners(agency)
            client_sock.send(self.send_res(generate_winners_message(winners)).encode('utf-8'))
        
        except OSError as e:
            logging.error("action: receive_bet_message | result: fail | error: {e}")


    def _terminate_all_connections(self):
        for c in self._connections:
            c.send(self.send_res("ERROR").encode('utf-8'))
            c.close()

        self._connections = []