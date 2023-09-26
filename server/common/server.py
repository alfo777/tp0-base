from .utils import Bet
from .utils import process_bet
from .utils import generate_winners_message
from .utils import do_lottery
import socket
import logging
import time

CANT_AGENCIES = 5
MSG_LEN = 8192

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

        while not self._sigterm_recv:
            try:
                self.__accept_new_connection()
                if ( len(self._connections) == CANT_AGENCIES ):
                    self.process_clients()
            
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
            self.send_msg(client_sock, "READY")

            while True: 
                msg = self.recv_msg(client_sock).decode('utf-8').rstrip("X")
                addr = client_sock.getpeername()
                if msg != "DONE":
                    logging.info(f'action: receive_batch_message | result: success | ip: {addr[0]}')
                    logging.info(f'action: processing_batch_message | result: pending | ip: {addr[0]}')
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
        remaining = MSG_LEN
        while remaining > 0:
            data = sock.recv(remaining)
            result += data
            remaining -= len(data)
        return result

    """creates message for client socket"""
    def generate_res(self, message):
        return message.ljust(MSG_LEN - 1, 'X') + "\n"

    """send response message to client"""
    def send_msg(self, sock, message):
        response = self.generate_res(message).encode('utf-8')
        remaining = MSG_LEN
        while remaining > 0:
            pos = MSG_LEN - remaining
            nBytesSent = sock.send(response[pos:MSG_LEN])
            logging.info(f'action: sending_response | result: success | message: {message} | bytes_sent: {nBytesSent}')
            remaining -= nBytesSent

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
            self.send_msg(c, "START_LOTTERY")

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
            n = 1
            message = ""
            addr = client_sock.getpeername()
            self.send_msg(client_sock, "READY")
            msg = self.recv_msg(client_sock).decode('utf-8').rstrip("X")
            logging.info(f'action: ready_message_sent | result: success | ip: {addr[0]}')
            agency = int(msg[0])
            winners = self._lottery_result.get_agency_winners(agency)
            logging.info(f'action: sending_winners | result: pending | ip: {addr[0]}')
            
            for winner in winners:
                winnerStr = generate_winners_message(winner)
                if len(message + winnerStr) <= MSG_LEN:
                    message = message + winnerStr
                
                else:  
                    self.send_msg(client_sock, message)
                    logging.info(f'action: sending_winners | result: done | message: {message}')
                    message = winnerStr
            
            self.send_msg(client_sock, message)
            logging.info(f'action: sending_winners | result: done | message: {message}')
            
            time.sleep(2)
            self.send_msg(client_sock, "DONE")
            logging.info(f'action: sending_done | result: done | ip: {addr[0]}')
        
        except OSError as e:
            logging.error("action: receive_bet_message | result: fail | error: {e}")


    def _terminate_all_connections(self):
        for c in self._connections:
            self.send_msg(c, "ERROR")
            c.close()

        self._connections = []