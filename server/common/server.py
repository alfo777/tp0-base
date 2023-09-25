from .utils import Bet
from .utils import process_bet
import socket
import logging

MSG_LEN = 1024

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._sigterm_recv = False

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """
        while not self._sigterm_recv:
            try:
                client_sock = self.__accept_new_connection()
                self.__handle_client_connection(client_sock)
            except OSError as e:
                break

        logging.info('action: exit_gracefully_done | result: server_closed')

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            msg = self.recv_msg(client_sock).decode('utf-8').rstrip("X")
            addr = client_sock.getpeername()
            logging.info(f'action: receive_bet_message | result: success | ip: {addr[0]} | msg: {msg}')
            bet = process_bet(msg)
            response = "OK"
            if bet == None:
                logging.info(f'action: send_error_message | result: fail')
                response = "ERROR"
            else:
                logging.info(f'action: receive_bet_message | result: success | dni: {bet.document} | number: {bet.number}')
            
            self.send_msg(client_sock, response)
        
        except OSError as e:
            logging.error("action: receive_bet_message | result: fail | error: {e}")
        finally:
            client_sock.close()

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
        return c

    def exit_gracefully(self, signum, frame):
        logging.info('action: exit_gracefully_receive | result: prepare_exit_gracefully')
        self._sigterm_recv = True
        self._server_socket.close()
