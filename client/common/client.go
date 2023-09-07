package common

import (
	"bufio"
	"fmt"
	"net"
	"time"
	"os"
	"strconv"
	"strings"
	log "github.com/sirupsen/logrus"
)

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopLapse     time.Duration
	LoopPeriod    time.Duration
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
}

// Client Entity that encapsulates a client bet
type Bet struct {
	agency string
	name string
	lastname string
	document string
	birthdate string
	number string
	nameSent bool
	lastnameSent bool
	documentSent bool
	birthdateSent bool
	betSent bool

}

// Read envoirement variables to generate the bet
func generateBet(agency string) Bet {
	bet := Bet {
		agency: agency,
		name: os.Getenv("NOMBRE"),
		lastname: os.Getenv("APELLIDO"),
		document: os.Getenv("DOCUMENTO"),
		birthdate: os.Getenv("NACIMIENTO"),
		number: os.Getenv("NUMERO"),
		nameSent: false,
		lastnameSent: false,
		documentSent: false,
		birthdateSent: false,
		betSent: false,
	}
	return bet
}

func addpaddingToLenString(str string) string {
	if len(str) == 1 {
		return "0" + str
	}
	return str
}


func addNameToMessage(bet Bet) string {
	return "N" + addpaddingToLenString(strconv.Itoa(len(bet.name))) + bet.name
}

func addLastnameToMessage(bet Bet) string {
	return "L" + addpaddingToLenString(strconv.Itoa(len(bet.lastname))) + bet.lastname
}

func addDocumentToMessage(bet Bet) string {
	return "D" + addpaddingToLenString(strconv.Itoa(len(bet.document))) + bet.document
}

func addBirthdateToMessage(bet Bet) string {
	return "B" + addpaddingToLenString(strconv.Itoa(len(bet.birthdate))) + bet.birthdate
}

func addNumberToMessage(bet Bet) string {
	return "V" + addpaddingToLenString(strconv.Itoa(len(bet.number))) + bet.number
}

func generateBetMessage(bet Bet) string {
	msg := bet.agency
	msg += addNameToMessage(bet)
	msg += addLastnameToMessage(bet)
	msg += addDocumentToMessage(bet)
	msg += addBirthdateToMessage(bet)
	msg += addNumberToMessage(bet)
	return strings.Join([]string{msg, strings.Repeat("X", 1024-len(msg))}, "")
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Fatalf(
	        "action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	c.conn = conn
	return nil
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	time.Sleep(8 * time.Second)
	// Send messages if the loopLapse threshold has not been surpassed
loop:
	// Send messages if the loopLapse threshold has not been surpassed
	for timeout := time.After(c.config.LoopLapse); ; {
		select {
		case <-timeout:
			log.Infof("action: timeout_detected | result: success | client_id: %v",
				c.config.ID,
			)
			break loop
		default:
		}
		//generate client bet
		bet := generateBet(c.config.ID)
		sendMessage := generateBetMessage(bet)

		// Create the connection the server in every loop iteration. Send an
		c.createClientSocket()

		fmt.Fprintf(
			c.conn,
			sendMessage,
		)
		msg, err := bufio.NewReader(c.conn).ReadString('\n')
		response := strings.Trim(msg, "\n")
		response = strings.Trim(response, "X")
		
		c.conn.Close()
		log.Infof("action: server_response_recieved | message: %s", response, )
		
		if err != nil {
			log.Errorf("action: bet_stored | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return
		} else if response == "ERROR" {
			log.Infof("action: bet_stored | result: fail | error: error ocurrer while trying to store bet")
			return
		}
		log.Infof("action: bet_stored | result: success | dni: %s | number: %s | message: %s}",
			bet.document,
			bet.number,
			response,
		)
		break loop
	}
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}