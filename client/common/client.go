package common

import (
	"bufio"
	"fmt"
	"net"
	"time"
	"os"
	"io"
	"strconv"
	"strings"
	"encoding/csv"
	"unicode/utf8"
	log "github.com/sirupsen/logrus"
)

const MAX_LEN = 99
const MSG_SIZE = 8192
var BATCH_SIZE, ERR = strconv.Atoi(os.Getenv("BATCH_SIZE"))

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
}



func processBets(c *Client, id string) {
	var batch []Bet
	nMsg := 1
	f, err := os.Open(os.Getenv("FILE_NAME"))
    if err != nil {
		log.Errorf("action: read_agency_file | result: fail | error: %s", err,)
		return
	}

    csvReader := csv.NewReader(f)
    
	for {
		var bet Bet
        rec, err := csvReader.Read()
		if err == io.EOF {
            break
        }
        if err != nil {
            log.Errorf("action: read_agency_file | result: fail | error: %s", err,)
		}

		bet = parseLineIntoBet(rec, id)
        
		batch = append(batch, bet)

		if ( len(batch) == BATCH_SIZE ) {
			var batchToSend []Bet
			var message string
			batchToSend, message = generateMessage(batch, id)
			log.Infof("action: sending_batch | result: pending | batch_number: %v}", nMsg, )
			fmt.Fprintf( c.conn, message,)
			log.Infof("action: batch_sent | result: sucess | batch_number: %v}", nMsg, )
			nMsg += 1
			batch = batchToSend

		}
    }

	for len(batch) != 0 {
		var batchToSend []Bet
		var message string
		batchToSend, message = generateMessage(batch, id)
		log.Infof("action: sending_batch | result: pending | batch_number: %v}", nMsg,)
		fmt.Fprintf( c.conn, message,)
		log.Infof("action: batch_sent | result: sucess | batch_number: %v}", nMsg,)
		nMsg += 1
		batch = batchToSend
	}

	defer f.Close()
}

func parseLineIntoBet(data []string, agency string) Bet {
    var bet Bet
	bet.agency = agency
	for j, field := range data {
		if j == 0 {
			bet.name = field
		} else if j == 1 {
			bet.lastname = field
		} else if j == 2 {
			bet.document = field
		} else if j == 3 {
			bet.birthdate = field
		} else if j == 4 {
			bet.number = field
		}
	}
    return bet
}

func addpaddingToLenString(str string) string {
	if len(str) == 1 {
		return "0" + str
	}
	return str
}

func truncateStr(str string) string {
	if ( len(str) > MAX_LEN ) {
		return str[:MAX_LEN]
	} 
	return str
}

func addNameToMessage(bet Bet) string {
	str := truncateStr(bet.name)
	return "N" + addpaddingToLenString(strconv.Itoa(utf8.RuneCountInString((str)))) + str
}

func addLastnameToMessage(bet Bet) string {
	str := truncateStr(bet.lastname)
	return "L" + addpaddingToLenString(strconv.Itoa(utf8.RuneCountInString(str))) + str
}

func addDocumentToMessage(bet Bet) string {
	str := truncateStr(bet.document)
	return "D" + addpaddingToLenString(strconv.Itoa(len(str))) + str
}

func addBirthdateToMessage(bet Bet) string {
	str := truncateStr(bet.birthdate)
	return "B" + addpaddingToLenString(strconv.Itoa(len(str))) + str
}

func addNumberToMessage(bet Bet) string {
	str := truncateStr(bet.number)
	return "V" + addpaddingToLenString(strconv.Itoa(len(str))) + str
}

func generateBetMessage(bet Bet) string {
	msg := ""
	msg += addNameToMessage(bet)
	msg += addLastnameToMessage(bet)
	msg += addDocumentToMessage(bet)
	msg += addBirthdateToMessage(bet)
	msg += addNumberToMessage(bet)
	return msg
}

func generateMessage(batch []Bet, id string) ([]Bet, string) {
	message := id
	var betsLeft []Bet
	lastProcessedBet := len(batch)
	for i := 0; i < len(batch); i++ {
		betStr := generateBetMessage(batch[i])
		
		if len(message + betStr) > MSG_SIZE {
			lastProcessedBet = i
			break
		}
		message += betStr
	}

	for i := lastProcessedBet; i < len(batch); i++ {
		betsLeft = append(betsLeft, batch[i])
	}

	return  betsLeft, strings.Join([]string{message, strings.Repeat("X", MSG_SIZE - len(message))}, "")
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

func recvMessage(c *Client) string {
	msg, err := bufio.NewReader(c.conn).ReadString('\n')
	response := strings.Trim(msg, "\n")
	response = strings.Trim(response, "X")

	log.Infof("action: server_response_recieved | message: %s", response, )
	
	if err != nil {
		log.Errorf("action: server_message_received | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return ""
	
	} else if response == "ERROR" {
		log.Errorf("action: server_message_received | result: fail | error: error message received from server")
		return ""
	}
	log.Infof("action: server_message_received | result: success | message: %s", response)
	
	return response
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	time.Sleep( 8 * time.Second)
	id := os.Getenv("CLI_ID")
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

		// Create the connection the server in every loop iteration. Send an
		c.createClientSocket()

		done:= "DONE"
		done = strings.Join([]string{done, strings.Repeat("X", MSG_SIZE - len(done))}, "")
		
		log.Infof("action: waiting_ready | result: pending}",)

		response := recvMessage(c)
		
		if ( response != "READY" ) {
			return
		}

		log.Infof("action: ready_received | result: done}",)
		
		processBets(c, id)
						
		log.Infof("action: sending_donde | result: pending}",)
		fmt.Fprintf( c.conn, done,)
		log.Infof("action: donde_sent | result: sucess ", )

		response = recvMessage(c)

		if ( response == "" ) {
			return
		}

		c.conn.Close()
		
		log.Infof("action: batch_stored | result: success | message: %s}", response, )
		break loop
	}
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}