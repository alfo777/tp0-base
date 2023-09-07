package common

import (
	"bufio"
	"fmt"
	"net"
	"time"
	"os"
	"strconv"
	"strings"
	"encoding/csv"
	"unicode/utf8"
	log "github.com/sirupsen/logrus"
)

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

func readFile(id string) [][]string {
	f, err := os.Open("agency-" + id + ".csv")
    if err != nil {
		log.Errorf("action: read_agency_file | result: fail | error: %s", err,)
		return nil
	}

    // remember to close the file at the end of the program
    defer f.Close()

    // read csv values using csv.Reader
    csvReader := csv.NewReader(f)
    data, err := csvReader.ReadAll()
    if err != nil {
		log.Errorf("action: read_agency_file | result: fail | error: %s", err,)
        return nil
    }
	return data
}

func createBetList(data [][]string, agency string) []Bet {
	var bets []Bet
    for i, line := range data {
        if i > 0 { 
			var bet Bet
			bet.agency = agency
			for j, field := range line {
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
            bets = append(bets, bet)
        }
    }
    return bets
}

func addpaddingToLenString(str string) string {
	if len(str) == 1 {
		return "0" + str
	}
	return str
}


func addNameToMessage(bet Bet) string {
	return "N" + addpaddingToLenString(strconv.Itoa(utf8.RuneCountInString(bet.name))) + bet.name
}

func addLastnameToMessage(bet Bet) string {
	return "L" + addpaddingToLenString(strconv.Itoa(utf8.RuneCountInString(bet.lastname))) + bet.lastname
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
	msg := ""
	msg += addNameToMessage(bet)
	msg += addLastnameToMessage(bet)
	msg += addDocumentToMessage(bet)
	msg += addBirthdateToMessage(bet)
	msg += addNumberToMessage(bet)
	return msg
}

func generateMessage(bets []Bet, msgN int, id string) string {
	betsToSend := 0
	j := BATCH_SIZE * msgN
	message := id
	for true {
		if betsToSend >= BATCH_SIZE || j >= len(bets) {
			break
		} else if betsToSend < BATCH_SIZE && j < len(bets) {
			message += generateBetMessage(bets[j])
			betsToSend++
		}
		j++
	}
	return  strings.Join([]string{message, strings.Repeat("X", 8192-len(message))}, "")
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
		log.Errorf("action: server_message_received | result: fail | error: error ocurred while trying to store bet")
		return ""
	}
	log.Infof("action: server_message_received | result: success | message: %s", response)
	
	return response
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	
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
		//generate client bets
		records := readFile(c.config.ID)

		if records == nil {
			return
		}
		bets := createBetList(records, c.config.ID)

		// Create the connection the server in every loop iteration. Send an
		c.createClientSocket()

		done:= "DONE"
		done = strings.Join([]string{done, strings.Repeat("X", 8192-len(done))}, "")
		
		response := recvMessage(c)
		nMsg := 0

		if ( response == "" ) {
			return
		}
		for true {
			sendMessage := generateMessage(bets, nMsg, c.config.ID)
			if sendMessage[0:2] == ( c.config.ID + "X" ) {
				break
			}
			log.Infof("action: sending_batch | result: pending | batch_number: %v}", nMsg, )
			fmt.Fprintf(
				c.conn,
				sendMessage,
			)
			nMsg += 1
			log.Infof("action: batch_sent | result: sucess | batch_number: %v}", nMsg, )
		}
						
		log.Infof("action: sending_donde | result: pending}",)
		fmt.Fprintf(
			c.conn,
			done,
		)
			
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