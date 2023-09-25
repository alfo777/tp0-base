import csv
import datetime
import time
import logging


""" Bets storage location. """
STORAGE_FILEPATH = "./bets.csv"
""" Simulated winner number in the lottery contest. """
LOTTERY_WINNER_NUMBER = 7574


""" A lottery bet registry. """
class Bet:
    def __init__(self, agency: str, first_name: str, last_name: str, document: str, birthdate: str, number: str):
        """
        agency must be passed with integer format.
        birthdate must be passed with format: 'YYYY-MM-DD'.
        number must be passed with integer format.
        """
        self.agency = int(agency)
        self.first_name = first_name
        self.last_name = last_name
        self.document = document
        self.birthdate = datetime.date.fromisoformat(birthdate)
        self.number = int(number)

"""this function process the bets sent by clients, if it success then returns client bet """
def process_bet( message: str) -> str:
    try:
        bets = parse_client_message(message)
        store_bets(bets)
        return "DONE"
    except Exception as e:
        logging.info(f'action: process_bet | result: fail | error: {e}')
        return None 


""" Checks whether a bet won the prize or not. """
def has_won(bet: Bet) -> bool:
    return bet.number == LOTTERY_WINNER_NUMBER

"""
Persist the information of each bet in the STORAGE_FILEPATH file.
Not thread-safe/process-safe.
"""
def store_bets(bets: list[Bet]) -> None:
    with open(STORAGE_FILEPATH, 'a+') as file:
        writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
        for bet in bets:
            writer.writerow([bet.agency, bet.first_name, bet.last_name,
                             bet.document, bet.birthdate, bet.number])

"""
Loads the information all the bets in the STORAGE_FILEPATH file.
Not thread-safe/process-safe.
"""
def load_bets() -> list[Bet]:
    with open(STORAGE_FILEPATH, 'r') as file:
        reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            yield Bet(row[0], row[1], row[2], row[3], row[4], row[5])

"""
Recieves a message that the client sends to the server and parse it into a bet
"""
def parse_client_message(message: str) -> Bet:
    i = 0
    bets = []
    agency = ""
    first_name = ""
    last_name = ""
    document = ""
    birthdate = ""
    number = ""

    while ( i < len(message) ):
        if ( i == 0 ):
            agency = message[0]
            i += 1

        field = message[i]
        str_len = int(message[i + 1] + message[i + 2] )
        i += 3

        if ( field == "N" ):
            first_name = message[i: i + str_len ]

        elif ( field == "L" ):
            last_name = message[i: i + str_len ]

        elif ( field == "D" ):
            document = message[i: i + str_len ]

        elif ( field == "B" ):
            birthdate = message[i: i + str_len ]

        elif ( field == "V" ):
            number = message[i: i + str_len ]
            bets.append(Bet(agency, first_name, last_name, document, birthdate, number))
            

        i += str_len
    
    return bets

""" A result with the list of winner fo each agency. """
class LoteryResult:
     
    def __init__(self):
        """the dict saves pair (agendy_id, winner_list)"""
        self._winners = {}


    def load_winner(self,winner ):
        """store a winner in the dict"""

        if self._winners.get(winner.agency) == None :
            self._winners[winner.agency] = []

        self._winners[winner.agency].append(winner.document)

    def get_agency_winners(self, agency):
        if self._winners.get(agency) == None :
            return []
        return self._winners[agency]

"""generate the result of the lotery """
def do_lottery():
    result = LoteryResult() 
    bets = load_bets()
    for bet in bets:
        if has_won(bet):
            result.load_winner(bet)

    return result

"""generate winners message for a agency"""
def generate_winners_message(winners):
    return addPaddingToStrLen(str(len(winners))) + winners

def addPaddingToStrLen(string):
    if ( len(string) == 1 ):
        return "0" + string
    return string