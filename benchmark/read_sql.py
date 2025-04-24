import mysql.connector
import dotenv
import os 


dotenv.load_dotenv(override=True) 


MYSQL_HOST      : str | None = os.getenv("MYSQL_HOST") 
MYSQL_USERNAME  : str | None = os.getenv("MYSQL_USERNAME")  
MYSQL_PASSWORD  : str | None = os.getenv("MYSQL_PASSWORD") 
MYSQL_

def main() -> None: 
    customers = mysql.connector.connect(
    ) 
    

