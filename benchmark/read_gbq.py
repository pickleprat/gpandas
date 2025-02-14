import time
from pandas_gbq import read_gbq
import os
from dotenv import load_dotenv
import certifi

def main():
    load_dotenv() 
    table_id = os.getenv("TABLEID")
    start = time.time()
    
    query = "SELECT * FROM " + table_id
    df = read_gbq(query, "jm-ebg")
    end = time.time() - start
    print(df)
    
    print(f"{end:.6f}")

if __name__ == "__main__":
    main()
