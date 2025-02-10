import pandas as pd
import time
from pandas_gbq import read_gbq

def main():
    start = time.time()
    
    query = "SELECT * FROM `jm-ebg.Brokerage2024.RevenueNew`"
    df = read_gbq(query, "jm-ebg")
    end = time.time() - start
    print(df)
    
    
    print(f"{end:.6f}")

if __name__ == "__main__":
    main()
