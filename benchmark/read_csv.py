import pandas as pd
import time

def main():
    start = time.perf_counter()
    df = pd.read_csv('./customers-2000000.csv')
    end = time.perf_counter()
    print(end-start)

if __name__ == '__main__':
    main()