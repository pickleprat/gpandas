import pandas as pd
import time

def main():
    start = time.perf_counter()
    df = pd.read_csv('C:/Users/ShivanandanaSh/Documents/projects/gpandas/benchmark/customers-2000000.csv')
    end = time.perf_counter()
    print(end-start)

if __name__ == '__main__':
    main()