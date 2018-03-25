from data_handler import *
from stats.stats_univariat import * 

def main():
    
    data_handler = DataHandler("F:/SNOWMODEL_RUNS/run1_45_10_max_eq/output_daily.nc", "F:/SNOWMODEL_RUNS/run1_45_10_max_eq/stats.nc")
    data_handler.setTimeSpan("2000-01-01", "2005-12-31", "S", season = [10,4])
    stats_univariat = StatsUnivariat(data_handler)
    stats_univariat.setVariablesToAnalyse([{"var": "asmh", "func": "sum"},{"var": "tas", "func": "mean"}])
    stats_univariat.calcAll()
  
if __name__== "__main__":
  main()
  
  