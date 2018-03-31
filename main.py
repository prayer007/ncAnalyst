from nc_manager import *
from stats.stats_univariat import * 
from point_manager import *

#def main():
    
nc_manager = NcManager("F:/SNOWMODEL_RUNS/run1_45_10_max_eq/output_daily.nc", "F:/SNOWMODEL_RUNS/run1_45_10_max_eq/2000_2100_11_3_5.nc")
nc_manager.setTimeSpan("2000-01-01", "2100-12-31", period = [1,15,11,3,5])
point_manager = PointManager()
point_manager.createPoint(11,61)
point_manager.createPoint(36,20)
stats_univariat = StatsUnivariat(nc_manager, point_manager)
stats_univariat.setVariablesToAnalyse([{"var": "asmh", "func": "sum"},{"var": "tas", "func": "mean"}])
stats_univariat.calcAll()



# if __name__== "__main__":
#   main()
  
  
  
  