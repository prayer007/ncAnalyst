from nc_manager import *
from stats.stats_univariat import * 
from point_manager import *

#def main():

WORKING_DIR = "F:/SNOWMODEL_RUNS/run1_45_10_max_eq/"
O_FILE_NAME = "2000_2100_12_2_9_rcp45.nc"
    
nc_manager = NcManager(WORKING_DIR, "output_daily_rcp45.nc", O_FILE_NAME)
#TODO: Works only if the usertimespan is equal to the timespan of the path !! CHANGE !!
nc_manager.setTimeSpan("1980-01-01", "2100-12-31", period = [1,15,12,2,9])
point_manager = PointManager()
point_manager.createPoint(11,61)
point_manager.createPoint(16,41)
point_manager.createPoint(36,20)
stats_univariat = StatsUnivariat(nc_manager, point_manager, WORKING_DIR + O_FILE_NAME)
stats_univariat.setVariablesToAnalyse([ {"var": "asmh", "func": {"name": "mean", "props":[]}},
                                        {"var": "tas", "func": {"name": "mean", "props":[]}},
                                        {"var": "tasmax", "func": {"name": "mean", "props":[]}},
                                        {"var": "tasmin", "func": {"name": "mean", "props":[]}},
                                        {"var": "emos", "func": {"name": "mean", "props":[]}},
                                        {"var": "emos_nat", "func": {"name": "mean", "props":[]}},
                                        {"var": "pr", "func": {"name": "sum", "props":[]}},
                                        {"var": "paswe", "func": {"name": "sum", "props":[]}},
                                        {"var": "sdasmos", "func": {"name": "mean", "props":[]}},
                                        {"var": "sdnoasmos", "func": {"name": "mean", "props":[]}},
                                        {"var": "sweosasm", "func": {"name": "count", "props":[True, ">", 90]}},
                                        {"var": "sweosnoasm", "func": {"name": "count", "props":[True, ">", 90]}},
                                        {"var": "wacon", "func": {"name": "sum", "props":[]}},
                                        ])
                                        
start_time = time.time()

stats_univariat.calcAll()

print("--- %s seconds ---" % (time.time() - start_time))


# if __name__== "__main__":
#   main()
  
  
  
  