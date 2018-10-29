from nc_manager import *
from stats.stats_univariat import * 
from point_manager import *

#def main():

months = [11,12,1,2,3,4]

for m in months:
    startMonth = 3
    endMonth = 3
    timespan = 9
    rcp  = 45
    
    WORKING_DIR = "G:/SNOWMODEL_RUNS/run1_"+ str(rcp) +"_10_max_eq/"
    O_FILE_NAME = "2000_2100_"+ str(startMonth) + "_" + str(endMonth) + "_" + str(timespan) + "_rcp" + str(rcp)  + ".nc"
        
    nc_manager = NcManager(WORKING_DIR, "output_daily_rcp" + str(rcp) + ".nc", O_FILE_NAME)
    #TODO: Works only if the usertimespan is equal to the timespan of the path !! CHANGE !!
    nc_manager.setTimeSpan("1980-01-01", "2100-12-31", period = [None,None,startMonth,endMonth,timespan])
    point_manager = PointManager()
    point_manager.createPoint(11,61)
    point_manager.createPoint(16,41)
    point_manager.createPoint(36,20)
    
    stats_univariat = StatsUnivariat(nc_manager, point_manager, WORKING_DIR + O_FILE_NAME)
    stats_univariat.setFilename("_" + str(startMonth) + "_" + str(endMonth) + "_" + str(timespan))
    stats_univariat.setVariablesToAnalyse([ {"var": "asmh", "func": {"name": "mean", "props":[]}},
                                            #{"var": "tas", "func": {"name": "mean", "props":[]}},
                                            #{"var": "tasmax", "func": {"name": "mean", "props":[]}},
                                            #{"var": "tasmin", "func": {"name": "mean", "props":[]}},
                                            #{"var": "emos", "func": {"name": "mean", "props":[]}},
                                            #{"var": "emos_nat", "func": {"name": "mean", "props":[]}},
                                            #{"var": "pr", "func": {"name": "sum", "props":[]}},
                                            #{"var": "paswe", "func": {"name": "sum", "props":[]}},
                                            #{"var": "sdasmos", "func": {"name": "mean", "props":[]}},
                                            #{"var": "sdnoasmos", "func": {"name": "mean", "props":[]}},
                                            #{"var": "sweosasm", "func": {"name": "count", "props":[True, ">", 90]}},
                                            #{"var": "sweosnoasm", "func": {"name": "count", "props":[True, ">", 90]}},
                                            #{"var": "wacon", "func": {"name": "sum", "props":[]}},
                                            ])
                                            
                                    
                                            
    start_time = time.time()
    
    stats_univariat.calcAll()

print("--- %s seconds ---" % (time.time() - start_time))


# if __name__== "__main__":
#   main()
  
  
  
  