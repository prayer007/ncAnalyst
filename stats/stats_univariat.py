import numpy as np
import pandas as pd
from datetime import *
import calendar
from csv_manager import *
from point_manager import *


class StatsUnivariat(object):    


    def __init__(self, nc_manager, point_manager):
        self.nc_manager = nc_manager
        self.point_manager = point_manager
        self.varsToBeAnalysed = []


    def __calc(self, varName, varToBeAnalysed, funcToBeApplied, csv):
        
        nc_manager = self.nc_manager
     
        if not nc_manager.customTimeFlag:
            warnings.warn("Attention! There is no timespan set, so default timespan is used. On big data that may leed to long execution times. Set the timespan with DataAnalysis().setTimeSpan(start, end, step)", UserWarning)
        
        # print("Calculating variable: " + varName)
        # print("Start time is: " + pd.to_datetime(str(nc_manager.start)).strftime('%Y-%m-%d'))
        # print("End time is: " + pd.to_datetime(str(nc_manager.end)).strftime('%Y-%m-%d'))
        # print("Step time is: " + nc_manager.step)
    

        for i, period in enumerate(nc_manager.spanStartSpanEnd):
            
            periodStartIdx = int(np.where(nc_manager.sourceDates==period["startDate"])[0])
            periodEndIdx = int(np.where(nc_manager.sourceDates==period["endDate"])[0])
            
            
            data = varToBeAnalysed[periodStartIdx:periodEndIdx]
            timestepStr = str(period["startDate"]) + " - " + str(period["endDate"]) # Timestep string for .csv output
            
            if funcToBeApplied == "sum":
                result = np.sum(data, axis = 0)
            elif funcToBeApplied == "mean":
                result = np.mean(data, axis = 0)
            
            for point in self.point_manager.pointsContainer:
                val = point.extractPtVal(result)
                csv.collectValues(varName, val, point)
                
            nc_manager.writeToOutputFile(varName, i, result)
            




    def calcAll(self):
        
        varsToBeAnalysed = self.varsToBeAnalysed 
        
        if not varsToBeAnalysed:
            print('There is no Variable to analyse.')
        
        csv = CsvManager("F:/SNOWMODEL_RUNS/run1_45_10_max_eq/", self.nc_manager.spanStartSpanEnd, varsToBeAnalysed, self.point_manager.pointsContainer)
        
        for var in varsToBeAnalysed:
            varName = var["var"]
            
            if varName != "skip":
                data = var["data"]
                func = var["func"]   
                if self.__calc(varName, data, func, csv) == 0:
                    return 0

        csv.writeDataToFile()
        self.nc_manager.dst.close()




    def setVariablesToAnalyse(self, vars_):
        
        # Appends the netCDF src variable. 
        nc_manager = self.nc_manager
        
        for i in vars_:
            try:
                i["data"] = nc_manager.src.variables[i["var"]]
                self.varsToBeAnalysed = vars_ 
                self.varShape = i["data"][0].shape
                nc_manager.varNamesToBeAnalysed = np.append(nc_manager.varNamesToBeAnalysed, i["var"])

            except KeyError as e: 
                print("Variable '" + i["var"] + "' not found in datafile. Continues with next variable...")
                i["var"] = "skip"
        
        nc_manager.initializeOutputFile(nc_manager.outputPath)
