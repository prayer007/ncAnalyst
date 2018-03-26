import numpy as np
import pandas as pd
from datetime import *
import calendar


class StatsUnivariat(object):    


    def __init__(self, data_handler):
        self.data_handler = data_handler    
        self.varsToBeAnalysed = []


    def __calc(self, varName, varToBeAnalysed, funcToBeApplied):
        
        data_handler = self.data_handler
     
        if not data_handler.customTimeFlag:
            warnings.warn("Attention! There is no timespan set, so default timespan is used. On big data that may leed to long execution times. Set the timespan with DataAnalysis().setTimeSpan(start, end, step)", UserWarning)
        
        # print("Calculating variable: " + varName)
        # print("Start time is: " + pd.to_datetime(str(data_handler.start)).strftime('%Y-%m-%d'))
        # print("End time is: " + pd.to_datetime(str(data_handler.end)).strftime('%Y-%m-%d'))
        # print("Step time is: " + data_handler.step)
    

        for period in  data_handler.spanStartSpanEnd:
            periodStartIdx = int(np.where(data_handler.daysSinceVec==period["startDate"])[0])
            periodEndIdx = int(np.where(data_handler.daysSinceVec==period["endDate"])[0])
            
            data = varToBeAnalysed[periodStartIdx:periodEndIdx]
            
            if funcToBeApplied == "sum":
                result = np.nansum(data, axis = 0)
            elif funcToBeApplied == "mean":
                result = np.nanmean(data, axis = 0)
            
            print result
            




    def calcAll(self):
        
        varsToBeAnalysed = self.varsToBeAnalysed 
        
        if not varsToBeAnalysed:
            print('There is no Variable to analyse.')
        
        for var in varsToBeAnalysed:
            varName = var["var"]
            
            if varName != "skip":
                data = var["data"]
                func = var["func"]   
                if self.__calc(varName, data, func) == 0:
                    return 0

        self.data_handler.dst.close()




    def setVariablesToAnalyse(self, vars_):
        
        # Appends the netCDF src variable. 
        data_handler = self.data_handler
        
        for i in vars_:
            try:
                i["data"] = data_handler.src.variables[i["var"]]
                self.varsToBeAnalysed = vars_ 
                self.varShape = i["data"][0].shape
                data_handler.varNamesToBeAnalysed = np.append(data_handler.varNamesToBeAnalysed, i["var"])

            except KeyError as e: 
                print("Variable '" + i["var"] + "' not found in datafile. Continues with next variable...")
                i["var"] = "skip"
        
        data_handler.initializeOutputFile(data_handler.outputPath)
