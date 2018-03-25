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
        
        print("Calculating variable: " + varName)
        print("Start time is: " + pd.to_datetime(str(data_handler.start)).strftime('%Y-%m-%d'))
        print("End time is: " + pd.to_datetime(str(data_handler.end)).strftime('%Y-%m-%d'))
        print("Step time is: " + data_handler.step)
    

        startIdx = np.where(data_handler.daysSinceVec==data_handler.start)[0]
        endIdx =  np.where(data_handler.daysSinceVec==data_handler.end)[0]
        stepCount = 0
        val = np.zeros(varToBeAnalysed[0].shape)
        vals = np.zeros(varToBeAnalysed[0].shape)
        stepRangeCount = 0
        seasonCounter = 0
        yearPeriodCounter = 0

        for i in np.arange(startIdx, endIdx, 1):

            currDate = data_handler.daysSinceVec[i]
            nextDate = data_handler.daysSinceVec[i+1]
            startDate = data_handler.daysSince

            if data_handler.step is "M":
                currTimeDelta = pd.to_datetime(date(currDate.year, currDate.month, 1)) - data_handler.daysSince
                nextTimeDelta = pd.to_datetime(date(nextDate.year, nextDate.month, 1)) - data_handler.daysSince

            elif data_handler.step is "Y" or data_handler.step is "3Y":
                currTimeDelta = pd.to_datetime(date(currDate.year, 1, 1)) - data_handler.daysSince
                nextTimeDelta = pd.to_datetime(date(nextDate.year, 1, 1)) - data_handler.daysSince
            elif data_handler.step is "S":
                currTimeDelta = pd.to_datetime(date(currDate.year, currDate.month, 1)) - data_handler.daysSince
                nextTimeDelta = pd.to_datetime(date(nextDate.year, nextDate.month, 1)) - data_handler.daysSince
                seasonBeginTimeDelta = pd.to_datetime(date(currDate.year, data_handler.season["start"], 1)) - data_handler.daysSince
                
                seasonStart = pd.to_datetime(date(data_handler.start.year+seasonCounter, data_handler.season["start"], 1))
                
                if data_handler.season["start"] > data_handler.season["end"]:
                    seasonEnd = pd.to_datetime(date(data_handler.start.year+1+seasonCounter, data_handler.season["end"], calendar.monthrange(data_handler.start.year+1+seasonCounter,data_handler.season["end"])[1]))
                else:
                    seasonEnd = pd.to_datetime(date(data_handler.start.year+seasonCounter, data_handler.season["end"], calendar.monthrange(data_handler.start.year+1+seasonCounter,data_handler.season["end"])[1]))
            else:
                raise Exception("Timestep option '" + data_handler.step + "' not exists.")


            func = funcToBeApplied 
            val = varToBeAnalysed[i]
            
            
            if data_handler.step is "M" or data_handler.step is "Y":
            
                if func == "sum" or func == "mean":
                     # If all values are masked it is assumed that it is missing data
                    if not np.ma.is_masked(np.nanmax(val)):
                        vals = vals + val # Agregate the data to the given step
                        stepCount = stepCount+1
                else:
                    raise Exception('Unknown statistics function:' + func) 
                
    
                # If new timestep begins (e.g. new month or year)
                if nextTimeDelta > currTimeDelta:
                    if func == "sum":
                        result = vals
                    elif func == "mean":
                        if stepCount != 0:
                            result = vals / stepCount
                        
                    vals = stepCount = 0
                    
                    # Write the result to the file    
                    data_handler.writeToOutputFile(varName, stepRangeCount, currDate, result)
                    stepRangeCount = stepRangeCount+1
                   
                    
            elif data_handler.step is "S":

                if currDate >= seasonStart and currDate <= seasonEnd:  
                    condition = True
                else: 
                    condition = False
                
                if func == "sum" or func == "mean":
                     # If all values are masked it is assumed that it is missing data
                    if not np.ma.is_masked(np.nanmax(val)) and condition:
                        
                        vals = vals + val # Agregate the data to the given step
                        stepCount = stepCount+1
                else:
                    raise Exception('Unknown statistics function:' + func) 
                
    
                # If new timestep begins (e.g. new month or year or season) calculate the statistics
                if currDate >= seasonEnd:
                    
                    if func == "sum":
                        result = vals
                    elif func == "mean":
                        if stepCount != 0:
                            result = vals / stepCount
                       
                    vals = stepCount = 0
                    
                    # Write the result to the file 

                    data_handler.writeToOutputFile(varName, stepRangeCount, currDate, result)
                    seasonCounter = seasonCounter+1
                    stepRangeCount = stepRangeCount+1
            
            
            elif data_handler.step is "3Y":
            
                if func == "sum" or func == "mean":
                     # If all values are masked it is assumed that it is missing data
                    if not np.ma.is_masked(np.nanmax(val)):
                        vals = vals + val # Agregate the data to the given step
                        stepCount = stepCount+1
                else:
                    raise Exception('Unknown statistics function:' + func) 
                
    
                # If new timestep begins (e.g. new month or year)
                if nextTimeDelta > currTimeDelta:
                    yearPeriodCounter = yearPeriodCounter + 1
                
                if yearPeriodCounter == 3: 
                    
                    if func == "sum":
                        result = vals
                    elif func == "mean":
                        if stepCount != 0:
                            result = vals / stepCount
                        
                    vals = stepCount = 0
                    
                    # Write the result to the file    
                    data_handler.writeToOutputFile(varName, stepRangeCount, currDate, result)
                    stepRangeCount = stepRangeCount+1
                    yearPeriodCounter = 0
            else:
                print("Attention! Unknown step: " + data_handler.step + ". Can not Calculate. Aborting ...") 
                return 0
            




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
