import numpy as np
import pandas as pd
from datetime import *
import calendar
from csv_manager import *
from point_manager import *


class StatsUnivariat(object):    
    '''
    This class handles the univariat statistical 
    analysis. 
    '''
    def __init__(self, nc_manager, point_manager):
        '''        
        Parameters
        ----------
        nc_manager : nc_manager.NcManager
            The manager for the src and dst ncfile
        point_manager : point_manager.PointManager
            Handler for the points
        '''
        self.nc_manager = nc_manager
        self.point_manager = point_manager
        self.varsToBeAnalysed = []


    def __calc(self, varName, varToBeAnalysed, funcToBeApplied, csv):
        '''        
        Calculates the statistics iteratively for each
        period. The shorter the time range the greater
        the number of iterations.
        
        Parameters
        ----------
        varName : string
            Variable name
        varToBeAnalysed : netCDF4._netCDF4.Variable
            Raw nc variable
        funcToBeApplied : string
            The univariate function name
        csv : csv_manager.Csv
            Manages the csv output             
        '''        
        nc_manager = self.nc_manager
     
        if not nc_manager.customTimeFlag:
            warnings.warn("Attention! There is no timespan set, so default timespan is used. On big data that may leed to long execution times. Set the timespan with DataAnalysis().setTimeSpan(start, end, step)", UserWarning)
        
        # print("Calculating variable: " + varName)
        # print("Start time is: " + pd.to_datetime(str(nc_manager.start)).strftime('%Y-%m-%d'))
        # print("End time is: " + pd.to_datetime(str(nc_manager.end)).strftime('%Y-%m-%d'))
        # print("Step time is: " + nc_manager.step)
    
        for i, period in enumerate(nc_manager.spanStartSpanEnd):
            
            periodStartIdx = nc_manager.sourceDatesIdx[period["startDate"]]
            periodEndIdx = nc_manager.sourceDatesIdx[period["endDate"]]

            boolArr = nc_manager.boolDateVec[periodStartIdx:periodEndIdx]
            data = varToBeAnalysed[periodStartIdx:periodEndIdx][boolArr]
            
            timestepStr = str(period["startDate"]) + " - " + str(period["endDate"]) # Timestep string for .csv output
            
            if funcToBeApplied == "sum":
                result = np.sum(data, axis = 0)
            elif funcToBeApplied == "mean":
                result = np.mean(data, axis = 0)
            else:
                raise ValueError("Function '" + funcToBeApplied + "' to be applied on variable '" + varName + "' not known.")
            
            for point in self.point_manager.pointsContainer:
                val = point.extractPtVal(result)
                csv.collectValues(varName, val, point)
                
            nc_manager.writeToOutputFile(varName, i, result)
            
            
    def calcAll(self):
        '''        
        Calls the calculating function iteratively 
        for every variable and writes the results
        to csv and ncfile.
        '''        
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
        '''        
        Stores the variables to be analyses and
        its corresponding statistic function
        
        Parameters
        ----------
        vars_ : ndarray
            Array of dicts with varname and statics
            function to be applied.
        '''            
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
