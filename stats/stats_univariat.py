import numpy as np
import pandas as pd
from datetime import *
import calendar
from csv_manager import *
from point_manager import *
import pickle

class StatsUnivariat(object):    
    '''
    This class handles the univariat statistical 
    analysis. 
    '''
    def __init__(self, nc_manager, point_manager, ofPath):
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
        self.ofPath = self.setOfPath(ofPath)


    def setOfPath(self, path):
        self.nc_manager.outputPath = path #Change the default path in the nc_manager
        self.ofPath = path
        
        return path
        

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
        
        print("Calculating variable: " + varName)

        for i, period in enumerate(nc_manager.spanStartSpanEnd):
            
            periodStartIdx = nc_manager.sourceDatesIdx[period["startDate"]]
            periodEndIdx = nc_manager.sourceDatesIdx[period["endDate"]]

            boolArr = nc_manager.boolDateVec[periodStartIdx:periodEndIdx]
            data = varToBeAnalysed[periodStartIdx:periodEndIdx][boolArr]
            
            timestepStr = str(period["startDate"]) + " - " + str(period["endDate"]) # Timestep string for .csv output
            
            funcName = funcToBeApplied["name"] 
            funcProps = funcToBeApplied["props"]
            
            if funcName == "count":
                result = self.calcCount(funcToBeApplied, data)
            elif funcName == "sum":
                result = self.calcSum(funcToBeApplied, data)
            elif funcName == "mean":
                result = self.calcMean(funcToBeApplied, data)
            else:
                raise ValueError("Function '" + funcToBeApplied + "' to be applied on variable '" + varName + "' not known.")
            
            for point in self.point_manager.pointsContainer:
                val = point.extractPtVal(result)
                csv.collectValues(varName, val, point)
            
            if funcProps:
                ncVarName = varName + "_[" + funcName + "_" + funcProps[1] + "_" + str(funcProps[2]) + "]"
            else:
                ncVarName = varName + "_[" + funcName + "]"
                
            nc_manager.writeToOutputFile(ncVarName, i, result)


    def calcAll(self):
        '''        
        Calls the calculating function iteratively 
        for every variable and writes the results
        to csv and ncfile.
        '''        
        varsToBeAnalysed = self.varsToBeAnalysed 
        
        if not varsToBeAnalysed:
            print('There is no Variable to analyse.')
        
        csv = CsvManager(self.nc_manager.workingDir, self.nc_manager.spanStartSpanEnd, varsToBeAnalysed, self.point_manager.pointsContainer)
        
        for var in varsToBeAnalysed:
            varName = var["var"]
            
            if varName != "skip":
                data = var["data"]
                func = var["func"]   
                if self.__calc(varName, data, func, csv) == 0:
                    return 0

        csv.writeDataToFile()
        self.nc_manager.dst.close()


    def calcCount(self, func, data):
        '''        
        Counts values in the defined timespan for
        a specific condition (E.g. All temps > 5
        for winter).
        
        Parameters
        ----------
        func : dict
            Dict with information about the statistical 
            function to be applied like name and properties
        data : ndarray
            Three dimensional array with the dataframes
            to be counted
            
        Returns
        ----------
        count : ndarray
            Array with the counted appearances
        '''
        resData = data.copy()
                
        if not func["props"]:
            resData[:] = 1
        elif func["props"][0] is True:
            try:
                sign = func["props"][1]
            except:
                raise ValueError("Sign not set. Leave function properties empty or choose a sign (<,>,<=,>=) and value") 
    
            try:
                val = func["props"][2]
            except:
                raise ValueError("No value set. Leave function properties empty or choose a value") 
                
            if sign == '<':
                resData[data < val] = 1
                resData[data >= val] = 0
            elif sign == '>':
                resData[data > val] = 1
                resData[data <= val] = 0
            elif sign == '<=':
                resData[data <= val] = 1
                resData[data > val] = 0
            elif sign == '>=':
                resData[data >= val] = 1
                resData[data < val] = 0
            else:
                raise ValueError("Wrong sign chosen. Available sign are (<,>,<=,>=)") 
        
        count = np.sum(resData, axis = 0)
            
        return count


    def calcSum(self, func, data):
        '''        
        Calculates the sum of the data. Either for all 
        values in the dataset or only for specific 
        values. Not used values are set to nan. 
        
        Parameters
        ----------
        func : dict
            Dict with information about the statistical 
            function to be applied like name and properties
        data : ndarray
            Three dimensional array with the dataframes
            to be counted
            
        Returns
        ----------
        sum_ : ndarray
            Array with the appearances summed up
        '''
        resData = data.copy()
        
        if not func["props"]:
            pass 
        elif func["props"][0] is True:
            try:
                sign = func["props"][1]
            except:
                raise ValueError("Sign not set. Leave function properties empty or choose a sign (<,>,<=,>=) and value") 
    
            try:
                val = func["props"][2]
            except:
                raise ValueError("No value set. Leave function properties empty or choose a value") 
                
            if sign == '<':
                resData[data >= val] = np.nan
            elif sign == '>':
                resData[data <= val] =  np.nan
            elif sign == '<=':
                resData[data > val] =  np.nan
            elif sign == '>=':
                resData[data < val] =  np.nan
            else:
                raise ValueError("Wrong sign chosen. Available sign are (<,>,<=,>=)") 
        
        sum_ = np.sum(resData, axis = 0)
            
        return sum_

 
    def calcMean(self, func, data):
        '''        
        Calculates the mean of the data. Either for all 
        values in the dataset or only for specific 
        values. Not used values are set to nan. 
        
        Parameters
        ----------
        func : dict
            Dict with information about the statistical 
            function to be applied like name and properties
        data : ndarray
            Three dimensional array with the dataframes
            to be counted
            
        Returns
        ----------
        sum_ : ndarray
            Array with the appearances summed up
        '''        
        resData = data.copy()
        
        if not func["props"]:
            pass
        elif func["props"][0] is True:
            try:
                sign = func["props"][1]
            except:
                raise ValueError("Sign not set. Leave function properties empty or choose a sign (<,>,<=,>=) and value") 
    
            try:
                val = func["props"][2]
            except:
                raise ValueError("No value set. Leave function properties empty or choose a value") 
                
            if sign == '<':
                resData[data >= val] = np.nan
            elif sign == '>':
                resData[data <= val] =  np.nan
            elif sign == '<=':
                resData[data > val] =  np.nan
            elif sign == '>=':
                resData[data < val] =  np.nan
            else:
                raise ValueError("Wrong sign chosen. Available sign are (<,>,<=,>=)") 
        
        mean_ = np.mean(resData, axis = 0)
            
        return mean_


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
        
        self.varsToBeAnalysed = vars_
        
        for i in vars_:
            try:
                i["data"] = nc_manager.src.variables[i["var"]]
                self.varShape = i["data"][0].shape

            except KeyError as e: 
                print("Variable '" + i["var"] + "' not found in datafile. Continues with next variable...")
                i["var"] = "skip"
        
        nc_manager.initializeOutputFile(self.ofPath, vars_)
