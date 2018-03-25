import numpy as np
import pandas as pd
from netCDF4 import Dataset
import warnings
from helpers import *
from datetime import *
import calendar






class DataAnalysis(object):
    
    '''
    Class for running a leave-one-out cross validation of simple
    geographically weighted regression models of station monthly and annual normals 
    to determine if a station is an outlier and has possible erroneous values 
    based on unrealistic model error.
    '''
    
    def __init__(self, ncPath, outputPath):
        
        '''        
        Parameters
        ----------
        stnda : twx.db.StationSerialDataDb
            A StationSerialDataDb object pointing to the
            database from which observations will be loaded.
        '''
        
        self.src = None 
        self.dest = None 
        self.outputPath = None
        
        self.setData(ncPath)
        self.setOutputPath(outputPath)
        self.daysSince = self.__setDaysSince(self.src.variables["time"])
        self.daysSinceVec = self.__setDaysSinceVec(self.src.variables["time"][:])
        self.yearsSinceVec = self.daysSinceVec.year.unique()
        self.monthsSince = self.__setMonthSince()
        self.yearsSince = self.__setYearsSince()
        self.varsToBeAnalysed = []
        self.varNamesToBeAnalysed = []
        self.varShape = None # Will be set on .setVariableToAnalyse()
        self.start = self.daysSinceVec[0]
        self.startTimDelta = self.start - self.daysSince
        self.end = self.daysSinceVec[-1]
        self.endTimDelta = self.end - self.daysSince
        self.step = "M"
        self.customTimeFlag = False
        
        
    
    
    def setData(self, ncPath):
        
        try:
            self.src = Dataset(ncPath, mode="r", format="NETCDF4")
        except IOError as (errno, strerror):
            print "Could not open netCDF file. I/O error({0}): {1}".format(errno, strerror)
        except:
            print "Could not open netCDF file. Unexpected error:", sys.exc_info()[0]
            raise



    def __initializeOutputFile(self, outputPath):

        try:
            self.dst = Dataset(outputPath, 'w', format="NETCDF4")
        except IOError as (errno, strerror):
            print "Could not open netCDF file. NO OUTPUT CREATED. I/O error({0}): {1}".format(errno, strerror)
        except:
            print "Could not open netCDF file. Unexpected error:", sys.exc_info()[0]
            raise
        
        # Create dimensions
        for name, dimension in self.src.dimensions.iteritems():
            self.dst.createDimension(name, len(dimension) if not dimension.isunlimited() else None)
            

        # Add variables
        
        if self.step is "M":
            tunits = "months since " + str(self.daysSince)  
        elif self.step is "Y" or self.step is "S":
            tunits = "years since " + str(self.daysSince)
        else: 
            tunits = "months since " + str(self.daysSince) 
            
        time = self.dst.createVariable(varname = 'time', datatype = 'i', dimensions = ('time'))  
        time.setncatts({k: self.src.variables["time"].getncattr(k) for k in self.src.variables["time"].ncattrs()})
        time.units = tunits
        
        varNameContainer = self.varNamesToBeAnalysed
  
        for name, variable in self.src.variables.iteritems():
        
            xDim = "x"
            yDim = "y"
        
            # take out the variable you don't want
            if name not in varNameContainer and name != xDim and name != yDim: 
                continue
            
            varOut = self.dst.createVariable(name, variable.datatype, variable.dimensions)
            
            if name == xDim or name == yDim:
                self.dst.variables[name][:] = self.src.variables[name][:]  
            
            # Set variable attributes  
            varOut.setncatts({k: variable.getncattr(k) for k in variable.ncattrs()})        
            
            

    def __writeToOutputFile(self, varName, stepIncr, date, data):
        

        
        if self.step is "M":
            startStep = self.monthsSince 
        elif self.step is "Y":
            startStep = self.yearsSince
        elif self.step is "S": 
            startStep = self.yearsSince
            stepIncr = np.where(self.yearsSinceVec==date.year)[0]
        

        currStep = startStep + stepIncr
        
        self.dst.variables["time"][stepIncr] = currStep
        self.dst.variables[varName][stepIncr,:,:] = data
        
        
        
        
            

    def __setDaysSince(self, dateTimeString):
        
        dts = dateTimeString.units.split()[2:][0]
        dateTime =  pd.to_datetime(dts) 
        
        
        return dateTime



    def __setMonthSince(self):
        
        
        date1 = self.daysSince
        date2 = self.daysSinceVec[0]
        
        monthsSince = (date2.year - date1.year) * 12 + (date2.month - date1.month)

        return monthsSince 


    def __setYearsSince(self):
        
        
        date1 = self.daysSince
        date2 = self.daysSinceVec[0]
        
        yearsSince = date2.year - date1.year

        return yearsSince 


    def __setDaysSinceVec(self, dateTimeStringVec):
        
        vfunc = np.vectorize(pd.to_timedelta)
        
        dateTimeVec = vfunc(dateTimeStringVec, "D") 
        
        
        dateTimeVec = self.daysSince +  dateTimeVec
        dateTimeVec = pd.to_datetime(dateTimeVec)
        
        return dateTimeVec





    def setOutputPath(self, op):
        self.outputPath = op


     
     
    def setVariablesToAnalyse(self, vars_):
        
        # Appends the netCDF src variable. 
        
        for i in vars_:
            try:
                i["data"] = self.src.variables[i["var"]]
                self.varsToBeAnalysed = vars_ 
                self.varShape = i["data"][0].shape
                self.varNamesToBeAnalysed = np.append(self.varNamesToBeAnalysed, i["var"])

            except KeyError as e: 
                print("Variable '" + i["var"] + "' not found in datafile. Continues with next variable...")
                i["var"] = "skip"
        
        self.__initializeOutputFile(self.outputPath)




    def setTimeSpan(self, start, end, step, **kwargs):
        
        self.customTimeFlag = True
        
        if step is "S":
            
            try: 
                s = kwargs["season"]
                self.season = {"start": s[0], "end": s[1]}
            except KeyError:
                self.season = {"start": 12, "end": 3}
                print "Parameter 'season' not set. Continuing with default 12-3"
            
        
        self.start = pd.to_datetime(start)
        self.end = pd.to_datetime(end)
        self.step = step
    
        if self.end > self.daysSinceVec[-1]:
            raise Exception("End time: " + str(self.end) + " is out of bounds. Bound end is " + str(self.daysSinceVec[-1]))
        
        if self.start < self.daysSinceVec[0]:
            raise Exception("Start time: " + str(self.start) + " is out of bounds. Bound start is " + str(self.daysSinceVec[0]))
        



    def __calc(self, varName, varToBeAnalysed, funcToBeApplied):
        
     
        if not self.customTimeFlag:
            warnings.warn("Attention! There is no timespan set, so default timespan is used. On big data that may leed to long execution times. Set the timespan with DataAnalysis().setTimeSpan(start, end, step)", UserWarning)
        
        print("Calculating variable: " + varName)
        print("Start time is: " + pd.to_datetime(str(self.start)).strftime('%Y-%m-%d'))
        print("End time is: " + pd.to_datetime(str(self.end)).strftime('%Y-%m-%d'))
        print("Step time is: " + self.step)
    


        startIdx = np.where(self.daysSinceVec==self.start)[0]
        endIdx =  np.where(self.daysSinceVec==self.end)[0]
        stepCount = 0
        val = np.zeros(varToBeAnalysed[0].shape)
        vals = np.zeros(varToBeAnalysed[0].shape)
        stepRangeCount = 0
        k = 0

        for i in np.arange(startIdx, endIdx, 1):

            currDate = self.daysSinceVec[i]
            nextDate = self.daysSinceVec[i+1]
            startDate = self.daysSince

            if self.step is "M":
                currTimeDelta = pd.to_datetime(date(currDate.year, currDate.month, 1)) - self.daysSince
                nextTimeDelta = pd.to_datetime(date(nextDate.year, nextDate.month, 1)) - self.daysSince

            elif self.step is "Y":
                currTimeDelta = pd.to_datetime(date(currDate.year, 1, 1)) - self.daysSince
                nextTimeDelta = pd.to_datetime(date(nextDate.year, 1, 1)) - self.daysSince
            elif self.step is "S":
                currTimeDelta = pd.to_datetime(date(currDate.year, currDate.month, 1)) - self.daysSince
                nextTimeDelta = pd.to_datetime(date(nextDate.year, nextDate.month, 1)) - self.daysSince
                seasonBeginTimeDelta = pd.to_datetime(date(currDate.year, self.season["start"], 1)) - self.daysSince
                
                seasonStart = pd.to_datetime(date(self.start.year+k, self.season["start"], 1))
                
                if self.season["start"] > self.season["end"]:
                    seasonEnd = pd.to_datetime(date(self.start.year+1+k, self.season["end"], calendar.monthrange(self.start.year+1+k,self.season["end"])[1]))
                else:
                    seasonEnd = pd.to_datetime(date(self.start.year+k, self.season["end"], calendar.monthrange(self.start.year+1+k,self.season["end"])[1]))
            else:
                currTimeDelta = pd.to_datetime(date(currDate.year, currDate.month, 1)) - self.daysSince
                nextTimeDelta = pd.to_datetime(date(nextDate.year, nextDate.month, 1)) - self.daysSince


            func = funcToBeApplied 
            val = varToBeAnalysed[i]
            
            if self.step is "M" or self.step is "Y":
            
                
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
                    self.__writeToOutputFile(varName, stepRangeCount, currDate, result)
                    stepRangeCount = stepRangeCount+1
                   
                    
            elif self.step is "S":

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

                    self.__writeToOutputFile(varName, stepRangeCount, currDate, result)
                    k = k+1
                    stepRangeCount = stepRangeCount+1
                    
            else:
                print("Attention! Unknown step: " + self.step + ". Can not Calculate. Aborting ...") 
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

        self.dst.close()


data_analysis = DataAnalysis("F:/SNOWMODEL_RUNS/run1_45_10_max_eq/output_daily.nc", "F:/SNOWMODEL_RUNS/run1_45_10_max_eq/stats.nc")
data_analysis.setTimeSpan("2000-01-01", "2100-12-31", "S", season = [10,4])
data_analysis.setVariablesToAnalyse([{"var": "asmh", "func": "sum"},{"var": "tas", "func": "mean"}])
data_analysis.calcAll()



