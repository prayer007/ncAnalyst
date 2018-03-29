import numpy as np
import pandas as pd
from netCDF4 import Dataset
import warnings
from helpers import *
from datetime import *
import calendar





class NcManager(object):
    
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
        self.allDaysSinceVec = pd.date_range(self.daysSince, self.daysSinceVec[-1])
        self.daysSinceVecIdx = self.src.variables["time"][:]
        self.yearsSinceVec = self.daysSinceVec.year.unique()
        self.srcStartDate = self.daysSinceVec[0]
        self.srcEndDate = self.daysSinceVec[-1]
        self.monthsSince = self.__setMonthSince()
        self.yearsSince = self.__setYearsSince()
        self.varNamesToBeAnalysed = []
        self.varShape = None # Will be set on .setVariableToAnalyse()
        self.start = None
        self.end = None
        self.userDateVec = None
        self.spanStartSpanEnd = None
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




    def createDateRanges(self, periodStart, periodEnd, yearRange):
        
        
        stepVec = np.array([])

        period = np.arange(self.userDateVec.year.unique().min(), self.userDateVec.year.unique().max(),yearRange+1)

        for i in period:
            
            startDate = pd.to_datetime(date(i, periodStart,1))
            
            if periodStart > periodEnd:
                endDate = pd.to_datetime(date(i+yearRange+1, periodEnd,calendar.monthrange(i+1, periodEnd)[1]))
            else:
                endDate = pd.to_datetime(date(i+yearRange, periodEnd,calendar.monthrange(i, periodEnd)[1]))
            
            if endDate <= self.srcEndDate: 
                stepVec = np.append(stepVec, [{     "startDate": startDate, 
                                                    "endDate": endDate,
                                                    "startIdx": self.daysSinceVecIdx[np.where(self.daysSinceVec==startDate)[0]],
                                                    "endIdx": self.daysSinceVecIdx[np.where(self.daysSinceVec==endDate)[0]],
                                             }])
        
        self.spanStartSpanEnd = stepVec
        
        return stepVec





    def createTimeBounds(self, dateRange):
        
        timebnd = np.empty((0,2))
        
        for i, item in enumerate(dateRange):
            timebnd = np.append(timebnd, [[item["startIdx"][0],item["endIdx"][0]]], axis=0)

        
        return timebnd




    def initializeOutputFile(self, outputPath):

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
        daysSince = str(self.daysSince).split()[0] 
        tunits = "days since " + daysSince
       
        dateRange = self.createDateRanges(self.period["start"], self.period["end"], self.period["yearRange"])
        timeBounds = self.createTimeBounds(dateRange)
            
        time = self.dst.createVariable(varname = 'time', datatype = 'i', dimensions = ('time'))  
        time.units = tunits
        time.bounds = "time_bnds"
        time[:] = timeBounds[:,1]
        
        bndsDim = self.dst.createDimension("bnds")
        time_bnds = self.dst.createVariable(varname = 'time_bnds', datatype = 'i', dimensions = ('time', 'bnds'))
        time_bnds.calendar = "gregorian";
        time_bnds.units = tunits
        

        time_bnds[:] = timeBounds
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
            
            

    def writeToOutputFile(self, varName, stepIncr, data):
        
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






    def setTimeSpan(self, start, end, **kwargs):
        
        self.customTimeFlag = True
        
        try: 
            s = kwargs["period"]
            
            if not isinstance(s, list):
                raise ValueError("Period parameter must be of type array!") 
            
            if len(s) == 0:
                print "No period set. Continuing with default 12-3"
                self.period = {"start": 12, "end": 3, "yearRange": 0}
            elif len(s) == 1:
                self.period = {"start": s[0], "end": s[0], "yearRange": 0}
            elif len(s) == 2:
                self.period = {"start": s[0], "end": s[1], "yearRange": 0}
            elif len(s) == 3:
                self.period = {"start": s[0], "end": s[1], "yearRange": s[2]}
            else:   
                raise ValueError("Too much entries in paramterer 'period'")
        except KeyError:
            self.period = {"start": 12, "end": 3, "yearRange": 0}
            print "Parameter 'period' not set. Continuing with default 12-3"
        
        self.start = pd.to_datetime(start)
        self.end = pd.to_datetime(end)
        self.userDateVec = pd.date_range(self.start, self.end)
    
        if self.end > self.daysSinceVec[-1]:
            raise Exception("End time: " + str(self.end) + " is out of bounds. Bound end is " + str(self.daysSinceVec[-1]))
        
        if self.start < self.daysSinceVec[0]:
            raise Exception("Start time: " + str(self.start) + " is out of bounds. Bound start is " + str(self.daysSinceVec[0]))
        










