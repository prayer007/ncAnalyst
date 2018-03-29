import numpy as np
import pandas as pd
from netCDF4 import Dataset
import warnings
from helpers import *
from datetime import *
import calendar


class NcManager(object):
    '''
    A class which handles all the information about the input
    and output ncfiles. Here the dateranges are created, the
    input file data id readed, the output file initilized and 
    written.
    '''
    def __init__(self, ncPath, outputPath):
        '''        
        Parameters
        ----------
        ncPath : string
            The path of the source file.
        outputPath : string
            The path of the destination file.
        '''
        self.src = None 
        self.dest = None 
        self.outputPath = None
        self.readData(ncPath)
        self.setOutputPath(outputPath)
        self.daysSince = self.__setDaysSince(self.src.variables["time"])
        self.sourceDates = self.__setSourceDates(self.src.variables["time"][:])
        self.allDaysSinceVec = pd.date_range(self.daysSince, self.sourceDates[-1])
        self.sourceDatesIdx = self.src.variables["time"][:]
        self.yearsSinceVec = self.sourceDates.year.unique()
        self.srcStartDate = self.sourceDates[0]
        self.srcEndDate = self.sourceDates[-1]
        self.varNamesToBeAnalysed = []
        self.varShape = None # Will be set on .setVariableToAnalyse()
        self.start = None
        self.end = None
        self.userDateVec = None
        self.spanStartSpanEnd = None
        self.step = "M"
        self.customTimeFlag = False
        
        
    def readData(self, ncPath):
        '''
        Reads the whole ncfile as a netCDF4.Dataset
        '''        
        try:
            self.src = Dataset(ncPath, mode="r", format="NETCDF4")
        except IOError as (errno, strerror):
            print "Could not open netCDF file. I/O error({0}): {1}".format(errno, strerror)
        except:
            print "Could not open netCDF file. Unexpected error:", sys.exc_info()[0]
            raise


    def createDateRanges(self, periodStart, periodEnd, yearRange):
        '''
        Here the dateranges are created from the user defined
        timespan, season and year range.
        
        Parameters
        ----------
        periodStart : int
            The period start as numeric month
        periodEnd : int
            The period end as numeric month
        yearRange : int
            The year range
            
        Returns
        ----------
        stepVec : ndarray
            Container with dicts which holds information
            about start/end Date and Indexes.
        '''        
        stepVec = np.array([])

        period = np.arange(self.userDateVec.year.unique().min(), self.userDateVec.year.unique().max(),yearRange+1)

        for i in period:
            
            startDate = pd.to_datetime(date(i, periodStart,1))
            
            if periodStart > periodEnd:
                endDate = pd.to_datetime(date(i+yearRange+1, periodEnd,calendar.monthrange(i+1, periodEnd)[1]))
            else:
                endDate = pd.to_datetime(date(i+yearRange, periodEnd,calendar.monthrange(i, periodEnd)[1]))
            
            if endDate <= self.srcEndDate and endDate <= self.userDateVec[-1]: 
                stepVec = np.append(stepVec, [{     "startDate": startDate, 
                                                    "endDate": endDate,
                                                    "startIdx": self.sourceDatesIdx[np.where(self.sourceDates==startDate)[0]],
                                                    "endIdx": self.sourceDatesIdx[np.where(self.sourceDates==endDate)[0]],
                                             }])

        self.spanStartSpanEnd = stepVec

        return stepVec


    def createTimeBounds(self, dateRange):
        '''
        The time bound dimension for the nc output
        file gets created.
        
        Parameters
        ----------
        dateRange : ndarray
            The period start as numeric month
            
        Returns
        ----------
        timebnd : ndarray
            Array with start and end index as days since (days since 
            is defined in the source ncfile).
        '''          
        timebnd = np.empty((0,2))
        
        for i, item in enumerate(dateRange):
            timebnd = np.append(timebnd, [[item["startIdx"][0],item["endIdx"][0]]], axis=0)

        return timebnd


    def initializeOutputFile(self, outputPath):
        '''
        Initializes the ouput file with all its dimension
        and metadata for the defined variables.
        
        Parameters
        ----------
        outputPath : string
            The output path of the file
        '''  
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
        
            if name not in varNameContainer and name != xDim and name != yDim: 
                continue
            
            varOut = self.dst.createVariable(name, variable.datatype, variable.dimensions)
            
            if name == xDim or name == yDim:
                self.dst.variables[name][:] = self.src.variables[name][:]  
            
            # Set variable attributes  
            varOut.setncatts({k: variable.getncattr(k) for k in variable.ncattrs()})        
            
            
    def writeToOutputFile(self, varName, stepIncr, data):
        '''
        Write the data iteratively to the file
        
        Parameters
        ----------
        varName : string
            The variable name
        stepIncr : int
            The current calculation step (Starting at 0)
        data : ndarray
            The data to write
        '''         
        self.dst.variables[varName][stepIncr,:,:] = data
        
        
    def __setDaysSince(self, dateTimeString):
        '''
        The date from which the src dataset starts
        
        Parameters
        ----------
        dateTimeString : string
            The raw datetime string of the ncfile
            
        Returns
        ----------
        dateTime : datetime
        '''         
        dts = dateTimeString.units.split()[2:][0]
        dateTime =  pd.to_datetime(dts) 
        
        return dateTime
        
        
    def __setSourceDates(self, daysSince):
        '''
        Creates dates of the source file dates
        
        Parameters
        ----------
        daysSince : ndarray
            The days since the dataset start 
            
        Returns
        ----------
        dateTimeVec : ndarray
            The dates as datetime
        '''        
        vfunc = np.vectorize(pd.to_timedelta)
        
        dateTimeVec = vfunc(daysSince, "D") 
        
        dateTimeVec = self.daysSince +  dateTimeVec
        dateTimeVec = pd.to_datetime(dateTimeVec)
        
        return dateTimeVec


    def setOutputPath(self, op):
        self.outputPath = op


    def setTimeSpan(self, start, end, **kwargs):
        '''
        Sets the user defined period and converts
        the input arguments to datetimes. Also 
        creates a user date vector regarding to 
        the defined period.
        
        Parameters
        ----------
        start : string
            User defined startdate
        end : string
            User defined enddate
        **kwargs
            e.g. 'season' for a user defined season to
            calculate
        '''         
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
    
        if self.end > self.sourceDates[-1]:
            raise Exception("End time: " + str(self.end) + " is out of bounds. Bound end is " + str(self.sourceDates[-1]))
        
        if self.start < self.sourceDates[0]:
            raise Exception("Start time: " + str(self.start) + " is out of bounds. Bound start is " + str(self.sourceDates[0]))