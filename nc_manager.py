#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from netCDF4 import Dataset
import warnings
from helpers import *
from datetime import *
import calendar
import numpy.ma as ma

class NcManager(object):
    '''
    A class which handles all the information about the input
    and output ncfiles. Here the dateranges are created, the
    input file data id readed, the output file initilized and 
    written.
    '''
    def __init__(self, working_dir, ncPath, outputPath):
        '''        
        Parameters
        ----------
        working_dir : string
            The working directory
        ncPath : string
            The path of the source file.
        outputPath : string
            The default output path of the destination file.

        Attributes
        ----------
        
        src : string
            Path to the source file
        dest : string
            Path to the destination file
        ouputPath : string
            Path were the output should be written  
        customTimeFlag : bool
            Flag to check if a timespan was set

        ----------
        To better describe the attributes regarding to the
        time dimension let´s assume following ncfile setting:
        time.units = "days since 1949-12-01 00:00:00"
        time index start = 18293, so starting date is 2000-01-01  
        time index end = 55182, so end date is 2100-12-31  
        user setting: "2000-01-01", "2011-12-31", period = [10,4,5]

        daysSince : datetime
            The date extracted from the "days since 1949-12-01 00:00:00" string 
        sourceDates : ndarray
            All the dates of the source file as datetimes (2000-01-01 to 2100-12-31)
        allDaysSinceVec : ndarray
            All the dates since the files starting date (1949-12-01 to 2100-12-31) 
        sourceDatesIdx : pandas.Series 
            Source dates as index and progressive index as value (Index= 2000-01-01 to 2100-12-31, value= 0 to 36889)  
        sourceDatesIdxAll : ndarray
            All indexes from start. 
        srcStartDate : datetime
            Start date of the source (2000-01-01)
        srcEndDate : datetime
            End date of the source (2100-12-31)
        datesToAnalyse : ndarray
            Only the dates that have to be analysed. From 2000-01-01 to 2011-12-31 all months from 10 (October) to 4 (April) 
        userDateVec : ndarray
            All dates from the user defined timespan (2000-01-01 to 2011-12-31)
        boolDateVec : ndarray
            A boolean date vector between user start and user end. 
        spanStartSpanEnd : ndarray
            Array with dicts of the user defined periods
            [{'startDate': Timestamp('2000-10-01 00:00:00'), 'endDate': Timestamp('2006-04-30 00:00:00'), 'endIdx': array([20604]), 'startIdx': array([18567])}]
        start : datetime
            User defined start date (2000-01-01) 
        end : datetime
            User defined end date (2011-12-31)
        '''
        self.src = None 
        self.dest = None 
        self.outputPath = None
        self.workingDir = working_dir
        self.readData(working_dir, ncPath)
        self.setOutputPath(outputPath)
        self.varNamesToBeAnalysed = []
        self.daysSince = self.__setDaysSince(self.src.variables["time"])
        self.sourceDates = self.__setSourceDates(self.src.variables["time"][:])
        self.allDaysSinceVec = pd.date_range(self.daysSince, self.sourceDates[-1])
        self.sourceDatesIdx = self.__setSourceDatesIdx(self.sourceDates)
        self.sourceDatesIdxAll = self.src.variables["time"][:]
        self.srcStartDate = self.sourceDates[0]
        self.srcEndDate = self.sourceDates[-1]
        self.datesToAnalyse = []
        self.userDateVec = []
        self.chunkIdxContainer = []
        self.boolDateVec = []
        self.varShape = None # Will be set on .setVariableToAnalyse()
        self.start = None
        self.end = None
        self.spanStartSpanEnd = None
        self.customTimeFlag = False
        
        
    def readData(self, working_dir, ncPath):
        '''
        Reads the whole ncfile as a netCDF4.Dataset
        '''        
        try:
            self.src = Dataset(working_dir + ncPath, mode="r", format="NETCDF4")
            self.src.set_auto_mask(False)
        except IOError as (errno, strerror):
            print "Could not open netCDF file. I/O error({0}): {1}".format(errno, strerror)
            raise
        except:
            print "Could not open netCDF file. Unexpected error:", sys.exc_info()[0]
            raise


    def createDateRanges(self, period):
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
        monthStart = period["monthStart"]
        monthEnd = period["monthEnd"]
        yearRange = period["yearRange"]
        
        userStartDate = pd.to_datetime(date(self.start.year, monthStart,1))
        userEndDate = pd.to_datetime(date(self.end.year, monthEnd, calendar.monthrange(self.end.year, monthEnd)[1]))
        
        datesToAnalyse = self.sourceDates[(self.sourceDates >= userStartDate) & (self.sourceDates <= userEndDate)]
        
        if monthStart >= monthEnd:
            datesToAnalyse = datesToAnalyse[(datesToAnalyse.month >= monthStart) | (datesToAnalyse.month <= monthEnd)]
        else:
            datesToAnalyse = datesToAnalyse[(datesToAnalyse.month >= monthStart) & (datesToAnalyse.month <= monthEnd)]

        self.datesToAnalyse = datesToAnalyse
        
    
        stepVec = np.array([])

        period = np.arange(self.userDateVec.year.unique().min(), self.userDateVec.year.unique().max(),yearRange+1)

        for i in period:
            
            startDate = pd.to_datetime(date(i, monthStart,1))
            
            if monthStart >= monthEnd:
                endDate = pd.to_datetime(date(i+yearRange+1, monthEnd,calendar.monthrange(i+1, monthEnd)[1]))
            else:
                endDate = pd.to_datetime(date(i+yearRange, monthEnd,calendar.monthrange(i, monthEnd)[1]))
            
            if endDate <= self.srcEndDate and endDate <= self.userDateVec[-1]: 
                stepVec = np.append(stepVec, [{     "startDate": startDate, 
                                                    "endDate": endDate,
                                                    "startIdx": self.sourceDatesIdxAll[np.where(self.sourceDates==startDate)[0]],
                                                    "endIdx": self.sourceDatesIdxAll[np.where(self.sourceDates==endDate)[0]],
                                             }])

        self.boolDateVec = self.__createBoolDateVec()
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


    def initializeOutputFile(self, outputPath, varsToBeAnalysed):
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
       
        dateRange = self.createDateRanges(self.period)
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
        
        varNameContainer = [i["var"] for i  in varsToBeAnalysed]
  
        for name, variable in self.src.variables.iteritems():
        
            xDim = "x"
            yDim = "y"
        
            if name not in varNameContainer and name != xDim and name != yDim: 
                continue
            
            for var in varsToBeAnalysed:
                if name == var["var"]:
                    funcName = var["func"]["name"]
                    funcProps = var["func"]["props"] 
                    if funcProps:
                        name = name + "_[" + funcName + "_" + funcProps[1] + "_" + str(funcProps[2]) + "]"
                    else:
                        name = name + "_[" + funcName + "]"

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


    def __setSourceDatesIdx(self, srcDates):
        '''
        Creates a pandas series with all the dates
        from the source file and its corresponding
        progressive index
        
        Returns
        ----------
        s : pandas:Series
            Index = Source file dates as datetime
            Value = Progressive index
        '''        
        s = pd.Series(np.arange(0,len(srcDates)), index=srcDates)
        
        return s


    def __createBoolDateVec(self):
        '''
        Creates a boolean date vector between
        user start and user end. Turns true if the
        dates to analyse are in this range. E.g. this
        is needed if a period over one year is chosen
        and only a specific season has to be analysed.
        
        Returns
        ----------
        boolArr : ndarray
            True = Dates that should be included for
            further processing.
            False = Dates that shouldn´t be included
        '''      
        boolArr = np.array([], dtype=bool)
        
        for date in self.userDateVec:
            if date in self.datesToAnalyse:
                boolArr = np.append(boolArr, True)
            else:
                boolArr = np.append(boolArr, False)

        return boolArr


    def __createChunkIndexes(self, userDates):
        '''
        Creates chunk indexes.
        
        Returns
        ----------
        chunkIdxs : ndarray
            The chunk indexes as tuples
        ''' 
        chunkIdxs = []
        step = 200
        for startIdx in np.arange(-1, len(userDates), step):
            endIdx = startIdx + step
            lastDateIdx = userDates.get_loc(userDates[-1])
            if startIdx+step > lastDateIdx:
                endIdx = lastDateIdx+1
                
            startIdx = startIdx+1
            
            chunkIdxs.append((startIdx, endIdx))
                
        return chunkIdxs

    
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
            
            ds = s[0]
            de = s[1]
            ms = s[2]
            me = s[3]
            y  = s[4]
            
            isMonthSet = (ms != 0 or ms is not None) and (me != 0 or me is not None) 
            isDaySet = (ds != 0 or ds is not None) and (de != 0 or de is not None)
            
            if len(s) == 0:
                print "No period set. Continuing with default 12-3"
                self.period = {"start": 12, "end": 3, "yearRange": 0}
            elif (isDaySet and isMonthSet):
                self.period = {"dayStart": ds, "dayEnd": de,"monthStart": ms, "monthEnd": me, "yearRange": y}
            elif isMonthSet:
                self.period = {"dayStart": 1, "dayEnd": None, "monthStart": ms, "monthEnd": me, "yearRange": y}
            else:   
                raise ValueError("Wrong entries in parameter 'period'")
        except KeyError:
            self.period = {"start": 12, "end": 3, "yearRange": 0}
            print "Parameter 'period' not set. Continuing with default 12-3"
        
        self.start = pd.to_datetime(start)
        self.end = pd.to_datetime(end)
        self.userDateVec = pd.date_range(self.start, self.end)
        #self.chunkIdxContainer = self.__createChunkIndexes(self.userDateVec)
    
        if self.end > self.sourceDates[-1]:
            raise Exception("End time: " + str(self.end) + " is out of bounds. Bound end is " + str(self.sourceDates[-1]))
        
        if self.start < self.sourceDates[0]:
            raise Exception("Start time: " + str(self.start) + " is out of bounds. Bound start is " + str(self.sourceDates[0]))