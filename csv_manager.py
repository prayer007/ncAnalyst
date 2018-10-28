#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import time

class CsvManager(object):
    '''
    Class for managing csv functionalities like initializing, 
    formatting, inserting values and writing output files for
    spotty data (e.g. statistics of user defined points).
    '''
    def __init__(self, working_dir, timespan, vars, points, filename): 
        self.collector = {}
        self.filename = filename
        self.workingDir = working_dir
        self.datesStrings = self.__dateFormatter(timespan)
        self.__initFileFrames(vars, points)


    def __initFileFrames(self, vars, points):
        '''
        Initializes the file frames and a collector as 
        a data and metadate storage (self.collector). This
        collector stores date-strings, an empty initialized 
        data frame and containers for iteratively store
        output data.
        
        Parameters
        ----------
        vars : dict
            Raw variables from ncfile and some metadata
        points : ndarray
            Container with point instances
        '''
        header = [var["var"] for var in vars]
        header.insert(0,"timestep")
        
        df = pd.DataFrame(columns=header)
        
        for point in points:
            self.collector[id(point)] = {   "df": df, 
                                            "fileName": str(point.getPointCoords()["xIdx"]) + "_" + str(point.getPointCoords()["yIdx"]) + self.filename,
                                            "timestep": {"data": self.datesStrings, "long_name": "timestep", "units": ""}
                                        }
        
            for var in vars:
                varName = var["var"]
                funcName = var["func"]["name"]
                
                try:
                    long_name = var["data"].long_name + "_" + funcName
                except:
                    long_name = varName + "_" + funcName
                    
                try:
                    units = var["data"].units
                except:
                    units = ""    
                
                self.collector[id(point)][varName] = {"data": [], "long_name": long_name, "units": units}
 
 
    def __dateFormatter(self, timespan):
        '''
        Formates the dates for human reading csv output.
        
        Parameters
        ----------
        timespan : ndarray
            Container with dicts which holds information
            about start/end Date and Indexes.
        
        Returns
        ----------
        timespan : ndarray
            Array with formatted dates
        '''        
        timespanStrs = []
        
        for time in timespan: 
            timespanStrs.append(time["startDate"].strftime("%Y-%m-%d") + "/" + time["endDate"].strftime("%Y-%m-%d"))
        
        return timespanStrs
 
        
    def writeDataToFile(self):
        '''
        Finally writes the data to a csv file.
        First an dataframe is created of the
        data arrays.
        '''
        collector = self.collector
        
        for pointId in collector:
            frame = self.collector[pointId]
            df = frame["df"] 

            for colName in list(df):
                df[colName] = frame[colName]["data"]
                
                if colName != "timestep":
                    unitSep = "/"
                else:
                    unitSep = ""
                    
                df = df.rename(index=str, columns={colName: frame[colName]["long_name"] + unitSep + frame[colName]["units"]})

            df.to_csv(self.workingDir + frame["fileName"] + ".csv", index=False)
    
    
    def collectValues(self, varName, val, point):
        '''
        Appends the data to the corresponding point and
        variable container.
        
        Parameters
        ----------
        varName : string
        val : numeric
            The extracted value for the point
        point : point_manager.Point()
            Point instance
        '''
        varValContainer = self.collector[id(point)][varName]["data"] 
        varValContainer.append(val)