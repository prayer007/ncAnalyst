class PointManager(object):
    '''
    Class for managing and creating Points.
    '''
    def __init__(self): 
        self.pointsContainer = []
    
    
    def createPoint(self, xIdx, yIdx):
        pt = Point(xIdx, yIdx)     
        self.pointsContainer.append(pt)


class Point(PointManager):
    '''
    The point class which holds information 
    about the point like its coordinates.
    '''
    def __init__(self, xIdx, yIdx): 
        self.__xIdx = xIdx
        self.__yIdx = yIdx
        
    
    def __setPointCoords(self,xIdx,yIdx):
        self.__xIdx = xIdx 
        self.__yIdx = yIdx
 
 
    def __setPointVal(self,val):
        self.__val = val 
        

    def getPointCoords(self):
        return {"xIdx": self.__xIdx, "yIdx": self.__yIdx}

 
    def getPointVal():
        return self.val 
 
 
    def extractPtVal(self, frame):
        '''
        Extracts the value of the calculation result
        for the specific point coordinates.
        
        Parameters
        ----------
        frame : ndarray
            The frame from where the data gets extracted. 
            
        Returns
        ----------
        val : float
            The single point value. 
        '''
        val = frame[self.__yIdx, self.__xIdx]
        self.__setPointVal(val)
        return val
        
        