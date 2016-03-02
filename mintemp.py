# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 11:20:20 2015

@author: daohai
"""

from mrjob.job import  MRJob

class MRmintemp(MRJob):
    
    def convertfahrenheit(self, rawcelsius): #helper function
        celsius=float(rawcelsius)/10.0
        fahrenheit=celsius*1.8+32.0
        return fahrenheit
        
        
    
    def mapper(self, _, line):
        (location, date, type, data, x, y, z, w) = line.split(',')
        if (type=='TMIN'):
            temperature=self.convertfahrenheit(data) #self. functioname to call the function
            yield location, temperature
        
    def reducer(self, location, temps):
        yield location, min(temps)
        
if __name__=='__main__':
    MRmintemp.run()