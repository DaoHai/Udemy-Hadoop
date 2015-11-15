# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 08:48:36 2015

@author: daohai
"""

from mrjob.job import  MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp)=line.split('\t')
        yield rating, 1
        
    def reducer(self, rating, occurences):
        yield rating, sum(occurences)
        
if __name__=='__main__':
    MRRatingCounter.run()