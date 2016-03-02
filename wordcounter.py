# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 11:34:53 2015

@author: daohai
"""

from mrjob.job import  MRJob

class MRWordCounter(MRJob):
    def mapper(self, key, line):
        words=line.split() #list of words for each line in a paragraph
        for word in words:
            yield word.lower(), 1
        
    def reducer(self, key, values):
        yield key, sum(values)
        
if __name__=='__main__':
    MRWordCounter.run()