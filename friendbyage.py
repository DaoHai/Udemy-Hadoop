# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 10:52:18 2015

@author: daohai
"""

from mrjob.job import  MRJob

class MRfriendbyage(MRJob):
    def mapper(self, key, line):
        (ID, name, age, numFriends)=line.split(',')
        yield age, float(numFriends)
        
    def reducer(self, age, numFriend): #age, numFriends is global var
        total=0
        numElements=0
        for x in numFriend: 
            total+=x
            numElements+=1
            
        yield age, total/numElements
        
if __name__=='__main__':
    MRfriendbyage.run()
