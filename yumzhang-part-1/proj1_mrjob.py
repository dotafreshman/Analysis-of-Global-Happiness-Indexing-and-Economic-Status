import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
import re


class Task1(MRJob):
    #OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol

    def mapper(self, _, line):
        data=line.split(',')
        if float(data[3])>=6:
            yield data[1],1
        else:
            yield data[1],-1

    def combiner(self, region,score):
        yield  region,sum(score)

    def reducer(self,region,score):
        yield region,sum(score)








if __name__=='__main__':
    Task1.run()