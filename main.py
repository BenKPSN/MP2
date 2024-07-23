from socket import *
from threading import *
from time import *

#DON'T KNOW IF THIS WORKS, JUST SOMETHING TO START

nextSeq = 5
base = 5
timeOutTime = 1
isACKList = []
maxSeqNum = 87
for i in range(maxSeqNum):
    isACKList.append(0)

def createSegment(message, seqNum):
    #WHATEVER

def timerACK(message, seqNum):
    notACK = True
    while(notACK):
        sleep(timeOutTime)
        if(isACKList[seqNum] == 0):
            #SEND
        else:
            notACK = False


def TCPSend(message):
    toSend = createSegment(message, nextSeq)
    #SOCKET STUFF
    #NEW THREAD
    timerACK(toSend, nextSeq)
    nextSeq = (nextSeq + len(message)) % maxSeqNum

def receiveACK(seqNum):
    if(seqNum > base):
        base = seqNum % base
        isACKList[seqNum] = 1
