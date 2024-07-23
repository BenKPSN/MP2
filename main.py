from socket import *
from threading import *
from time import *

#DON'T KNOW IF THIS WORKS, JUST SOMETHING TO START

nextSeq = 5
base = 5
timeOutTime = 1
estTime = 0
devTime = 0
isACKList = []
timeList = []
maxSeqNum = 87
for i in range(maxSeqNum):
    isACKList.append(0)
    timeList.append(0)

def createSegment(message, seqNum):
    #WHATEVER

def timerACK(message, seqNum):
    notACK = True
    global isACKList
    global timeOutTime
    while(notACK):
        sleep(timeOutTime)
        if(isACKList[seqNum] == 0):
            #SEND
        else:
            notACK = False


def TCPSend(message):
    global nextSeq
    global timeList
    toSend = createSegment(message, nextSeq)
    #SOCKET STUFF
    timeList[nextSeq] = gmtime(time())
    #NEW THREAD
    timerACK(toSend, nextSeq)
    nextSeq = (nextSeq + len(message)) % maxSeqNum

def receiveACK(seqNum):
    global isACKList
    global base
    if(seqNum > base):
        base = seqNum % base
        isACKList[seqNum] = 1

while True:
    #SOCKETS?
    #THREAD
    message = input("Please enter a message: ")
    #THREAD
    TCPSend(message)
    seqNum = nextSeq
    #RECEIVE SOCKET
    recvTime = gmtime(time())
    receiveACK(seqNum)
    measureTime = recvTime - timeList[seqNum]
    devTime = 0.75 * devTime + 0.25 * abs(measureTime - estTime)
    estTime = 0.875 * estTime + 0.125 * measureTime
    timeOutTime = estTime + 4 * devTime
