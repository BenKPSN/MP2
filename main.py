from socket import *
from threading import *
from time import *
import struct

#DON'T KNOW IF THIS WORKS, JUST SOMETHING TO START

nextSeq = 5
expectedSeq = 5
base = 5
timeOutTime = 1
estTime = 0
devTime = 0
isACKList = []
timeList = []
maxSeqNum = 87
cwnd = 99
for i in range(maxSeqNum):
    isACKList.append(0)
    timeList.append(0)

icmp = getprotobyname("icmp")
mySocket = socket(AF_INET, SOCK_RAW, icmp)

def createSegment(message, seqNum, isACK):
    sourcePort = 9876
    destPort = 9876
    ackNum = seqNum + 1
    checksum = 0
    isACK = 0 #REPLACE WITH 0 OR 1 DEPENDING ON VALUE

    #16, 16, 32, 32, 16, 16
    header = struct.pack("!HHIIHH", sourcePort, destPort, seqNum, ackNum, cwnd, checksum)
    
    #Must be at most 255 characters
    data = struct.pack("p", message)

    #CHECKSUM

    header = struct.pack("!HHIIHH", sourcePort, destPort, seqNum, ackNum, cwnd, checksum)
    toSend = header + data
    return toSend


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


def TCPSend(message, isACK):
    global nextSeq
    global timeList
    global isACKList
    toSend = createSegment(message, nextSeq, isACK)
    #SOCKET STUFF
    timeList[nextSeq] = gmtime(time())
    isACKList[nextSeq] = 0
    #NEW THREAD
    timerACK(toSend, nextSeq)
    #nextSeq = (nextSeq + len(message)) % maxSeqNum
    nextSeq += 1 #FOCUS ON BASICS FOR NOW

def timeoutCalc(sendTime, timeReceived):
    global timeOutTime
    measureTime = timeReceived - sendTime
    devTime = 0.75 * devTime + 0.25 * abs(measureTime - estTime)
    estTime = 0.875 * estTime + 0.125 * measureTime
    timeOutTime = estTime + 4 * devTime

def receiveACK(seqNum):
    global isACKList
    global base
    global timeList
    global recvTime
    timeoutCalc(timeList[seqNum], recvTime)
    if(seqNum > base):
        base = seqNum % base
        isACKList[seqNum] = 1

def TCPReceive():
    global expectedSeq
    global recvTime
    #THIS WILL ALWAYS ACKNOWLEDGE THE FIRST PACKET
    ackToSend = createSegment("", 0, True)
    while True:
        #RECEIVE MESSAGE
        #FIND ACK VALUE
        #FIND SEQ NUM
        #PERFORM CHECKSUM CALCULATIONS
        #FIND CHECKSUM
        if(calcChecksum == receivedChecksum):
            if ackValue == 1:
                recvTime = gmtime(time())
                receiveACK(seqNum)
            elif (expectedSeq == receivedSeqNum):
                #GRAB DATA AND PRINT
                ackToSend = createSegment("", receivedSeqNum, True)
                #DIRECTLY SEND
                expectedSeq += 1
        else:
            #DIRECTLY SEND

#THREAD
TCPReceive()

while True:
    #SOCKETS?
    #THREAD
    message = input("Please enter a message: ")
    #THREAD
    TCPSend(message, False)
    #seqNum = nextSeq
    #RECEIVE SOCKET
    #recvTime = gmtime(time())
    #receiveACK(seqNum)
