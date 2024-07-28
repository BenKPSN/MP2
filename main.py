from socket import *
from threading import *
from time import *
import struct

#DON'T KNOW IF THIS WORKS, JUST SOMETHING TO START

portNum = 9876
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

#icmp = getprotobyname("icmp")
mySocket = socket(AF_INET, SOCK_DGRAM)
mySocket.bind(('', portNum))

def createSegment(message, seqNum, isACK):
    global portNum
    sourcePort = portNum
    destPort = portNum
    ackNum = seqNum + 1
    checksum = 0
    #isACK = 0 #REPLACE WITH 0 OR 1 DEPENDING ON VALUE

    sendSeqNum = seqNum
    if (isACK):
        sendSeqNum = 0

    #16, 16, 32, 32, 16, 16
    #header = struct.pack("!HHIIHH", sourcePort, destPort, seqNum, ackNum, cwnd, checksum)
    
    #Must be at most 255 characters
    #data = struct.pack("p", message)

    #CHECKSUM

    #header = struct.pack("!HHIIHH", sourcePort, destPort, seqNum, ackNum, cwnd, checksum)

    toSend = ['localhost', sourcePort, destPort, sendSeqNum, ackNum, cwnd, checksum, message]
    #toSend = header + data
    return toSend


def timerACK(message, seqNum):
    notACK = True
    global isACKList
    global timeOutTime
    global mySocket
    global portNum
    while(notACK):
        sleep(timeOutTime)
        if(isACKList[seqNum] == 0):
            #SEND
            mySocket.sendto(message.encode(), ('localhost', portNum))
        else:
            notACK = False


def TCPSend(message, isACK):
    global nextSeq
    global timeList
    global isACKList
    global mySocket
    global portNum
    toSend = createSegment(message, nextSeq, isACK)
    #SOCKET STUFF
    mySocket.sendto(toSend.encode(), ('localhost', portNum))
    currSeq = nextSeq
    timeList[currSeq] = gmtime(time())
    isACKList[currSeq] = 0
    #NEW THREAD
    tA = Thread(target=timerACK,args=(toSend,currSeq))
    tA.start()
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
    global portNum
    global mySocket
    #THIS WILL ALWAYS ACKNOWLEDGE THE FIRST PACKET
    ackToSend = createSegment("", 0, True)
    while True:
        #RECEIVE MESSAGE
        message, clientAddress = mySocket.recvfrom(portNum)
        message = message.decode()
        #FIND ACK VALUE
        ackValue = message[4]
        #FIND SEQ NUM
        receivedSeqNum = message[3]
        #PERFORM CHECKSUM CALCULATIONS
        #FIND CHECKSUM
        receivedChecksum = message[6]
        if(calcChecksum == receivedChecksum):
            if ackValue == 1:
                recvTime = gmtime(time())
                receiveACK(seqNum)
            elif (expectedSeq == receivedSeqNum):
                #GRAB DATA AND PRINT
                print(message[7])
                ackToSend = createSegment("", receivedSeqNum, True)
                #DIRECTLY SEND
                mySocket.sendto(ackToSend.encode(), ('localhost', portNum))
                expectedSeq += 1
        else:
            #DIRECTLY SEND
            mySocket.sendto(ackToSend.encode(), ('localhost', portNum))

#THREAD
tR = Thread(target=TCPReceive)
tR.start()

while True:
    #SOCKETS?
    #THREAD
    message = input("Please enter a message: ")
    #THREAD
    tS = Thread(target=TCPSend,args=(message, False, ))
    tS.start()
    #seqNum = nextSeq
    #RECEIVE SOCKET
    #recvTime = gmtime(time())
    #receiveACK(seqNum)
