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
sendReady = False
recvReady = False
for i in range(maxSeqNum):
    isACKList.append(0)
    timeList.append(0)

#icmp = getprotobyname("icmp")
mySocket = socket(AF_INET, SOCK_DGRAM)
mySocket.bind(('', portNum))

def createSegment(message, seqNum, isACK, isSYN):
    global portNum
    global cwnd
    sourcePort = portNum
    destPort = portNum
    ackNum = seqNum + 1
    checksum = 0
    #isACK = 0 #REPLACE WITH 0 OR 1 DEPENDING ON VALUE

    sendSeqNum = seqNum
    ACK = 0
    if (isACK):
        sendSeqNum = 0
        ACK = 1
    SYN = 0
    if (isSYN):
        SYN = 1

    #16, 16, 32, 32, 16, 16
    #header = struct.pack("!HHIIHH", sourcePort, destPort, seqNum, ackNum, cwnd, checksum)
    
    #Must be at most 255 characters
    #data = struct.pack("p", message)

    #CHECKSUM

    #header = struct.pack("!HHIIHH", sourcePort, destPort, seqNum, ackNum, cwnd, checksum)

    toSend = ['localhost', sourcePort, destPort, sendSeqNum, ackNum, cwnd, checksum, SYN, ACK, message]
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
    toSend = createSegment(message, nextSeq, isACK, False)
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
    else:
        #DROPPED A PACKET, HANDLE

def TCPReceive():
    global expectedSeq
    global recvTime
    global portNum
    global mySocket
    #THIS WILL ALWAYS ACKNOWLEDGE THE FIRST PACKET
    ackToSend = createSegment("", 0, True, False)
    while True:
        #RECEIVE MESSAGE
        message, clientAddress = mySocket.recvfrom(portNum)
        recvTime = gmtime(time())
        message = message.decode()
        #FIND IF ACK
        ackValue = message[8]
        #FIND SEQ NUM
        receivedSeqNum = message[3]
        #PERFORM CHECKSUM CALCULATIONS
        #FIND CHECKSUM

        receivedChecksum = message[6]
        if(calcChecksum == receivedChecksum):
            if ackValue == 1:
                receiveACK(seqNum)
            elif (expectedSeq == receivedSeqNum):
                #GRAB DATA AND PRINT
                print(message[9])
                ackToSend = createSegment("", receivedSeqNum, True, False)
                #DIRECTLY SEND
                mySocket.sendto(ackToSend.encode(), ('localhost', portNum))
                expectedSeq += 1
        else:
            #DIRECTLY SEND
            mySocket.sendto(ackToSend.encode(), ('localhost', portNum))

def handshakeSend(sock):
    global nextSeq
    global portNum
    global sendReady
    handshakeMessage = createSegment("", nextSeq, False, True)
    sock.sendto(handshakeMessage.encode(), ('localhost', portNum))
    currSeq = nextSeq
    timeList[currSeq] = gmtime(time())
    isACKList[currSeq] = 0
    #NEW THREAD
    tA = Thread(target=timerACK,args=(handshakeMessage,currSeq))
    tA.start()
    synACKGot = False
    while (not synACKGot):
        response, clientAddress = sock.recvfrom(portNum)
        recvTime = gmtime(time())
        response = response.decode()
        #FIND IF SYNACK
        recvACK = response[8]
        recvSYN = response[7]
        if(recvACK == 1 and recvSYN == 1):
            synACKGot = True
            isACKList[currSeq] = 1
            timeoutCalc(timeList[currSeq], recvTime)
    handshakeResponse = createSegment("", nextSeq, True, False)
    sock.sendto(handshakeResponse.encode(), ('localhost', portNum))
    sendReady = True

def handshakeRecv(sock):
    global nextSeq
    global portNum
    global recvReady
    synGot = False
    while (not synGot):
        message, clientAddress = sock.recvfrom(portNum)
        message = message.decode()
        #FIND IF SYN
        recvSYN = response[7]
        recvACK = response[8]
        if(recvSYN == 1 and recvACK == 0):
            synGot = True
    handshakeACK = createSegment("", nextSeq, True, True)
    sock.sendto(handshakeACK.encode(), ('localhost', portNum))
    currSeq = nextSeq
    timeList[currSeq] = gmtime(time())
    isACKList[currSeq] = 0
    #NEW THREAD
    tA = Thread(target=timerACK,args=(handshakeACK,currSeq))
    tA.start()
    ackGot = False
    while (not ackGot):
        response, clientAddress = sock.recvfrom(portNum)
        recvTime = gmtime(time())
        response = response.decode()
        #FIND IF ACK
        recvACK = response[8]
        recvSYN = response[7]
        if(recvACK == 1 and recvSYN == 0):
            ackGot = True
            isACKList[currSeq] = 1
            timeoutCalc(timeList[currSeq], recvTime)
    recvReady = True

def threeWayHandshake(sock):
    hS = Thread(target=handshakeSend,args=(sock, ))
    hR = Thread(target=handshakeRecv,args=(sock, ))
    hS.start()
    hR.start()

threeWayHandshake(mySocket)

while (not sendReady or not recvReady):
    sleep(1)

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
