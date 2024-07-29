from socket import *
from threading import *
from time import *
import struct
import pickle

#THIS WORKS

recvPort = 9876
sendPort = 9875
nextSeq = 5
expectedSeq = 5
base = 5
timeOutTime = 1
estTime = 0
devTime = 0
isACKList = []
timeList = []
unackedMessages = []
maxSeqNum = 87
cwnd = 99
testDone = 0
testEndSeq = 0
sendReady = False
recvReady = False
sendDone = False
recvDone = False
sendTest = False
recvTest = False
for i in range(maxSeqNum):
    isACKList.append(0)
    timeList.append(0)
    unackedMessages.append([])

def createSegment(message, seqNum, isACK, isSYN):
    global recvPort
    global cwnd
    global testDone
    sourcePort = recvPort
    destPort = sendPort
    ackNum = seqNum + 1
    checksum = Checksum(message)
    #isACK = 0 #REPLACE WITH 0 OR 1 DEPENDING ON VALUE

    sendSeqNum = seqNum
    ACK = 0
    if (isACK):
        sendSeqNum = 0
        ACK = 1
        ackNum = seqNum
    SYN = 0
    if (isSYN):
        SYN = 1

    #16, 16, 32, 32, 16, 16
    #header = struct.pack("!HHIIHH", sourcePort, destPort, seqNum, ackNum, cwnd, checksum)
    
    #Must be at most 255 characters
    #data = struct.pack("p", message)

    #CHECKSUM

    #header = struct.pack("!HHIIHH", sourcePort, destPort, seqNum, ackNum, cwnd, checksum)

    toSend = ['localhost', sourcePort, destPort, sendSeqNum, ackNum, cwnd, checksum, SYN, ACK, message, testDone]
    #toSend = header + data
    return toSend


def timerACK(message, seqNum, testNum, messageTestNum):
    notACK = True
    global isACKList
    global timeOutTime
    global mySocket
    global recvPort
    global sendPort
    while(notACK):
        sleep(timeOutTime)
        if(isACKList[seqNum] == 0 and not sendDone):
            #SEND
            if(messageTestNum == 6):
                if(testNum != 1):
                    mySocket.sendto(pickle.dumps(message), ('localhost', sendPort))
                else:
                    notACK = False
        else:
            notACK = False


def TCPSend(message, isACK):
    global nextSeq
    global timeList
    global isACKList
    global mySocket
    global recvPort
    global sendPort
    global sendDone
    global maxSeqNum
    global sendTest
    toSend = createSegment(message, nextSeq, isACK, False)
    #SOCKET STUFF
    mySocket.sendto(pickle.dumps(toSend), ('localhost', sendPort))
    currSeq = nextSeq
    #timeList[currSeq] = gmtime(time())
    timeList[currSeq] = time()
    isACKList[currSeq] = 0
    unackedMessages[currSeq] = toSend
    #NEW THREAD
    tA = Thread(target=timerACK,args=(toSend,currSeq,0,0))
    tA.start()
    #nextSeq = (nextSeq + len(message)) % maxSeqNum
    nextSeq += 1 #FOCUS ON BASICS FOR NOW
    if(message == "DONE"):
        sendDone = True
        for i in range(maxSeqNum):
            isACKList[i] = 1
    if(message == "TEST"):
        sendTest = True

def timeoutCalc(sendTime, timeReceived):
    global timeOutTime
    global estTime
    global devTime
    measureTime = timeReceived - sendTime
    devTime = 0.75 * devTime + 0.25 * abs(measureTime - estTime)
    estTime = 0.875 * estTime + 0.125 * measureTime
    timeOutTime = estTime + 4 * devTime

def receiveACK(seqNum):
    global isACKList
    global base
    global timeList
    global recvTime
    global maxSeqNum
    global lastMessage
    global testEndSeq
    global recvTest
    timeoutCalc(timeList[seqNum], recvTime)
    if(recvTest):
        print(seqNum)
        testEndSeq = seqNum
    if(seqNum > base):
        base = seqNum
        base = base % maxSeqNum
        isACKList[seqNum] = 1
    else:
        if(seqNum == lastACK):
            timesACK += 1
        if(timesACK == 3):
            timesACK = 0
            mySocket.sendto(pickle.dumps(unackedMessages[base]), ('localhost', sendPort))
            timeList[seqNum] = time()

# a very simple checksum, will replace with a two-dimensional parity scheme when I figure it out
def Checksum(message):
    csum = 0
    for i in range(len(message)):
        #csum = csum + message[i]
        csum += 1

    res = 255 - csum % 256
    return res

def TCPReceive():
    global expectedSeq
    global recvTime
    global recvPort
    global sendPort
    global mySocket
    global sendDone
    global recvDone
    global sendTest
    global recvTest
    #THIS WILL ALWAYS ACKNOWLEDGE THE FIRST PACKET
    ackToSend = createSegment("", 0, True, False)
    while (not recvDone):
        #while(sendTest):
        #    sleep(1)
        #RECEIVE MESSAGE
        message, clientAddress = mySocket.recvfrom(recvPort)
        #recvTime = gmtime(time())
        recvTime = time()
        message = pickle.loads(message)
        #print("message received")
        #FIND IF ACK
        isACK = message[7]
        ackValue = message[4]
        #FIND SEQ NUM
        receivedSeqNum = message[3]
        #PERFORM CHECKSUM CALCULATIONS
        #FIND CHECKSUM

        calcChecksum = Checksum(message[9])

        receivedChecksum = message[6]
        if(calcChecksum == receivedChecksum):
            if (isACK == 1 and not sendDone):
                print("ACK")
                receiveACK(ackValue)
            elif (message[9] == "DONE" and sendDone):
                print("Receiver closed")
                print(receivedSeqNum)
                ackToSend = createSegment("DONE", receivedSeqNum, True, False)
                mySocket.sendto(pickle.dumps(ackToSend), ('localhost', sendPort))
                sleep(timeOutTime*2)
                recvDone = True
            elif (expectedSeq == receivedSeqNum):
                #FOR THE TEST PROCESS
                testDone = message[10]
                if(message[9] == "DONE"):
                    print("Received DONE")
                    sendDone = True
                    recvDone = True
                    ackToSend = createSegment("", receivedSeqNum, True, False)
                    #DIRECTLY SEND
                    print(receivedSeqNum)
                    mySocket.sendto(pickle.dumps(ackToSend), ('localhost', sendPort))
                    doneMessage = createSegment("DONE", receivedSeqNum + 1, False, False)
                    mySocket.sendto(pickle.dumps(doneMessage), ('localhost', sendPort))
                    print("waiting for last ACK")
                    finalACK = False
                    while(not finalACK):
                        message, clientAddress = mySocket.recvfrom(recvPort)
                        message = pickle.loads(message)
                        isACK = message[7]
                        ackValue = message[4]
                        if(isACK == 1 and message[4] == (receivedSeqNum + 1)):
                            print("Received final ACK")
                            finalACK = True
                elif(message[9] == "TEST"):
                    sendTest = True
                    recvTest = True
                    ackToSend = createSegment("TEST", receivedSeqNum, True, False)
                    #DIRECTLY SEND
                    print(receivedSeqNum)
                    mySocket.sendto(pickle.dumps(ackToSend), ('localhost', sendPort))
                    tst = Thread(target=testingReceiver)
                    tst.start()
                else:
                    #GRAB DATA AND PRINT
                    print(message[9])
                    ackToSend = createSegment("", receivedSeqNum, True, False)
                    #DIRECTLY SEND
                    mySocket.sendto(pickle.dumps(ackToSend), ('localhost', sendPort))
                    expectedSeq += 1
            else:
                mySocket.sendto(pickle.dumps(ackToSend), ('localhost', sendPort))
        else:
            #DIRECTLY SEND
            mySocket.sendto(pickle.dumps(ackToSend), ('localhost', sendPort))
    #mySocket.close()

def handshakeSend(sock):
    global nextSeq
    global recvPort
    global sendPort
    global sendReady
    #print("sending message")
    handshakeMessage = createSegment("", nextSeq, False, True)
    sock.sendto(pickle.dumps(handshakeMessage), ('localhost', sendPort))
    currSeq = nextSeq
    #timeList[currSeq] = gmtime(time())
    timeList[currSeq] = time()
    isACKList[currSeq] = 0
    #NEW THREAD
    tA = Thread(target=timerACK,args=(handshakeMessage,currSeq,0,0))
    tA.start()
    synACKGot = False
    #print("waiting for SYNACK")
    while (not synACKGot):
        response, clientAddress = sock.recvfrom(recvPort)
        #recvTime = gmtime(time())
        recvTime = time()
        response = pickle.loads(response)
        #FIND IF SYNACK
        recvACK = response[8]
        recvSYN = response[7]
        if(recvACK == 1 and recvSYN == 1):
            synACKGot = True
            isACKList[currSeq] = 1
            timeoutCalc(timeList[currSeq], recvTime)
    #print("SYNACK got. Sending ACK")
    handshakeResponse = createSegment("", nextSeq, True, False)
    sock.sendto(pickle.dumps(handshakeResponse), ('localhost', sendPort))
    sendReady = True

def handshakeRecv(sock):
    global nextSeq
    global recvPort
    global sendPort
    global recvReady
    synGot = False
    #print("waiting for message")
    while (not synGot):
        message, clientAddress = sock.recvfrom(recvPort)
        message = pickle.loads(message)
        #FIND IF SYN
        recvSYN = message[7]
        recvACK = message[8]
        if(recvSYN == 1 and recvACK == 0):
            synGot = True
    #print("everything's good, sending ACK")
    handshakeACK = createSegment("", nextSeq, True, True)
    sock.sendto(pickle.dumps(handshakeACK), ('localhost', sendPort))
    currSeq = nextSeq
    #timeList[currSeq] = gmtime(time())
    timeList[currSeq] = time()
    isACKList[currSeq] = 0
    #NEW THREAD
    tA = Thread(target=timerACK,args=(handshakeACK,currSeq,0,0))
    tA.start()
    ackGot = False
    #print("waiting for ACK")
    while (not ackGot):
        response, clientAddress = sock.recvfrom(recvPort)
        #recvTime = gmtime(time())
        recvTime = time()
        response = pickle.loads(response)
        #FIND IF ACK
        recvACK = response[8]
        recvSYN = response[7]
        if(recvACK == 1 and recvSYN == 0):
            ackGot = True
            isACKList[currSeq] = 1
            timeoutCalc(timeList[currSeq], recvTime)
    recvReady = True
    #print("ACK got")

def threeWayHandshake(sock):
    sleep(5)
    #print("in threeway")
    hS = Thread(target=handshakeSend,args=(sock, ))
    hR = Thread(target=handshakeRecv,args=(sock, ))
    hS.start()
    hR.start()

def testingSender():
    global sendTest
    global recvTest
    global mySocket
    global sendPort
    global recvPort
    global testEndSeq
    global testDone
    global base
    while(sendTest and not recvTest):
        message, clientAddress = mySocket.recvfrom(recvPort)
        message = pickle.loads(message)
        isACK = message[7]
        ackValue = message[4]
        calcChecksum = Checksum(message[9])
        receivedChecksum = message[6]
        if(receivedChecksum == calcChecksum and isACK == 1 and message[9] == "TEST"):
            recvTest = True
    print("We will now begin the tests.")
    print('\n')

    keepBase = base
    base = 0
    
    print("TEST 1")
    print("10 messages will be sent, consisting of the numbers 1 through 10.")
    print("Message #6, however, will be forced to not send, nor resend.")
    print("It will only resend when we get the Triple Duplicate ACK.")
    sleep(3)
    
    testDone = 1
    testEndSeq = 0
    testSend = 1
    while(testSend != 11):
        toSend = createSegment(str(testSend), testSend, False, False)
        mySocket.sendto(pickle.dumps(toSend), ('localhost', sendPort))
        currSeq = testSend
        timeList[currSeq] = time()
        isACKList[currSeq] = 0
        unackedMessages[currSeq] = toSend
        tA = Thread(target=timerACK,args=(toSend,currSeq,1,testSend))
        tA.start()
        testSend += 1
    print("All sent. Awaiting response.")
    while(testEndSeq != 10):
        sleep(0.5)
    testEndSeq = 0
    print("Test 1 complete")
    print("\n")

    print("Testing complete!")
    print("\n")
    sendTest = False
    recvTest = False
    base = keepBase

def testingReceiver():
    global expectedSeq
    keepSeq = expectedSeq
    expectedSeq = 1
    print("Awaiting sender to test. Please do not enter anything until testing is complete.")
    while(not testDone):
        sleep(0.1)
    print("All tests are done! You may now send.")
    sendTest = False
    recvTest = False
    expectedSeq = keepSeq

userSelected = False
while (not userSelected):
    userNum = input("Which user are you (1 or 2): ")
    if(userNum == "1"):
        userSelected = True
        recvPort = 9876
        sendPort = 9875
    elif(userNum == "2"):
        userSelected = True
        recvPort = 9875
        sendPort = 9876
    else:
        print("ERROR: Not a valid user. Please try again.")

#icmp = getprotobyname("icmp")
mySocket = socket(AF_INET, SOCK_DGRAM)
mySocket.bind(('', recvPort))

print("Please wait for set up.")

threeWayHandshake(mySocket)

while (not sendReady or not recvReady):
    sleep(1)

#THREAD
tR = Thread(target=TCPReceive)
tR.start()

print('\n')
print("You may now begin sending information.")
print("To disconnect, please type DONE")

while (not sendDone):
    #SOCKETS?
    #THREAD
    message = input()
    if(message == "DONE"):
        sendDone = True
    #THREAD
    tS = Thread(target=TCPSend,args=(message, False, ))
    tS.start()
    #seqNum = nextSeq
    #RECEIVE SOCKET
    #recvTime = gmtime(time())
    #receiveACK(seqNum)
    if(message == "TEST"):
        testingSender()
print("You have now been disconnected.")
