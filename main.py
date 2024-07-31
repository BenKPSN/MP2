from socket import *
from threading import *
from time import *
import struct
import pickle
import random

#A metric ton of global variables.
recvPort = 9876
sendPort = 9875
nextSeq = 5
expectedSeq = 5
base = 0
timeOutTime = 1
estTime = 0
devTime = 0
isACKList = []
timeList = []
unackedMessages = []
maxSeqNum = 87
rwnd = 99
timesACK = 0
recvRWND = 15
testDone = 0
testEndSeq = 0
cwnd = 1
numTransit = 0
sendReady = False
recvReady = False
sendDone = False
recvDone = False
sendTest = False
recvTest = False


#Setting up three important lists.
for i in range(maxSeqNum):

    #This list will keep track of if messages have been ACKed or not.
    isACKList.append(0)

    #This list will keep track of the time a message was sent.
    timeList.append(0)

    #This list will keep track of messages to resend (so the ones that weren't ACKED right.)
    unackedMessages.append([])

#Creates a segment to send through the socket.
def createSegment(message, seqNum, isACK, isSYN):
    global recvPort
    global maxSeqNum
    global rwnd
    global testDone
    sourcePort = recvPort
    destPort = sendPort
    ackNum = seqNum + 1
    ackNum = ackNum % maxSeqNum
    checksum = Checksum(message)
    sendSeqNum = seqNum
    ACK = 0
    if (isACK):
        sendSeqNum = 0
        ACK = 1
        ackNum = seqNum
    SYN = 0
    if (isSYN):
        SYN = 1
    
    #This is basically the header values for TCP, and the message is at index 9.
    toSend = ['localhost', sourcePort, destPort, sendSeqNum, ackNum, rwnd, checksum, SYN, ACK, message, testDone]
    return toSend

#Each message will have a timer attached to them. When it's up, it will resend that message.
def timerACK(message, seqNum, testNum, messageTestNum):
    notACK = True
    global isACKList
    global timeOutTime
    global mySocket
    global recvPort
    global sendPort

    #This goes until the message has been ACKed.
    while(notACK):
        sleep(timeOutTime)

        #If the message has not been ACKed and we have not closed the connection, we resend the message.
        if(isACKList[seqNum] == 0 and not sendDone):
            mySocket.sendto(pickle.dumps(message), ('localhost', sendPort))
        
        #Otherwise, we note we're done.
        else:
            notACK = False

#Sends a message.
def TCPSend(message, isACK, cwnd, transitNum):
    global nextSeq
    global timeList
    global isACKList
    global mySocket
    global recvPort
    global sendPort
    global sendDone
    global maxSeqNum
    global sendTest
    #global cwnd
    global numTransit

    #This value is for randomly choosing if a message fails to send or gets lost.
    randomChance = random.randint(1,51)

    #FIX THIS: SHOULD STOP MESSAGES FROM SENDING WHEN WINDOW IS FULL
    #ALSO, MAYBE HAVE THE MESSAGES SAVED FOR LATER? IDK
    while(transitNum >= cwnd):
        sleep(0.1)
    
    #Create and send the segment.
    toSend = createSegment(message, nextSeq, isACK, False)
    #Random chance to fail to send. This counts as both taking too long and getting lost.
    if(randomChance != 1 and randomChance != 2 and randomChance != 3):
        mySocket.sendto(pickle.dumps(toSend), ('localhost', sendPort))
    
    #Set up everything the message needs. Time it was sent, the fact it's unACKed, etc.
    currSeq = nextSeq
    timeList[currSeq] = time()
    isACKList[currSeq] = 0
    #Random chance to never send, counting as getting lost.
    if(randomChance == 1):
        isACKList[currSeq] = 1
    numTransit += 1
    unackedMessages[currSeq] = toSend
    
    #Thread to handle the timer for that message.
    tA = Thread(target=timerACK,args=(toSend,currSeq,0,0))
    tA.start()
    nextSeq += 1
    nextSeq = nextSeq % maxSeqNum

    #This sets up everything for closing connection. All messages will no longer be sent.
    if(message == "DONE"):
        sendDone = True
        for i in range(maxSeqNum):
            isACKList[i] = 1
    
    #Triple Duplicate ACK Test preparation.
    if(message == "TEST"):
        sendTest = True

#Calculation for the timeout. Same as in class.
def timeoutCalc(sendTime, timeReceived):
    global timeOutTime
    global estTime
    global devTime
    measureTime = timeReceived - sendTime
    devTime = 0.75 * devTime + 0.25 * abs(measureTime - estTime)
    estTime = 0.875 * estTime + 0.125 * measureTime
    timeOutTime = estTime + 4 * devTime

#This handles when an ACK is received, both expected and not.
"""def receiveACK(seqNum, isTest):
    global isACKList
    global base
    global timeList
    global recvTime
    global maxSeqNum
    global lastMessage
    global testEndSeq
    global recvRWND
    global numTransit
    global cwnd
    global timesACK
    timeoutCalc(timeList[seqNum], recvTime)

    #For checking the ACKs received when testing.
    if(isTest):
        print(seqNum)
        testEndSeq = seqNum
    
    #If this is true, we have an ACK we expected. Stop resending the message and increase base.
    if(seqNum > base):
        timesACK = 0
        base = seqNum
        base = base % maxSeqNum
        isACKList[seqNum] = 1
        numTransit -= 1
        if(recvRWND > cwnd):
            cwnd += 1
    
    #Otherwise....
    else:

        #For checking amount of duplicate ACKs.
        if(seqNum == base):
            timesACK += 1
        
        #Perform Triple Duplicate ACK. As this is a loss, we also lower CWND.
        if(timesACK >= 3):
            #print("TRIPLE")
            timesACK = 0
            cwnd = cwnd // 2
            mySocket.sendto(pickle.dumps(unackedMessages[base]), ('localhost', sendPort))
            timeList[seqNum] = time()
"""

# a very simple checksum, will replace with a two-dimensional parity scheme when I figure it out
def Checksum(message):
    csum = 0
    for i in range(len(message)):
        #csum = csum + message[i]
        csum += 1

    res = 255 - csum % 256
    return res

#An always on function that constantly listens to messages.
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
    global recvRWND
    global testDone
    global base
    global timeList
    global recvTime
    global testEndSeq
    global maxSeqNum
    global isACKList
    global numTransit
    global cwnd
    global unackedMessages
    
    #If first packet fails, this acknowledges it.
    ackToSend = createSegment("", 0, True, False)
    currentlyTesting = False
    keepSeq = 0
    ackAmount = timesACK

    #The main listener.
    while (not recvDone):
        
        #Receive a message.
        message, clientAddress = mySocket.recvfrom(recvPort)
        
        #Get all info about it.
        recvTime = time()
        message = pickle.loads(message)
        isACK = message[8]
        ackValue = message[4]
        receivedSeqNum = message[3]
        calcChecksum = Checksum(message[9])
        receivedChecksum = message[6]

        #If checksums match....
        if(calcChecksum == receivedChecksum):

            #If it's an ACK (and we aren't closing connection)....
            if (isACK == 1 and not sendDone):

                #This is a weird thing but it makes sure the ACK printing in receiveACK() works.
                if(testDone != 0):
                    currentlyTesting = True
                else:
                    currentlyTesting = False
                
                #if(ackValue == base)
                
                #Handle the ACK.
                #receiveACK(ackValue, currentlyTesting, ackAmount)
                timeoutCalc(timeList[ackValue], recvTime)

                #For checking the ACKs received when testing.
                if(currentlyTesting):
                    print(ackValue)
                    testEndSeq = ackValue
    
                #If this is true, we have an ACK we expected. Stop resending the message and increase base.
                if(ackValue > base):
                    ackAmount = 0
                    base = ackValue
                    base = base % maxSeqNum
                    isACKList[ackValue] = 1
                    numTransit -= 1
                    if(recvRWND > cwnd):
                        cwnd += 1
    
                #Otherwise....
                else:

                    #For checking amount of duplicate ACKs.
                    if(ackValue == base):
                        ackAmount += 1
                        print('DUPLICATE ACK RECEIVED')
        
                    #Perform Triple Duplicate ACK. As this is a loss, we also lower CWND.
                    if(ackAmount >= 3):
                        #print("TRIPLE DUPLICATE")
                        ackAmount = 0
                        cwnd = cwnd // 2
                        firstUnACK = (base + 1) % maxSeqNum
                        mySocket.sendto(pickle.dumps(unackedMessages[firstUnACK]), ('localhost', sendPort))
                        timeList[ackValue] = time()
            
            #If the receiver acknowledged our disconnect....
            elif (message[9] == "DONE" and sendDone):
                #print("Receiver closed")

                #Send an ACK.
                ackToSend = createSegment("DONE", receivedSeqNum, True, False)
                sleep(1)
                mySocket.sendto(pickle.dumps(ackToSend), ('localhost', sendPort))
                sleep(timeOutTime*2)

                #Fully disconnect.
                recvDone = True
            
            #If we got the right sequence number....
            elif (expectedSeq == receivedSeqNum):

                #Grab other useful message info.
                testDone = message[10]
                recvRWND = message[5]
                #If the message was the sender disconnecting....
                if(message[9] == "DONE"):
                    print("Sender has disconnected. Press enter to shut down.")
                    sendDone = True
                    recvDone = True

                    #Send an ACK.
                    #ackToSend = createSegment("", receivedSeqNum, True, False)
                    #mySocket.sendto(pickle.dumps(ackToSend), ('localhost', sendPort))

                    #Send our own message to disconnect.
                    #print(receivedSeqNum)
                    ackToLookFor = receivedSeqNum + 1
                    doneMessage = createSegment("DONE", ackToLookFor, False, False)
                    mySocket.sendto(pickle.dumps(doneMessage), ('localhost', sendPort))
                    #print("waiting for last ACK")
                    finalACK = False

                    #Now we wait for the ACK.
                    while(not finalACK):
                        message, clientAddress = mySocket.recvfrom(recvPort)
                        message = pickle.loads(message)
                        isACK = message[8]
                        ackValue = message[4]
                        #print(ackValue)
                        #print(ackToLookFor)

                        #If we received it, we can fully disconnect.
                        if(isACK == 1 and ackValue == ackToLookFor):
                            #print("Received final ACK")
                            finalACK = True
                
                #If the message was the beginning of testing Triple Duplicate ACK....
                elif(message[9] == "TEST"):
                    sendTest = True
                    recvTest = True
                    keepSeq = expectedSeq
                    expectedSeq = 1

                    #Send an ACK.
                    ackToSend = createSegment("TEST", receivedSeqNum, True, False)
                    testDone = 1
                    mySocket.sendto(pickle.dumps(ackToSend), ('localhost', sendPort))
                
                #Check if the test is done.
                elif(expectedSeq == 11 and recvTest):
                    print("Testing done!")
                    expectedSeq = keepSeq
                    sendTest = False
                    recvTest = False
                
                #Otherwise....
                else:
                    
                    #Print the message.
                    print(message[9])
                    #Check if last number in the test seq was recieved
                    if ((testDone == 1) and (message[9] == "10")):
                        testDone = 0

                    #Send an ACK.
                    ackToSend = createSegment("", receivedSeqNum, True, False)
                    mySocket.sendto(pickle.dumps(ackToSend), ('localhost', sendPort))
                    expectedSeq += 1
            
            #Otherwise....
            else:
                #Send the last ACK we sent.
                mySocket.sendto(pickle.dumps(ackToSend), ('localhost', sendPort))
        
        #Otherwise....
        else:
            #Send the last ACK we sent.
            mySocket.sendto(pickle.dumps(ackToSend), ('localhost', sendPort))

#This is for the sender's side of the Three Way Handshake.
def handshakeSend(sock):
    global nextSeq
    global recvPort
    global sendPort
    global sendReady
    
    #Send the handshake.
    handshakeMessage = createSegment("", nextSeq, False, True)
    sock.sendto(pickle.dumps(handshakeMessage), ('localhost', sendPort))

    #Get all info for that message.
    currSeq = nextSeq
    timeList[currSeq] = time()
    isACKList[currSeq] = 0

    #Start the timer.
    tA = Thread(target=timerACK,args=(handshakeMessage,currSeq,0,0))
    tA.start()
    synACKGot = False

    #Wait until we receive the ACK for the handshake.
    while (not synACKGot):
        response, clientAddress = sock.recvfrom(recvPort)
        recvTime = time()
        response = pickle.loads(response)
        recvACK = response[8]
        recvSYN = response[7]

        #If it's the correct ACK, we move on.
        if(recvACK == 1 and recvSYN == 1):
            synACKGot = True
            isACKList[currSeq] = 1
            timeoutCalc(timeList[currSeq], recvTime)
    
    #Send the ACK and get ready to begin.
    handshakeResponse = createSegment("", nextSeq, True, False)
    sock.sendto(pickle.dumps(handshakeResponse), ('localhost', sendPort))
    sendReady = True

#This is for the receiver's side of the Three Way Handshake.
def handshakeRecv(sock):
    global nextSeq
    global recvPort
    global sendPort
    global recvReady
    synGot = False
    
    #Wait for a handshake message.
    while (not synGot):
        message, clientAddress = sock.recvfrom(recvPort)
        message = pickle.loads(message)
        recvSYN = message[7]
        recvACK = message[8]

        #Checking if we got the right message.
        if(recvSYN == 1 and recvACK == 0):
            synGot = True
    
    #Send an ACK.
    handshakeACK = createSegment("", nextSeq, True, True)
    sock.sendto(pickle.dumps(handshakeACK), ('localhost', sendPort))

    #Get all info for that message.
    currSeq = nextSeq
    timeList[currSeq] = time()
    isACKList[currSeq] = 0
    
    #Start the timer.
    tA = Thread(target=timerACK,args=(handshakeACK,currSeq,0,0))
    tA.start()
    ackGot = False
    
    #We need an ACK from the sender, so we wait for one.
    while (not ackGot):
        response, clientAddress = sock.recvfrom(recvPort)
        recvTime = time()
        response = pickle.loads(response)
        recvACK = response[8]
        recvSYN = response[7]

        #Check if it's the right ACK.
        if(recvACK == 1 and recvSYN == 0):
            ackGot = True
            isACKList[currSeq] = 1
            timeoutCalc(timeList[currSeq], recvTime)
    
    #We're now ready to receive.
    recvReady = True

#This just starts both sides of the Handshake.
def threeWayHandshake(sock):

    #Important to make sure the timing is right.
    sleep(5)
    hS = Thread(target=handshakeSend,args=(sock, ))
    hR = Thread(target=handshakeRecv,args=(sock, ))
    hS.start()
    hR.start()

#Triple Duplicate ACK testing.
def testingSender():
    global sendTest
    global recvTest
    global mySocket
    global sendPort
    global recvPort
    global testEndSeq
    global testDone
    global base
    global cwnd
    global numTransit

    #Wait for an ACK from the receiver before beginning.
    while(sendTest and not recvTest):
        message, clientAddress = mySocket.recvfrom(recvPort)
        message = pickle.loads(message)
        isACK = message[8]
        ackValue = message[4]
        calcChecksum = Checksum(message[9])
        receivedChecksum = message[6]
        if(receivedChecksum == calcChecksum and isACK == 1 and message[9] == "TEST"):
            recvTest = True
    print("We will now begin testing Triple Duplicate ACK.")
    print('\n')

    #Save base's value, since we're going to change it.
    keepBase = base
    base = 0
    
    print("10 messages will be sent, consisting of the numbers 1 through 10.")
    print("Message #6, however, will be forced to not send, nor resend.")
    print("It will only resend when we get the Triple Duplicate ACK.")
    sleep(3)
    
    testDone = 1
    testEndSeq = 0
    testSend = 1

    #Send the 10 messages. 6 is lost.
    while(testSend != 11):
        toSend = createSegment(str(testSend), testSend, False, False)
        if(testSend != 6):
            mySocket.sendto(pickle.dumps(toSend), ('localhost', sendPort))
        currSeq = testSend
        timeList[currSeq] = time()
        isACKList[currSeq] = 0
        if(testSend == 6):
            isACKList[currSeq] = 1
        unackedMessages[currSeq] = toSend
        tA = Thread(target=timerACK,args=(toSend,currSeq,1,testSend))
        tA.start()
        testSend += 1

    #Waiting until message 10 is ACKed.
    while(testEndSeq < 10):
        sleep(0.5)
    testEndSeq = 0

    #Sending one last message to tell receiver we're done.
    toSend = createSegment(str(testSend), testSend, False, False)
    mySocket.sendto(pickle.dumps(toSend), ('localhost', sendPort))
    currSeq = testSend
    timeList[currSeq] = time()
    isACKList[currSeq] = 0
    isACKList[currSeq] = 1
    unackedMessages[currSeq] = toSend
    tA = Thread(target=timerACK,args=(toSend,currSeq,1,testSend))
    tA.start()

    #This is to reset the receiver and tell it we're done.
    testDone = 0
    TCPSend("", False, cwnd, numTransit)

    print("Test complete! You may begin typing once more.")
    print("\n")
    sendTest = False
    recvTest = False
    base = keepBase

#Waiting for the sender to be done testing.
userSelected = False

#Select a user.
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


#Socket.
mySocket = socket(AF_INET, SOCK_DGRAM)
mySocket.bind(('', recvPort))

print("Please wait for set up.")

threeWayHandshake(mySocket)

#Wait for both sides to be ready.
while (not sendReady or not recvReady):
    sleep(1)

#Thread for the receiver.
tR = Thread(target=TCPReceive)
tR.start()

print('\n')
print("You may now begin sending information.")
print("To test the Triple Duplicate ACK functionality, please type TEST")
print("\n")
print("To disconnect, please type DONE")

#Lasts until we either type DONE
while (not sendDone):
    message = input()
    if(message == "DONE"):
        sendDone = True
    
    tS = Thread(target=TCPSend,args=(message, False, cwnd, numTransit))
    tS.start()
    if(message == "TEST"):
        testingSender()


