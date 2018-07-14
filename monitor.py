import socket
from _thread import *
import threading
import random
import pickle
import csv
import sys
from RepeatedTimer import *
from CloudStatus import *
numberOfUser=int(sys.argv[1])
connectionPool=dict()
performanceInstances=[]


def monitorInstances(performanceInstances):
 values=dict()
 options=[1,2,3,4,5,6,7,8,9,10]
 values['a']=random.choice(options)
 values['b']=random.choice(options)
 values['c']=random.choice(options)
 values['d']=random.choice(options)
 values['e']=random.choice(options)
 values['f']=random.choice(options)
 idleInstance=CloudStatus(values)
 performanceInstances.append(idleInstance)
 print("HelloWorld") #run performance algorithm and  insert into the instance array.


def updateEdgeServer(performaneInstances,controlPort):
    s=createNewSocketConnection('0.0.0.0',controlPort)
    c, addr = s.accept()
    while(True):
       ping = c.recv(1024)
       currentInstance=performanceInstances[-1]
       stream=pickle.dumps(currentInstance)
       c.send(stream)
def createClientSocket(host,port):
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s.connect((host,port))
    return s

def createChannelForUser(userId):
 dedicatedPool=dict()
 for resolution, socket in connectionPool.items():
    dedicatedPool[resolution]=socket[userId]
 return dedicatedPool

def readInstanceInfo(fileName):
 with open(fileName) as f:
    next(f)  # Skip the headeri
    tupleTemp=[]
    reader = csv.reader(f, skipinitialspace=True)
    for row in reader:
     resolution = row[0]
     startPort = row[1]
     numberOfInstance=row[2]
     tupleTemp.append(tuple([resolution, [startPort,numberOfInstance]]))
    return dict(tupleTemp)

def createNewSocketConnection(host,portt):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, portt))
        s.listen(10)
        return s

def sendForComputation(socket,frameSize, imgByte):
  socket.send(frameSize)
  status=socket.recv(1024).decode("utf-8")
  socket.send(imgByte)
  return socket.recv(1024)
 
def idlePerformance():
 values=dict()
 values['a']=1
 values['b']=2
 values['c']=4
 values['d']=5
 values['e']=6
 values['f']=7
 idleInstance=CloudStatus(values)
 return idleInstance

def interfaceCamera(port,dedicatedConnections):
    print("Edge Server are expected at port",port)
    s=createNewSocketConnection('0.0.0.0',port)
    while(True):
     c, addr = s.accept()
     while True:
      try:
        frameSize = c.recv(1024)
        imageLength= int(frameSize.decode("utf-8"))
        c.send((str('True')).encode("utf-8"))
        i=0
        chunk=b''
        while(i!=imageLength):
         imgByte= c.recv(1024)
         chunk=chunk+imgByte
         i=i+len(imgByte)
        data=pickle.loads(chunk)
        dataResult=sendForComputation(dedicatedConnections[data.resolution],frameSize,chunk)
        c.send("Helloworld".encode("utf-8"))
      except:
        break
def Main(numberOfUser,performanceInstances):
 host='127.0.0.1'
 instanceInfo=readInstanceInfo('config.csv')
 for resolution,accessConfig in instanceInfo.items(): 
  initialPort=int(accessConfig[0])
  pool=[]
  for counter in range(0, int(accessConfig[1])):
   connectorPort=initialPort+counter
   socket=createClientSocket('127.0.0.1',connectorPort)
   pool.append(socket)
  connectionPool[resolution]=pool
 userPort=9090
 controlPort=9080
 for user in range(0,numberOfUser):
  dedicatedConnections=createChannelForUser(user)
  start_new_thread(interfaceCamera,(userPort,dedicatedConnections))
  userPort=userPort+1
 idleInstance=idlePerformance()
 performanceInstances.append(idleInstance)
 start_new_thread(updateEdgeServer,(performanceInstances,9080))
 backGroundProcess = RepeatedTimer(5, monitorInstances, performanceInstances)
 print("Initilization completed") 
 input()


if __name__ == '__main__':
    Main(numberOfUser,performanceInstances)
