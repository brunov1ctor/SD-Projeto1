# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The Python implementation of the GRPC helloworld.Greeter server."""

from concurrent import futures
import logging
import grpc
import projeto_pb2
import projeto_pb2_grpc
import RWLock
import threading
import time
import ast

lock = RWLock.RWLock()
dicionario = {}

#-----------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------IO MODULE----------------------------------------------------------

class ThreadRead (threading.Thread):
   def __init__(self):
      threading.Thread.__init__(self)

   def run(self):
      print ("Starting ThreadRead")
      read_db()
      print ("Exiting ThreadRead")

class ThreadWrite(threading.Thread):
    def __init__(self,counter):
        threading.Thread.__init__(self)
        self.counter = counter
    def run(self):
        print('Starting ThreadWrite')
        write_db(self.counter)
        print('Exiting ThreadWrite')

def read_db():
    lock.writer_acquire
    try:
        f = open('backup.txt','r')
        global dicionario
        dicionario = ast.literal_eval(f.read())
        
    finally:
        lock.writer_release

def write_db(t):
    while True:
        time.sleep(t)
        lock.writer_acquire()
        try:
            f = open('backup.txt','w')
            f.write(str(dicionario))
            f.close()
            
        finally:
            lock.writer_release()
        
#------------------------------------------------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------------------------------
class Greeter(projeto_pb2_grpc.GreeterServicer):

    
    def set(self, request, context):            
               
        print("#############State server: ##############")
        lock.writer_acquire()

        try:
            if request.chave in dicionario:
                return projeto_pb2.MessageReply(e='ERROR',
                versao= dicionario[request.chave][0], timestamp= dicionario[request.chave][1],
                dados=dicionario[request.chave][2])
            else:
                dicionario[request.chave] = (1,request.timestamp,request.dados)
                print(dicionario)

                return projeto_pb2.MessageReply(e='SUCCESS',
                versao=None, timestamp= None,
                dados=None)
        finally:
            lock.writer_release()

    def get(self, request, context):

        lock.reader_acquire()
        try:
            if request.chave in dicionario:
                return projeto_pb2.MessageReply(e='SUCCESS',
                versao= dicionario[request.chave][0], timestamp= dicionario[request.chave][1],
                dados=dicionario[request.chave][2])

            else:
                return projeto_pb2.MessageReply(e='ERROR',
                versao=None, timestamp= None,
                dados=None)
        finally:
            lock.reader_release()
			
    def delete(self, request, context):
        if request.versao == 0:
            e,vers,ts,data = self.delete_no_vers(request,context)
        else:
            e,vers,ts,data = self.del_vers123(request,context)

        return projeto_pb2.MessageReply(e=e,
        versao=vers, timestamp= ts,
        dados=data)
			
    def delete_no_vers(self, request, context):
        print("#############State server: ##############")
        
        lock.writer_acquire()
        try:
            if request.chave in dicionario:
                e = 'SUCCESS'
                vers = dicionario[int(request.chave)][0]
                ts = dicionario[int(request.chave)][1]
                data = dicionario[int(request.chave)][2]

                del dicionario[int(request.chave)]  
                print(dicionario)

                return e,vers,ts,data
            else:
                e = 'ERROR'
                return e,None,None,None
        finally:
            lock.writer_release()
    
    def del_vers123(self, request, context):
        print("#############State server: ##############")
        
        lock.writer_acquire()
        try:
            if request.chave in dicionario:
                if  request.versao == dicionario[int(request.chave)][0]:
                    e = 'SUCCESS'
                    vers = request.versao
                    ts = dicionario[int(request.chave)][1]
                    data = dicionario[int(request.chave)][2]

                    del dicionario[request.chave]     
                    print(dicionario)

                    return e,vers,ts,data
                else:
                    print(dicionario)
                    e = 'ERROR_WV'
                    vers = dicionario[request.chave][0]
                    ts = dicionario[int(request.chave)][1]
                    data = dicionario[int(request.chave)][2]
                    
                    return e,vers,ts,data
                    
            else:
                print(dicionario)
                e = 'ERROR_NE'
                return e,None,None,None
        finally:
            lock.writer_release()

    def testandset(self, request, context):
        lock.writer_acquire()
        try:
            if request.chave in dicionario:
                if  request.versao == dicionario[request.chave][0]:
                    dicionario[request.chave] = (request.versao+1,request.timestamp,request.dados)
                    print(dicionario)

                    return projeto_pb2.MessageReply(e='SUCCESS',
                    versao=request.versao, timestamp= request.timestamp,
                    dados=request.dados)
                else:
                    print(dicionario)

                    return projeto_pb2.MessageReply(e='ERROR_WV',
                    versao= dicionario[request.chave][0], timestamp= dicionario[request.chave][1],
                    dados=dicionario[request.chave][2])
            else:
                print(dicionario)
                return projeto_pb2.MessageReply(e='ERROR_NE',
                versao=None, timestamp= None,
                dados=None)
        finally:
            lock.writer_release()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    projeto_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    thread_read = ThreadRead()
    print('Insira de quanto em quanto tempo o servidor deve salvar os dados em disco (Int) : ')
    t = int(input())
    thread_write = ThreadWrite(t)
    thread_read.setDaemon(True)
    thread_write.setDaemon(True)
    thread_read.start()
    thread_write.start()
    serve()
