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

dicionario = {}


class Greeter(projeto_pb2_grpc.GreeterServicer):

    
    def set(self, request, context):
        dicionario[int(request.chave)] = (request.versao,request.timestamp,request.dados)             
               
        print("#############State server: ##############")
        if request.chave in dicionario: 
            print(dicionario)            
            #Armazenamento em disco com recuperação de dados no caso de falhas:
            arquivo = open('backup.txt','w')
            arquivo.write(str(dicionario)+"\n")
            arquivo.close()
            return projeto_pb2.MessageReply(e='SUCCESS',
            versao=request.versao, timestamp= request.timestamp,
            dados=request.dados)
        else:
            return projeto_pb2.MessageReply(e='ERROR',
            versao=request.versao, timestamp= request.timestamp,
            dados=request.dados)
            
    def get(self, request, context):
        if request.chave in dicionario:
            return projeto_pb2.MessageReply(e='SUCCESS',
            versao=None, timestamp= None,
            dados=None)
        else:
            return projeto_pb2.MessageReply(e='ERROR',
            versao=request.versao, timestamp= request.timestamp,
            dados=request.dados)
    
    def delete(self, request, context):
        print("#############State server: ##############")
        
        if request.chave in dicionario:            
            del dicionario[int(request.chave)]  
            print(dicionario)
            #Armazenamento em disco com recuperação de dados no caso de falhas:
            arquivo = open('backup.txt','w')
            arquivo.write(str(dicionario)+"\n")
            arquivo.close()
            return projeto_pb2.MessageReply(e='SUCCESS',
            versao=None, timestamp= None,
            dados=None)
        else:
            return projeto_pb2.MessageReply(e='ERROR',
            versao=request.versao, timestamp= request.timestamp,
            dados=request.dados)
    
    def del_vers(self, request, context):
        print("#############State server: ##############")
        
        if request.chave in dicionario:
            if  request.versao == dicionario[int(request.chave)][0]:
                del dicionario[int(request.chave)]     
                print(dicionario)
                #Armazenamento em disco com recuperação de dados no caso de falhas:
                arquivo = open('backup.txt','w')
                arquivo.write(str(dicionario)+"\n")
                arquivo.close()
                return projeto_pb2.MessageReply(e='SUCCESS',
                versao=None, timestamp= None,
                dados=None)
            else:
                print(dicionario)
                return projeto_pb2.MessageReply(e='ERROR_WV',
                versao=request.versao, timestamp= request.timestamp,
                dados=request.dados)    
                  
        else:
            print(dicionario)
            return projeto_pb2.MessageReply(e='ERROR_NE',
            versao=request.versao, timestamp= request.timestamp,
            dados=request.dados)
    
    def testandset(self, request, context):
        if request.chave in dicionario:
            if  request.versao > dicionario[int(request.chave)][0]:
                dicionario[int(request.chave)] = (request.versao,request.timestamp,request.dados)
                print(dicionario)
                #Armazenamento em disco com recuperação de dados no caso de falhas:
                arquivo = open('backup.txt','w')
                arquivo.write(str(dicionario)+"\n")
                arquivo.close()
                return projeto_pb2.MessageReply(e='SUCCESS',
                versao=None, timestamp= None,
                dados=None)
            else:
                print(dicionario)
                return projeto_pb2.MessageReply(e='ERROR_WV',
                versao=request.versao, timestamp= request.timestamp,
                dados=request.dados)    
                  
        else:
            print(dicionario)
            return projeto_pb2.MessageReply(e='ERROR_NE',
            versao=request.versao, timestamp= request.timestamp,
            dados=request.dados)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    projeto_pb2_grpc.add_GreeterServicer_to_server(Greeter(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
