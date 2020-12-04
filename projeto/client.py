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
"""The Python implementation of the GRPC helloworld.Greeter client."""

from __future__ import print_function
import logging

import grpc

import projeto_pb2
import projeto_pb2_grpc

def menu():
    print('Escolha uma opcao:')
     
    

def run():
  dicionario = dict() 
  channel = grpc.insecure_channel('localhost:50051')
  stub = projeto_pb2_grpc.GreeterStub(channel)
  response = stub.set(projeto_pb2.ChaveValor(chave=1, versao=1, timestamp=20, dados=bytes(3)))
  print("Tentou setar chave com " + response.e)
  response = stub.set(projeto_pb2.ChaveValor(chave=2, versao=1, timestamp=30, dados=bytes(3)))
  print("Tentou setar chave com " + response.e)
  response = stub.set(projeto_pb2.ChaveValor(chave=3, versao=1, timestamp=30, dados=bytes(3)))
  print("Tentou setar chave com " + response.e)
  response = stub.get(projeto_pb2.ChaveValor(chave=2, versao=1, timestamp=20, dados=bytes(3)))
  print("Buscou chave com " + response.e)
  response = stub.del_vers(projeto_pb2.ChaveValor(chave=2, versao=2, timestamp=20, dados=bytes(3)))
  print("Tentou deletar a chave com " + response.e)
  response = stub.testandset(projeto_pb2.ChaveValor(chave=1, versao=1, timestamp=20, dados=bytes(3)))
  print("Teste e set a chave com " + response.e)
  


if __name__ == '__main__':
    logging.basicConfig()
    #menu()
    run()
