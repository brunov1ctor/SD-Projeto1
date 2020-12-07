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

import datetime
import logging

import grpc

import projeto_pb2
import projeto_pb2_grpc

def menu():
  chave = 0
  valor = (0, 0, bytes(1))

  print('##########################################')
  print('Bem vindo ao Banco de Dados NoSQL rudimentar!')
  print('##########################################\n')

  # print('Conectando ao servidor…')

  # dicionario = dict() 
  channel = grpc.insecure_channel('localhost:50051')
  stub = projeto_pb2_grpc.GreeterStub(channel)
  # print('Erro de conexão: ' + str(e))
  # print('Encerrando o programa…')

  while True:
    resposta = input('Insira a opção desejada (caixa alta e baixa ignoradas):\n\nset - Para adicionar um par Chave-Valor\nget - Para pesquisar uma entrada pela chave\ndel - Para remover uma entrada do banco\ntestAndSet - Para atualizar o mapa se a versão atual no sistema corresponde à versão especificada\nSair - Para sair do programa\n')

    if resposta.lower() == 'sair':
      break

    elif resposta.lower() == 'set':
      par = menuChaveValor()
      response = stub.set(projeto_pb2.ChaveValor(chave=par[0], versao=par[1][0], timestamp=par[1][1], dados=par[1][2]))
      print("Tentou setar chave. Resposta do servidor: " + response.e)

    elif resposta.lower() == 'get':
      chave = menuChave('pesquisar')
      response = stub.get(projeto_pb2.ChaveValor(chave=chave, versao=valor[0], timestamp=valor[1], dados=valor[2]))
      print("Tentou buscar chave. Resposta do servidor: " + response.e)

    elif resposta.lower() == 'del':
      chave = menuChave('deletar')
      while True:
        resposta2 = input('Deseja especificar a versão? Responda s/n:\n')
        if resposta2.lower() in ['s','sim','yes','y']:
          versao = menuChave('deletar','versão')
          response = stub.delete(projeto_pb2.ChaveValor(chave=chave, versao=versao, timestamp=valor[1], dados=valor[2]))
          print("Tentou deletar a chave. Resposta do servidor: " + response.e)
          break
        elif resposta2.lower() in ['n','nao','não','no','n']:
          response = stub.delete(projeto_pb2.ChaveValor(chave=chave, versao=valor[0], timestamp=valor[1], dados=valor[2]))
          print("Tentou deletar a chave. Resposta do servidor: " + response.e)
          break
        print('Favor inserir uma resposta válida!')

    elif resposta.lower() == 'testandset':
      chave = menuChave('atualizar','chave antiga')
      versao = menuChave('atualizar','versão antiga')
      valor = menuChaveValor()[1]
      response = stub.testandset(projeto_pb2.ChaveValor(chave=chave, versao=versao, timestamp=valor[1], dados=valor[2]))
      print("Tentou testar e dar set na chave. Resposta do servidor: " + response.e)

    else:
      print('Favor inserir uma opção válida!')
    print('\n')
  print('\n')

def menuChaveValor(): #Pede para o usuário inserir um par chave valor corretamente formatado, e devolve numa dupla (chave, valor), onde valor = (versão, timestamp, dados)
  #return (1,(2,3,bytes(4)))
  chave = menuChave('')
  valor = menuChave('', 'versão')
  #  timestamp = menuChave('', 'timestamp')
  timestamp = int(str(datetime.datetime.now().timestamp()).replace('.', ''));
  dados = bytes(input('Insira os dados desejados:\n'),'utf-8')
  return (chave,(valor, timestamp, dados))


def menuChave(verb='', subst='chave'):
  while True:
    chave = input('Insira a ' + subst + ' que deseja ' + verb.lower() + ': ')
    try:
      chave = int(chave)
      break
    except:
      print('Insira uma' + subst + ' que seja um inteiro!')
  return chave
     
    

def run():
  # dicionario = dict() 
  # channel = grpc.insecure_channel('localhost:50051')
  # stub = projeto_pb2_grpc.GreeterStub(channel)
  # response = stub.set(projeto_pb2.ChaveValor(chave=1, versao=1, timestamp=20, dados=bytes(3)))
  # print("Tentou setar chave com " + response.e)
  # response = stub.set(projeto_pb2.ChaveValor(chave=2, versao=1, timestamp=30, dados=bytes(3)))
  # print("Tentou setar chave com " + response.e)
  # response = stub.set(projeto_pb2.ChaveValor(chave=3, versao=1, timestamp=30, dados=bytes(3)))
  # print("Tentou setar chave com " + response.e)
  # response = stub.get(projeto_pb2.ChaveValor(chave=2, versao=1, timestamp=20, dados=bytes(3)))
  # print("Buscou chave com " + response.e)
  # response = stub.del_vers(projeto_pb2.ChaveValor(chave=2, versao=2, timestamp=20, dados=bytes(3)))
  # print("Tentou deletar a chave com " + response.e)
  # response = stub.testandset(projeto_pb2.ChaveValor(chave=1, versao=1, timestamp=20, dados=bytes(3)))
  # print("Teste e set a chave com " + response.e)
  pass
  


if __name__ == '__main__':
    logging.basicConfig()
    menu()
    run()
