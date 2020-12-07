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

#Parâmetro cliente é usado para diferenciar varios clientes simultaneos.
def testes(cliente=1):

    print('##########################################')
    print('Iniciando bateria de testes')
    print('##########################################')

    channel = grpc.insecure_channel('localhost:50051')
    stub = projeto_pb2_grpc.GreeterStub(channel)

    print("Testando estados das APIs")

    # Set com sucesso
    chave = cliente*10_000 + 1_123
    resposta = stub.set(projeto_pb2.ChaveValor(
        chave=chave, timestamp=12, dados=bytes(
            f"Dados da chave {chave}", "utf-8")))
    assert resposta.e == "SUCCESS", f"Inserção da chave {chave} falhou"
    assert verifica_atributos(resposta),\
        f"Reposta da inserção da chave {chave} possui atributos inesperados"

    # Set com chave repetida
    resposta = stub.set(projeto_pb2.ChaveValor(
        chave=chave, timestamp=123, dados=bytes(
            f"Dados da chave {chave} repetida", "utf-8")))
    assert resposta.e == "ERROR",\
        f"Inserção da chave repetida {chave} não obteve o retorno ERROR esperado"
    assert verifica_atributos(resposta, 1, 12, bytes(f"Dados da chave {chave}", "utf-8")),\
        f"Reposta da inserção da chave repetida {chave} tem atributos inesperados"

    # Get com sucesso
    resposta = stub.get(projeto_pb2.ChaveValor(chave=chave))
    assert resposta.e == "SUCCESS", f"Não foi possível selecionar a chave {chave}"
    assert verifica_atributos(resposta, 1, 12, bytes(f"Dados da chave {chave}", "utf-8")),\
        f"Reposta do seleção da chave {chave} possui atributos inesperados"

    # Get com chave inexistente
    resposta = stub.get(projeto_pb2.ChaveValor(chave=789))
    assert resposta.e == "ERROR",\
        "Seleção da chave 789 não obteve o retorno ERROR esperado"
    assert verifica_atributos(resposta),\
        "Reposta da seleção da chave 789 possui atributos inesperados"

    # TestAndSet com sucesso
    resposta = stub.testandset(projeto_pb2.ChaveValor(
        chave=chave, versao=1, timestamp=1234, dados=bytes(
            f"Novos dados da chave {chave}", "utf-8")))
    assert resposta.e == "SUCCESS", f"Sobrescrita da chave {chave} falhou"
    assert verifica_atributos(resposta, 1, 1234, bytes(f"Novos dados da chave {chave}", "utf-8")),\
        f"Reposta da sobrescrita da chave {chave} possui atributos inesperados"

    # TestAndSet com versão desencontrada
    resposta = stub.testandset(projeto_pb2.ChaveValor(
        chave=chave, versao=1, timestamp=12345, dados=bytes(
            f"Novos novos dados da chave {chave}", "utf-8")))
    assert resposta.e == "ERROR_WV",\
        f"Sobrescrita da chave {chave}v1 não obteve o retorno ERROR_WV esperado"
    assert verifica_atributos(resposta, 2, 1234, bytes(f"Novos dados da chave {chave}", "utf-8")),\
        f"Reposta da sobrescrita da chave {chave}v1 possui atributos inesperados"

    # TestAndSet com chave inexistente
    resposta = stub.testandset(projeto_pb2.ChaveValor(
        chave=789, versao=1, timestamp=12, dados=bytes(
            "Novos dados da chave 789", "utf-8")))
    assert resposta.e == "ERROR_NE",\
        "Sobrescrita da chave 789 não obteve o retorno ERROR_NE esperado"
    assert verifica_atributos(resposta),\
        "Reposta da sobrescrita da chave 789 possui atributos inesperados"

    # Delete sem versão com chave inexistente
    resposta = stub.delete(projeto_pb2.ChaveValor(chave=789))
    assert resposta.e == "ERROR",\
        "Exclusão da chave 789 não obteve o retorno ERROR esperado"
    assert verifica_atributos(resposta),\
        "Exclusão da chave 789 possui atributos inesperados"

    # Inserindo para excluir
    chave += 1
    resposta = stub.set(projeto_pb2.ChaveValor(
        chave=chave, timestamp=21, dados=bytes(
            f"Dados da chave {chave}", "utf-8")))
    assert resposta.e == "SUCCESS", f"Inserção da chave {chave} falhou"
    assert verifica_atributos(resposta),\
        f"Reposta da inserção da chave {chave} possui atributos inesperados"

    # Verificando inserção
    resposta = stub.get(projeto_pb2.ChaveValor(chave=chave))
    assert resposta.e == "SUCCESS", f"Não foi possível selecionar a chave {chave}"
    assert verifica_atributos(resposta, 1, 21, bytes(f"Dados da chave {chave}", "utf-8")),\
        f"Reposta do seleção da chave {chave} possui atributos inesperados"

    # Delete sem versão com sucesso
    resposta = stub.delete(projeto_pb2.ChaveValor(chave=chave))
    assert resposta.e == "SUCCESS", f"Exclusão da chave {chave} falhou"
    assert verifica_atributos(resposta, 1, 21, bytes(f"Dados da chave {chave}", "utf-8")),\
        f"Reposta da exclusão da chave {chave} possui atributos inesperados"

    # Verificando exclusão
    resposta = stub.get(projeto_pb2.ChaveValor(chave=chave))
    assert resposta.e == "ERROR",\
        f"Seleção da chave {chave} não obteve o retorno ERROR esperado"
    assert verifica_atributos(resposta),\
        f"Reposta da seleção da chave {chave} possui atributos inesperados"

    # Delete com versão com chave inexistente
    resposta = stub.delete(projeto_pb2.ChaveValor(chave=789, versao=1))
    assert resposta.e == "ERROR_NE",\
        "Exclusão da chave 789 não obteve o retorno ERROR_NE esperado"
    assert verifica_atributos(resposta),\
        "Reposta da exclusão da chave 789 possui atributos inesperados"

    # Inserindo para excluir
    chave += 1
    resposta = stub.set(projeto_pb2.ChaveValor(
        chave=chave, timestamp=21, dados=bytes(
            f"Dados da chave {chave}", "utf-8")))
    assert resposta.e == "SUCCESS", f"Inserção da chave {chave} falhou"
    assert verifica_atributos(resposta),\
        f"Reposta da inserção da chave {chave} possui atributos inesperados"

    # Verificando inserção
    resposta = stub.get(projeto_pb2.ChaveValor(chave=chave))
    assert resposta.e == "SUCCESS", f"Não foi possível selecionar a chave {chave}"
    assert verifica_atributos(resposta, 1, 21, bytes(f"Dados da chave {chave}", "utf-8")),\
        f"Reposta do seleção da chave {chave} possui atributos inesperados"

    # Delete com versão desencontrada
    resposta = stub.delete(projeto_pb2.ChaveValor(chave=chave, versao=321))
    assert resposta.e == "ERROR_WV",\
        f"Exclusão da chave {chave} não obteve o retorno ERROR_WV esperado"
    assert verifica_atributos(resposta, 1,21, bytes(f"Dados da chave {chave}", "utf-8")),\
        f"Reposta da exclusão da chave {chave} possui atributos inesperados"

    # Delete com versão com sucesso
    resposta = stub.delete(projeto_pb2.ChaveValor(chave=chave, versao=1))
    assert resposta.e == "SUCCESS", f"Exclusão da chave {chave} falhou"
    assert verifica_atributos(resposta, 1, 21, bytes(f"Dados da chave {chave}", "utf-8")),\
        f"Reposta da exclusão da chave {chave} possui atributos inesperados"

    # Verificando exclusão
    resposta = stub.get(projeto_pb2.ChaveValor(chave=chave))
    assert resposta.e == "ERROR",\
        f"Seleção da chave {chave} não obteve o retorno ERROR esperado"
    assert verifica_atributos(resposta),\
        f"Reposta da seleção da chave {chave} possui atributos inesperados"

    print("realizando testes de estresse")

    print("Inserção")

    # Teste de estresse
    # Inserção
    for i in range(1000):
        p = int(i/10)
        chave = cliente*10_000 + 2000 + i
        print("\r" + "#"*p + " "*(100-p) + "|", end="")
        resposta = stub.set(projeto_pb2.ChaveValor(
            chave=chave, timestamp=456, dados=bytes(
                f"Dados de estresse da chave {chave}", "utf-8")))
        assert resposta.e == "SUCCESS", f"Inserção da chave {chave} falhou"
        assert verifica_atributos(resposta),\
            f"Reposta da inserção da chave {chave} tem atributos inesperados"

    print("\nAtualização")

    # Atualização
    for i in range(1000):
        p = int(i/10)
        chave = cliente*10_000 + 2000 + i
        print("\r" + "#"*p + " "*(100-p) + "|", end="")
        resposta = stub.testandset(projeto_pb2.ChaveValor(
            chave=chave, versao=1, timestamp=789, dados=bytes(
                f"Novos dados da chave {chave}", "utf-8")))
        assert resposta.e == "SUCCESS", f"Sobrescrita da chave {chave} falhou"
        assert verifica_atributos(resposta, 1, 789, bytes(
                f"Novos dados da chave {chave}", "utf-8")),\
            f"Reposta da sobrescrita da chave {chave} tem atributos inesperados"

    print("\nLeitura")

    # Leitura
    for i in range(1000):
        p = int(i/10)
        chave = cliente*10_000 + 2000 + i
        print("\r" + "#"*p + " "*(100-p) + "|", end="")
        resposta = stub.get(projeto_pb2.ChaveValor(chave=chave))
        assert resposta.e == "SUCCESS", f"seleção da chave {chave} falhou"
        assert verifica_atributos(resposta, 2, 789, bytes(
                f"Novos dados da chave {chave}", "utf-8")),\
            f"Reposta da seleção da chave {chave} tem atributos inesperados"


def verifica_atributos(resposta, versao=0, timestamp=0, dados=b''):
    """ Verifica se a resposta possui atributos
        versão, timestamp e dados corretos """
    if resposta.versao != versao or\
        resposta.timestamp != timestamp or\
            resposta.dados != dados:
        return False
    return True


if __name__ == '__main__':
    logging.basicConfig()
    from sys import argv
    cliente = -1
    if len(argv) > 1:
        cliente = int(argv[1])
        print(f"Iniciando como cliente {cliente}")
    else:
        cliente = 1
        print(f"Argumento de cliente não passado. Iniciando como cliente 1")
    try:
        testes(cliente)
        print("\nTodos os testes obtiveram sucesso")
    except AssertionError as e:
        print("\nUm teste falhou.\nDetalhes da exceção:\n", e)
