import sys
import time
import json
import asyncio
import warnings

# Compatibilidade para Windows (Python 3.8+): o ProactorEventLoop padrão do Windows
# não implementa add_reader() usado internamente pela thread de IO do rabbitpy.
warnings.filterwarnings("ignore", category=DeprecationWarning)
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import rabbitpy

# Compatibilidade de parâmetros com o roteiro do professor (suporte a type='topic')
_orig_exchange_init = rabbitpy.Exchange.__init__
def _compat_exchange_init(self, channel, name, *args, **kwargs):
    if 'type' in kwargs and 'exchange_type' not in kwargs:
        kwargs['exchange_type'] = kwargs.pop('type')
    return _orig_exchange_init(self, channel, name, *args, **kwargs)
rabbitpy.Exchange.__init__ = _compat_exchange_init

URL_AMQP = 'amqp://guest:guest@localhost:5672/%2f'


def publicar_mensagem(routing_key='energia.CRITICAL', payload=None, raw_body=None):
    """
    Publica uma mensagem na Topic Exchange 'logs_cidade'.
    Suporta payload formatado em JSON ou envio de raw_body (para teste de mensagens inválidas).
    """
    if payload is None and raw_body is None:
        payload = {
            "timestamp": time.time(),
            "modulo": "sensor_temperatura_04",
            "leitura": 89.5,
            "status": "SOBREAQUECIMENTO"
        }

    body = raw_body if raw_body is not None else json.dumps(payload)

    with rabbitpy.Connection(URL_AMQP) as connection:
        with connection.channel() as channel:
            # Instancia e declara a Topic Exchange (conforme o roteiro do exercício 1)
            exchange = rabbitpy.Exchange(channel, name='logs_cidade', type='topic')
            exchange.declare()

            # Cria o objeto Message com propriedades de persistência
            message = rabbitpy.Message(
                channel,
                body_value=body,
                properties={
                    'content_type': 'application/json',
                    'delivery_mode': 2  # Mensagem persistente em disco
                }
            )

            # Publica para a exchange informando a routing key
            message.publish(exchange=exchange, routing_key=routing_key)
            print(f" [x] Mensagem enviada para '{routing_key}': {body}")


if __name__ == '__main__':
    # Permite especificar cenários de teste via linha de comando
    if len(sys.argv) > 1:
        opcao = sys.argv[1].lower()
        if opcao == 'critico' or opcao == '1':
            print(">> Enviando alerta crítico de energia (capturado por *.CRITICAL)...")
            publicar_mensagem(routing_key='energia.CRITICAL')
        elif opcao == 'trafego' or opcao == '2':
            print(">> Enviando log de tráfego regular (capturado por trafego.*)...")
            payload = {
                "timestamp": time.time(),
                "modulo": "radar_avenida_central",
                "veiculo_id": "ABC-1234",
                "velocidade_kmh": 78.2,
                "status": "FLUXO_NORMAL"
            }
            publicar_mensagem(routing_key='trafego.velocidade', payload=payload)
        elif opcao == 'trafego_critico' or opcao == '3':
            print(">> Enviando alerta crítico de tráfego (capturado por AMBOS os consumidores!)...")
            payload = {
                "timestamp": time.time(),
                "modulo": "sensor_tunel_leste",
                "leitura": "FOGO_DETECTADO",
                "status": "CRITICO_BLOQUEIO_TOTAL"
            }
            publicar_mensagem(routing_key='trafego.CRITICAL', payload=payload)
        elif opcao == 'invalido' or opcao == '4':
            print(">> Enviando payload malformado para testar NACK / JSONDecodeError...")
            publicar_mensagem(routing_key='trafego.sensor_danificado', raw_body="{ malformed_json: not_valid ")
        else:
            print(f"Uso: python publisher.py [critico | trafego | trafego_critico | invalido]")
    else:
        # Execução padrão do Exercício 1
        publicar_mensagem()
