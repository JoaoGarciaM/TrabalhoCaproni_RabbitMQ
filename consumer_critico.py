import sys
import json
import time
import asyncio
import warnings

# Compatibilidade para Windows (Python 3.8+): o ProactorEventLoop padrão do Windows
# não implementa add_reader() usado internamente pela thread de IO do rabbitpy.
warnings.filterwarnings("ignore", category=DeprecationWarning)
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import rabbitpy

# Compatibilidade com a sintaxe do roteiro (rabbitpy 3.x)
if not hasattr(rabbitpy.Queue, 'consume_messages'):
    rabbitpy.Queue.consume_messages = rabbitpy.Queue.consume

_orig_exchange_init = rabbitpy.Exchange.__init__
def _compat_exchange_init(self, channel, name, *args, **kwargs):
    if 'type' in kwargs and 'exchange_type' not in kwargs:
        kwargs['exchange_type'] = kwargs.pop('type')
    return _orig_exchange_init(self, channel, name, *args, **kwargs)
rabbitpy.Exchange.__init__ = _compat_exchange_init

URL_AMQP = 'amqp://guest:guest@localhost:5672/%2f'


def main():
    try:
        with rabbitpy.Connection(URL_AMQP) as connection:
            with connection.channel() as channel:
                # Fair Dispatch: limita a entrega a 1 mensagem não confirmada por vez
                channel.prefetch_count(1)

                # Declara a Topic Exchange 'logs_cidade'
                exchange = rabbitpy.Exchange(channel, name='logs_cidade', type='topic')
                exchange.declare()

                # Declara a fila durável para persistência dos dados
                queue = rabbitpy.Queue(channel, name='fila_alertas_criticos', durable=True)
                queue.declare()

                # Vincula a fila à exchange para capturar qualquer routing key que termine com '.CRITICAL'
                queue.bind(exchange, routing_key='*.CRITICAL')

                print(' [*] Aguardando mensagens críticas (*.CRITICAL). Para sair pressione CTRL+C')

                # Consumo iterativo usando o gerador de mensagens do rabbitpy
                for message in queue.consume_messages():
                    data = json.loads(message.body)
                    routing_key = message.routing_key

                    print(f" [!] [ALERTA CRÍTICO RECEBIDO] {routing_key}: {data}")

                    # Simula tempo de processamento pesado
                    time.sleep(2)

                    # Confirmação manual (ACK)
                    message.ack()
                    print(f" [✓] Mensagem confirmada com sucesso (ACK enviado).")

    except KeyboardInterrupt:
        print("\n [!] Consumidor crítico finalizado pelo usuário.")
    except Exception as e:
        print(f"\n [ERRO] Ocorreu um erro na execução do consumidor crítico: {e}")


if __name__ == '__main__':
    main()
