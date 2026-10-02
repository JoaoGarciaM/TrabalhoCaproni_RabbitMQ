import sys
import json
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
                # Declara a Topic Exchange 'logs_cidade'
                exchange = rabbitpy.Exchange(channel, name='logs_cidade', type='topic')
                exchange.declare()

                # Declara a fila durável para logs de tráfego
                queue = rabbitpy.Queue(channel, name='fila_auditoria_trafego', durable=True)
                queue.declare()

                # Vincula a fila para capturar mensagens com prefixo 'trafego.' e qualquer palavra seguinte
                queue.bind(exchange, routing_key='trafego.*')

                print(' [*] Monitorando tráfego urbano (trafego.*). Para sair pressione CTRL+C')

                for message in queue.consume_messages():
                    try:
                        # Tenta desserializar o corpo em formato JSON
                        data = json.loads(message.body)
                        print(f" [TRÁFEGO LOG] {message.routing_key} -> {data}")

                        # Confirma processamento bem-sucedido
                        message.ack()
                        print(" [✓] Mensagem de tráfego processada e confirmada (ACK).")
                    except json.JSONDecodeError:
                        # Em caso de mensagem com JSON inválido/corrompido, descarta para não bloquear a fila
                        print(f" [ERRO] Mensagem malformada descartada: {message.body}")
                        message.nack(requeue=False)
                        print(" [✗] Mensagem rejeitada sem re-enfileiramento (NACK requeue=False).")

    except KeyboardInterrupt:
        print("\n [!] Consumidor de auditoria finalizado pelo usuário.")
    except Exception as e:
        print(f"\n [ERRO] Ocorreu um erro no consumidor de auditoria: {e}")


if __name__ == '__main__':
    main()
