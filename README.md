# Trabalho Prático: Conceitos e Funcionamento do RabbitMQ

Este repositório contém a resolução prática e teórica da atividade de **Mensageria com RabbitMQ**, utilizando o protocolo **AMQP** e a biblioteca Python **`rabbitpy`**.

---

## 📁 Estrutura do Projeto

```text
TrabalhoCaproni_RabbitMQ/
│
├── docker-compose.yml       # Orquestração do contêiner RabbitMQ + Management UI
├── requirements.txt         # Dependências do projeto (rabbitpy)
├── publisher.py             # Exercício 1: Publicador com Topic Exchange
├── consumer_critico.py      # Exercício 2: Consumidor Crítico (*.CRITICAL) com Manual ACK
├── consumer_auditoria.py    # Exercício 3: Consumidor de Tráfego (trafego.*) com NACK
├── RESPOSTAS.md             # Respostas completas para as Questões Teórico-Práticas (Blocos A e B)
└── README.md                # Este guia de execução e testes
```

---

## 🚀 Como Executar o Projeto

### 1. Iniciar o Servidor RabbitMQ

Certifique-se de que o **Docker Desktop** esteja em execução e execute na raiz do projeto:

```bash
docker compose up -d
```

> **Alternativa (sem Docker Compose):**
> ```bash
> docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
> ```

*   **Porta AMQP:** `5672` (onde os scripts Python se conectam).
*   **Painel Web (Management):** [http://localhost:15672](http://localhost:15672)
    *   **Usuário padrão:** `guest`
    *   **Senha padrão:** `guest`

---

### 2. Instalar Dependências Python

Instale o pacote `rabbitpy` usando o arquivo `requirements.txt`:

```bash
pip install -r requirements.txt
```

---

### 3. Execução dos Scripts e Cenários de Teste

Para observar o comportamento distribuído e o desacoplamento, abra **três terminais** separados.

#### 📌 Terminal 1: Consumidor Crítico (Exercício 2)
Monitora alertas críticos com padrão `*.CRITICAL` e realiza confirmação manual (`message.ack()`):
```bash
python consumer_critico.py
```

#### 📌 Terminal 2: Consumidor de Auditoria de Tráfego (Exercício 3)
Monitora logs de tráfego urbanístico (`trafego.*`) e descarta mensagens com formato corrompido (`message.nack(requeue=False)`):
```bash
python consumer_auditoria.py
```

#### 📌 Terminal 3: Publicador de Mensagens (Exercício 1)
O publicador envia dados para a Exchange `logs_cidade`. Você pode testar os diferentes cenários:

1. **Execução Padrão (Exercício 1 do PDF):**
   Envia alerta crítico de energia (`energia.CRITICAL`):
   ```bash
   python publisher.py
   ```
   *Resultado esperado:* Capturado apenas pelo **Consumidor Crítico** (Terminal 1).

2. **Log de Tráfego Regular:**
   Envia telemetria de radar de trânsito (`trafego.velocidade`):
   ```bash
   python publisher.py trafego
   ```
   *Resultado esperado:* Capturado apenas pelo **Consumidor de Auditoria** (Terminal 2).

3. **Alerta Crítico de Tráfego (Multicast / Topic):**
   Envia alerta crítico em túnel (`trafego.CRITICAL`):
   ```bash
   python publisher.py trafego_critico
   ```
   *Resultado esperado:* Capturado por **ambos os consumidores** simultaneamente, demonstrando o poder de roteamento da Topic Exchange do RabbitMQ.

4. **Teste de Erro e NACK (Payload Malformado):**
   Envia uma mensagem com JSON sintaticamente inválido:
   ```bash
   python publisher.py invalido
   ```
   *Resultado esperado:* O Consumidor de Auditoria captura o erro no bloco `try/except json.JSONDecodeError`, emite o NACK com `requeue=False` e a mensagem corrompida é descartada sem travar o consumidor.

---

## 📖 Resolução das Questões Teóricas

O arquivo [`RESPOSTAS.md`](RESPOSTAS.md) contém a resolução aprofundada de todas as perguntas dos seguintes blocos:

*   **Bloco A: Comparativo de Modelos de Comunicação (Sockets vs. RPC vs. AMQP)**
    *   *Questão 1:* Acoplamento Temporal e Espacial.
    *   *Questão 2:* Complexidade e Abstração do Protocolo.
    *   *Questão 3:* Garantias de Entrega e Tolerância a Falhas (`durable=True` + `message.ack()`).
*   **Bloco B: Aspectos Específicos do AMQP e rabbitpy**
    *   *Questão 4:* Abstração Orientada a Objetos no `rabbitpy`.
    *   *Questão 5:* Tratamento de Mensagens Inválidas (Diferença entre `message.nack(requeue=True)` e `message.nack(requeue=False)`).
*   **Síntese de Arquitetura:** Por que o Publicador nunca envia diretamente para a Fila.
