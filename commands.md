# Primeiro devemos gerar um ID para o cluster

```
KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
```

# Em seguida, devemos formatar o armazenamento
```
   bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/server.properties --standalone
```

# Agora, estamos prontos para iniciar o kafka
```
bin/kafka-server-start.sh config/server.properties
```

# Criar um tópico
```
bin/kafka-topics.sh --create --bootstrap-server <server_address> --replication-factor <num_replicas> --partitions <num_partitions> --topic <topic_name>
```

# Listar os tópicos criados
```
bin/kafka-topics.sh --list --bootstrap-server <server_address>
```

# Criar um produtor de mensagens no console
```
bin/kafka-console-producer.sh --bootstrap-server <server_address> --topic <topic_name>
```
obs: ao rodar esse comando, cada linha que digitarmos no console será uma mensagem enviada para o tópico.

# Criar um consumidor de mensagens no console
```
bin/kafka-console-consumer.sh --bootstrap-server <server_address> --topic <topic_name> --from-beginning
```
obs: se não passarmos o parâmetro `--from-beginning`, o consumidor irá ignorar as mensagens que foram 
enviadas antes da sua criação. Passando essa propriedade, lemos todas as mensagens, desde o início e não apenas as novas.

# Alterar o número de partições de um tópico já existente
```
bin/kafka-topics.sh --alter --bootstrap-server <server_address> --topic <topic_name> --partitions <num_partitions>
```
Se quisermos alterar a configuração padrão de número de partições de uma instância do Kafka, devemos editar o arquivo que está no diretório `config/server.properties`.

```
# ...
num.partitions=1
# ...
```

# Descrever todos os consumer groups
```
bin/kafka-consumer-groups.sh --bootstrap-server <server_address> --describe --all-groups
```

# Descrever consumidores de um grupo específico
```
bin/kafka-consumer-groups.sh --bootstrap-server <server_address> --describe --group <group_name>
```
