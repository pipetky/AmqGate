# AMQGate
Allows PHP clients to quickly publish over a unix domain socket and keep connections established by AMQGate.

This app establishes connection with AMQ server like RabbitMQ and creates unix domain socket per each queue.
Next, it reads messages from socket and publishes it.

When AMQGate connect to AMQ server it will create unix domain socket in `/tmp/amqgate/` directory named \<qname\>.socket
According the example there will be two sockets - example_q.sock and example_q2.sock
## Installation
### Docker
[https://hub.docker.com/r/pipetky/amqgate](https://hub.docker.com/r/pipetky/amqgate)
#### Running
```
docker run -it --rm -v /tmp/amqgate/:/tmp/amqgate/ -v $(pwd)/config.yaml:/app/config.yaml pipetky/amqgate:0.0.1
``` 

### From source
[Install Go](https://go.dev/doc/install)

```
git clone https://github.com/pipetky/AmqGate.git && \
cd AmqGate && \
CGO_ENABLED=0 GOOS=linux go build -o ./amqgate && \
sudo mv amqgate /usr/local/bin/
```
#### Running
```
amqgate -c config.yaml
```
### Configuration
Example config.yaml file:
```yaml
- qname: "example_q"
  server: "192.168.10.3"
  port: "5672"
  cpq: 4 # Connections per queue
  login: "test"
  pass: "test"
  vhost: "test" # Optional, default ""
  durable: True # Optional, default False
  no_wait: True # Optional, default False
  auto_delete: True # Optional, default False
  exclusive: True # Optional, default False
  exchange: "test" # Optional, default ""
- qname: "example_q2"
  server: "192.168.10.3"
  port: "5672"
  cpq: 2 # Connections per queue
  login: "test"
  pass: "test"
```
### Example php code
```php
<?php

$myfile = fopen("test_50k.txt", "r") or die("Unable to open file!");
$file = fread($myfile,filesize("test_50k.txt"));
fclose($myfile);
$socket_file = "/tmp/amqgate/example_q.sock";

for ($i = 0; $i <= 100000; $i++) {

$socket = socket_create(AF_UNIX, SOCK_STREAM, 0);
socket_connect($socket, $socket_file);
socket_write($socket, $file, $msg_len = strlen($file));
socket_close($socket);
}
?>
```
---
Aleksandr Karabchevskiy - pipetky@gmail.com

Project Link: https://github.com/pipetky/AmqGate