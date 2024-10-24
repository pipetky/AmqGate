package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/akamensky/argparse"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

type Conf struct {
	QName      string `yaml:"qname"`
	Server     string `yaml:"server"`
	Port       string `yaml:"port"`
	Login      string `yaml:"login"`
	Password   string `yaml:"pass"`
	Vhost      string `yaml:"vhost"`
	Cpq        int    `yaml:"cpq"`
	Durable    bool   `yaml:"durable"`
	AutoDelete bool   `yaml:"auto_delete"`
	Exclusive  bool   `yaml:"exclusive"`
	NoWait     bool   `yaml:"no_wait"`
	Exchange   string `yaml:"exchange"`
	Mandatory  bool   `yaml:"mandatory"`
	Immediate  bool   `yaml:"immediate"`
}

type AmqGate struct {
	Channels   []*amqp.Channel
	Ctx        context.Context
	Cancel     context.CancelFunc
	Queues     []amqp.Queue
	Server     string
	Port       string
	Login      string
	Password   string
	Socket     net.Listener
	Cpq        int
	QName      string
	Conns      []*amqp.Connection
	Wg         sync.WaitGroup
	Vhost      string
	Durable    bool
	NoWait     bool
	Immediate  bool
	Mandatory  bool
	Exclusive  bool
	AutoDelete bool
	Exchange   string
}

func NewAmqGate(config Conf) *AmqGate {
	ctx, cancel := context.WithCancel(context.Background())

	return &AmqGate{
		Cpq:        config.Cpq,
		Port:       config.Port,
		Login:      config.Login,
		Password:   config.Password,
		Server:     config.Server,
		QName:      config.QName,
		Ctx:        ctx,
		Cancel:     cancel,
		Vhost:      config.Vhost,
		Durable:    config.Durable,
		NoWait:     config.NoWait,
		AutoDelete: config.AutoDelete,
		Exclusive:  config.Exclusive,
		Immediate:  config.Immediate,
		Mandatory:  config.Mandatory,
		Exchange:   config.Exchange,
	}

}

func (r *AmqGate) connect() {
	r.Conns = r.Conns[:0]
	r.Channels = r.Channels[:0]
	r.Queues = r.Queues[:0]
	t := 1 * time.Second

	if err := os.MkdirAll("/tmp/amqgate", os.ModePerm); err != nil {
		log.Fatal(err)
	}
	log.Infoln("Trying to connect " + r.Server + ":" + r.Port)
	for i := 0; i < r.Cpq; i++ {
		for {
			conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/%s", r.Login, r.Password, r.Server, r.Port, r.Vhost))
			if err != nil {

				log.WithFields(logrus.Fields{
					"event": "connect_amq",
					"error": err,
				}).Error("Error connect to " + r.Server + ":" + r.Port + " , retrying after " + t.String())
				time.Sleep(t)
				t *= 2
				continue
			}
			r.Conns = append(r.Conns, conn)
			break
		}
		t = 1

		for {
			ch, err := r.Conns[i].Channel()
			if err != nil {

				log.WithFields(logrus.Fields{
					"event": "channel",
					"error": err,
				}).Error("Error open channel, retrying after " + t.String())

				time.Sleep(t)
				t *= 2
				continue
			}
			r.Channels = append(r.Channels, ch)
			break
		}
		t = 1
		for {
			q, err := r.Channels[i].QueueDeclare(
				r.QName,      // name
				r.Durable,    // durable
				r.AutoDelete, // delete when unused
				r.Exclusive,  // exclusive
				r.NoWait,     // no-wait
				nil,          // arguments
			)
			if err != nil {

				log.WithFields(logrus.Fields{
					"event": "queue_rabbitmq",
					"error": err,
				}).Error("Error declare queue! retrying after " + t.String())

				time.Sleep(t * time.Second)
				t *= 2
				continue

			}
			r.Queues = append(r.Queues, q)
			break
		}
	}
	log.Infoln("Done!")

}

func (r *AmqGate) openSocket() {
	log.Infoln("Trying to create socket /tmp/amqgate/" + fmt.Sprintf("%s.sock", r.QName))
	err := os.Remove(fmt.Sprintf("/tmp/amqgate/%s.sock", r.QName))
	if err != nil {
		if !errors.Is(err, unix.ENOENT) {
			log.WithFields(logrus.Fields{
				"event": "unix_socket",
				"error": err,
			}).Fatal("Error remove old unix socket!")
		}
	}

	for i := 0; i < 5; i++ {

		socket, err := net.Listen("unix", fmt.Sprintf("/tmp/amqgate/%s.sock", r.QName))
		if err != nil {
			if i == 4 {
				log.WithFields(logrus.Fields{
					"event": "unix_socket",
					"error": err,
				}).Fatal("Error create unix socket!")
			}
			log.WithFields(logrus.Fields{
				"attempt": i + 1,
			}).Infoln("Trying to open a socket")

			time.Sleep(2 * time.Second)
			continue
		}
		r.Socket = socket
		log.Infoln("Done!")
		return
	}

}

func (r *AmqGate) start(wg *sync.WaitGroup) {
	defer wg.Done()
	r.connect()
	r.openSocket()
	defer r.Socket.Close()
	for i := 0; i < r.Cpq; i++ {
		defer r.Conns[i].Close()
		defer r.Channels[i].Close()
		defer r.Socket.Close()
	}
	buf := make([]byte, 65536)

	for {
		select {
		case <-r.Ctx.Done():
			return
		default:
			for i := 0; i < r.Cpq; i++ {
				r.Wg.Add(1)
				go func(ctx context.Context) {
					defer r.Wg.Done()
					conn, err := r.Socket.Accept()

					if err != nil {
						log.WithFields(logrus.Fields{
							"event": "unix_socket",
							"error": err,
						}).Error("Error reading from socket!")
						os.Remove(fmt.Sprintf("/tmp/amqgate/%s.sock", r.QName))
						r.Cancel()
						return
					}
					defer conn.Close()

					_, err = conn.Read(buf)
					if err != nil {
						os.Remove(fmt.Sprintf("/tmp/amqgate/%s.sock", r.QName))
						log.WithFields(logrus.Fields{
							"event": "unix_socket",
							"error": err,
						}).Error("Error reading from socket!")
						r.Cancel()
						return
					}
					message := string(buf)
					ctx, cancel := context.WithTimeout(r.Ctx, 5*time.Second)
					defer cancel()

					for j := 0; j < 5; j++ {

						err = r.Channels[i%r.Cpq].PublishWithContext(ctx,
							r.Exchange,             // exchange
							r.Queues[i%r.Cpq].Name, // routing key
							r.Mandatory,            // mandatory
							r.Immediate,            // immediate
							amqp.Publishing{
								ContentType: "text/plain",
								Body:        []byte(message),
							})
						if err != nil {
							if j == 4 {
								r.Cancel()
								return
							}
							log.WithFields(logrus.Fields{
								"event": "publish_queue",
								"error": err,
								"queue": r.Queues[i%r.Cpq].Name,
							}).Error("Error publishing to queue!")
							time.Sleep(1 * time.Second)
							r.connect()
							log.WithFields(logrus.Fields{
								"attempt": j + 1,
							}).Infoln("Trying to publish in queue!")
							continue
						}
						break
					}
				}(r.Ctx)
			}
			r.Wg.Wait()
		}
	}
}

func (r *AmqGate) stop() {
	r.Cancel()
	for i := 0; i < r.Cpq; i++ {
		r.Channels[i].Close()
		r.Conns[i].Close()
	}
	r.Socket.Close()
}

func main() {
	parser := argparse.NewParser("AmqGate", "A unix socket creator through tcp for amqp")
	configFilePath := parser.String("c", "config", &argparse.Options{Required: false, Help: "yaml config path"})
	err := parser.Parse(os.Args)
	if err != nil {
		// In case of error print error and print usage
		// This can also be done by passing -h or --help flags
		fmt.Print(parser.Usage(err))
	}
	if *configFilePath == "" {
		*configFilePath = "config.yaml"
	}
	log.SetFormatter(&log.JSONFormatter{})
	var conf []Conf

	ConfigFile, err := os.ReadFile(*configFilePath)
	if err != nil {
		log.WithFields(logrus.Fields{
			"event": "config_file",
			"error": err,
			"file":  *configFilePath,
		}).Fatal("Error reading config file!")
	}
	err = yaml.Unmarshal(ConfigFile, &conf)
	if err != nil {
		log.WithFields(logrus.Fields{
			"event": "config_file",
			"error": err,
			"file":  *configFilePath,
		}).Fatal("Error parsing config file!")
	}

	syscall.Umask(0000)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	var gates []*AmqGate
	for _, c := range conf {
		gates = append(gates, NewAmqGate(c))
	}
	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		for _, g := range gates {
			g.stop()
		}
	}()

	wg := sync.WaitGroup{}
	for _, g := range gates {
		wg.Add(1)
		go g.start(&wg)
	}
	wg.Wait()
	os.Exit(0)
}
