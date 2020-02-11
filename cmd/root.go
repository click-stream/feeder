package cmd

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"

	"github.com/click-stream/feeder/common"
	"github.com/click-stream/feeder/input"
	"github.com/click-stream/feeder/output"
	"github.com/click-stream/feeder/processor"

	utils "github.com/devopsext/utils"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

var VERSION = "unknown"

var log = utils.GetLog()
var env = utils.GetEnvironment()

type envVars struct {
	LOG_FORMAT   string
	LOG_LEVEL    string
	LOG_TEMPLATE string

	PROMETHEUS_URL    string
	PROMETHEUS_LISTEN string

	TEMPLATE_TIME_FORMAT string
	TEMPLATE_LAYOUT      string

	HTTP_V1_URL string
	HTTP_LISTEN string
	HTTP_TLS    bool
	HTTP_CERT   string
	HTTP_KEY    string
	HTTP_CHAIN  string

	HTTP_HEADER_ORIGIN      string
	HTTP_HEADER_IPV4        string
	HTTP_HEADER_SESSION     string
	HTTP_HEADER_USER_AGENT  string
	HTTP_HEADER_REFERRER    string
	HTTP_HEADER_LANG        string
	HTTP_HEADER_FINGERPRINT string
	HTTP_HEADER_COUNTRY     string
	HTTP_HEADER_PROVIDER    string
	HTTP_HEADER_PROPERTY    string

	KAFKA_BROKERS                     string
	KAFKA_CLIENT_ID                   string
	KAFKA_PRODUCER_FLUSH_FREQUENCY    int
	KAFKA_PRODUCER_FLUSH_MAX_MESSAGES int
	KAFKA_NET_MAX_OPEN_REQUESTS       int
	KAFKA_NET_DIAL_TIMEOUT            int
	KAFKA_NET_READ_TIMEOUT            int
	KAFKA_NET_WRITE_TIMEOUT           int

	KAFKA_DEFAULT_V1_TOPIC_PART string
	KAFKA_MESSAGES_V1_TOPIC     string
	KAFKA_AGENTS_V1_TOPIC       string
	KAFKA_EVENTS_V1_TOPIC       string
	KAFKA_ATTRIBUTES_V1_TOPIC   string
}

var vars = envVars{

	LOG_FORMAT:   env.Get("FEEDER_LOG_FORMAT", "text").(string),
	LOG_LEVEL:    env.Get("FEEDER_LOG_LEVEL", "info").(string),
	LOG_TEMPLATE: env.Get("FEEDER_LOG_TEMPLATE", "{{.func}} [{{.line}}]: {{.msg}}").(string),

	PROMETHEUS_URL:    env.Get("FEEDER_PROMETHEUS_URL", "/metrics").(string),
	PROMETHEUS_LISTEN: env.Get("FEEDER_PROMETHEUS_LISTEN", "127.0.0.1:8080").(string),

	TEMPLATE_TIME_FORMAT: env.Get("FEEDER_TEMPLATE_TIME_FORMAT", "2006-01-02T15:04:05.999Z").(string),
	TEMPLATE_LAYOUT:      env.Get("FEEDER_TEMPLATE_LAYOUT", "").(string),

	HTTP_V1_URL: env.Get("FEEDER_HTTP_V1_URL", "/v1").(string),
	HTTP_LISTEN: env.Get("FEEDER_HTTP_LISTEN", ":80").(string),
	HTTP_TLS:    env.Get("FEEDER_HTTP_TLS", false).(bool),
	HTTP_CERT:   env.Get("FEEDER_HTTP_CERT", "").(string),
	HTTP_KEY:    env.Get("FEEDER_HTTP_KEY", "").(string),
	HTTP_CHAIN:  env.Get("FEEDER_HTTP_CHAIN", "").(string),

	HTTP_HEADER_ORIGIN:      env.Get("FEEDER_HTTP_HEADER_ORIGIN", "X-Forwarded-Origin").(string),
	HTTP_HEADER_IPV4:        env.Get("FEEDER_HTTP_HEADER_IPV4", "X-Forwarded-IPv4").(string),
	HTTP_HEADER_SESSION:     env.Get("FEEDER_HTTP_HEADER_SESSION", "X-Forwarded-Session").(string),
	HTTP_HEADER_USER_AGENT:  env.Get("FEEDER_HTTP_HEADER_USER_AGENT", "X-Forwarded-UserAgent").(string),
	HTTP_HEADER_REFERRER:    env.Get("FEEDER_HTTP_HEADER_REFERRER", "X-Forwarded-Referrer").(string),
	HTTP_HEADER_LANG:        env.Get("FEEDER_HTTP_HEADER_LANG", "X-Forwarded-Lang").(string),
	HTTP_HEADER_FINGERPRINT: env.Get("FEEDER_HTTP_HEADER_FINGERPRINT", "X-Forwarded-Fingerprint").(string),
	HTTP_HEADER_COUNTRY:     env.Get("FEEDER_HTTP_HEADER_COUNTRY", "X-Forwarded-Country").(string),
	HTTP_HEADER_PROVIDER:    env.Get("FEEDER_HTTP_HEADER_PROVIDER", "X-Forwarded-Provider").(string),
	HTTP_HEADER_PROPERTY:    env.Get("FEEDER_HTTP_HEADER_PROPERTY", "X-Forwarded-Property").(string),

	KAFKA_BROKERS:                     env.Get("FEEDER_KAFKA_BROKERS", "").(string),
	KAFKA_CLIENT_ID:                   env.Get("FEEDER_KAFKA_CLIENT_ID", "feeder_kafka").(string),
	KAFKA_PRODUCER_FLUSH_FREQUENCY:    env.Get("FEEDER_KAFKA_PRODUCER_FLUSH_FREQUENCY", 1).(int),
	KAFKA_PRODUCER_FLUSH_MAX_MESSAGES: env.Get("FEEDER_KAFKA_PRODUCER_FLUSH_MAX_MESSAGES", 100).(int),
	KAFKA_NET_MAX_OPEN_REQUESTS:       env.Get("FEEDER_KAFKA_NET_MAX_OPEN_REQUESTS", 5).(int),
	KAFKA_NET_DIAL_TIMEOUT:            env.Get("FEEDER_KAFKA_NET_DIAL_TIMEOUT", 30).(int),
	KAFKA_NET_READ_TIMEOUT:            env.Get("FEEDER_KAFKA_NET_READ_TIMEOUT", 30).(int),
	KAFKA_NET_WRITE_TIMEOUT:           env.Get("FEEDER_KAFKA_NET_WRITE_TIMEOUT", 30).(int),

	KAFKA_DEFAULT_V1_TOPIC_PART: env.Get("FEEDER_KAFKA_DEFAULT_V1_TOPIC_PART", "").(string),
	KAFKA_MESSAGES_V1_TOPIC:     env.Get("FEEDER_KAFKA_MESSAGES_V1_TOPIC", "messages.v1").(string),
	KAFKA_AGENTS_V1_TOPIC:       env.Get("FEEDER_KAFKA_AGENTS_V1_TOPIC", "agents.v1").(string),
	KAFKA_EVENTS_V1_TOPIC:       env.Get("FEEDER_KAFKA_EVENTS_V1_TOPIC", "events.v1").(string),
	KAFKA_ATTRIBUTES_V1_TOPIC:   env.Get("FEEDER_KAFKA_ATTRIBUTES_V1_TOPIC", "attributes.v1").(string),
}

func startMetrics(wg *sync.WaitGroup) {

	wg.Add(1)

	go func(wg *sync.WaitGroup) {

		defer wg.Done()

		log.Info("Start metrics...")

		http.Handle(vars.PROMETHEUS_URL, promhttp.Handler())

		listener, err := net.Listen("tcp", vars.PROMETHEUS_LISTEN)
		if err != nil {
			log.Panic(err)
		}

		log.Info("Metrics are up. Listening...")

		err = http.Serve(listener, nil)
		if err != nil {
			log.Panic(err)
		}

	}(wg)
}

func interceptSyscall() {

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)
	go func() {
		<-c
		log.Info("Exiting...")
		os.Exit(1)
	}()
}

func Execute() {

	rootCmd := &cobra.Command{
		Use:   "feeder",
		Short: "Feeder",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {

			log.CallInfo = true
			log.Init(vars.LOG_FORMAT, vars.LOG_LEVEL, vars.LOG_TEMPLATE)

		},
		Run: func(cmd *cobra.Command, args []string) {

			log.Info("Booting...")

			var wg sync.WaitGroup

			startMetrics(&wg)

			//render.SetTextTemplateConfig(vars, vars.TEMPLATE_TIME_FORMAT, vars.TEMPLATE_LAYOUT)

			v1ProcessorConfig := processor.V1ProcessorConfig{
				HeaderOrigin:      vars.HTTP_HEADER_ORIGIN,
				HeaderIPv4:        vars.HTTP_HEADER_IPV4,
				HeaderSession:     vars.HTTP_HEADER_SESSION,
				HeaderUserAgent:   vars.HTTP_HEADER_USER_AGENT,
				HeaderReferrer:    vars.HTTP_HEADER_REFERRER,
				HeaderLang:        vars.HTTP_HEADER_LANG,
				HeaderFingerprint: vars.HTTP_HEADER_FINGERPRINT,
				HeaderCountry:     vars.HTTP_HEADER_COUNTRY,
				HeaderProvider:    vars.HTTP_HEADER_PROVIDER,
				HeaderProperty:    vars.HTTP_HEADER_PROPERTY,
			}

			var httpInput common.Input = input.NewHttpInput(vars.HTTP_V1_URL, vars.HTTP_LISTEN,
				vars.HTTP_TLS, vars.HTTP_CERT, vars.HTTP_KEY, vars.HTTP_CHAIN, &v1ProcessorConfig)

			if reflect.ValueOf(httpInput).IsNil() {
				log.Panic("Http input is invalid. Terminating...")
			}

			inputs := common.NewInputs()
			inputs.Add(&httpInput)

			var kafkaConfig = output.KafkaConfig{
				FlushFrequency:     vars.KAFKA_PRODUCER_FLUSH_FREQUENCY,
				FlushMaxMessages:   vars.KAFKA_PRODUCER_FLUSH_MAX_MESSAGES,
				NetMaxOpenRequests: vars.KAFKA_NET_MAX_OPEN_REQUESTS,
				NetDialTimeout:     vars.KAFKA_NET_DIAL_TIMEOUT,
				NetReadTimeout:     vars.KAFKA_NET_READ_TIMEOUT,
				NetWriteTimeout:    vars.KAFKA_NET_WRITE_TIMEOUT,
				DefaultV1TopicPart: vars.KAFKA_DEFAULT_V1_TOPIC_PART,
				MessagesV1Topic:    vars.KAFKA_MESSAGES_V1_TOPIC,
				AgentsV1Topic:      vars.KAFKA_AGENTS_V1_TOPIC,
				EventsV1Topic:      vars.KAFKA_EVENTS_V1_TOPIC,
				AttributesV1Topic:  vars.KAFKA_ATTRIBUTES_V1_TOPIC,
			}

			var kafkaOutput common.Output = output.NewKafkaOutput(&wg, vars.KAFKA_CLIENT_ID, vars.KAFKA_BROKERS, &kafkaConfig)

			if reflect.ValueOf(kafkaOutput).IsNil() {
				log.Panic("Kafka output is invalid. Terminating...")
			}

			outputs := common.NewOutputs(vars.TEMPLATE_TIME_FORMAT)
			outputs.Add(&kafkaOutput)

			inputs.Start(&wg, outputs)

			wg.Wait()
		},
	}

	flags := rootCmd.PersistentFlags()

	flags.StringVar(&vars.LOG_FORMAT, "log-format", vars.LOG_FORMAT, "Log format: json, text, stdout")
	flags.StringVar(&vars.LOG_LEVEL, "log-level", vars.LOG_LEVEL, "Log level: info, warn, error, debug, panic")
	flags.StringVar(&vars.LOG_TEMPLATE, "log-template", vars.LOG_TEMPLATE, "Log template")

	flags.StringVar(&vars.PROMETHEUS_URL, "prometheus-url", vars.PROMETHEUS_URL, "Prometheus endpoint url")
	flags.StringVar(&vars.PROMETHEUS_LISTEN, "prometheus-listen", vars.PROMETHEUS_LISTEN, "Prometheus listen")

	flags.StringVar(&vars.TEMPLATE_TIME_FORMAT, "template-time-format", vars.TEMPLATE_TIME_FORMAT, "Template time format")
	flags.StringVar(&vars.TEMPLATE_LAYOUT, "template-layout", vars.TEMPLATE_LAYOUT, "Template layout name")

	flags.StringVar(&vars.HTTP_V1_URL, "http-v1-url", vars.HTTP_V1_URL, "Http v1 url")
	flags.StringVar(&vars.HTTP_LISTEN, "http-listen", vars.HTTP_LISTEN, "Http listen")
	flags.BoolVar(&vars.HTTP_TLS, "http-tls", vars.HTTP_TLS, "Http TLS")
	flags.StringVar(&vars.HTTP_CERT, "http-cert", vars.HTTP_CERT, "Http cert file or content")
	flags.StringVar(&vars.HTTP_KEY, "http-key", vars.HTTP_KEY, "Http key file or content")
	flags.StringVar(&vars.HTTP_CHAIN, "http-chain", vars.HTTP_CHAIN, "Http CA chain file or content")

	flags.StringVar(&vars.HTTP_HEADER_ORIGIN, "http-header-origin", vars.HTTP_HEADER_ORIGIN, "Http header origin")
	flags.StringVar(&vars.HTTP_HEADER_IPV4, "http-header-ipv4", vars.HTTP_HEADER_IPV4, "Http header ipv4")
	flags.StringVar(&vars.HTTP_HEADER_SESSION, "http-header-session", vars.HTTP_HEADER_SESSION, "Http header session")
	flags.StringVar(&vars.HTTP_HEADER_USER_AGENT, "http-header-user-agent", vars.HTTP_HEADER_USER_AGENT, "Http header user agent")
	flags.StringVar(&vars.HTTP_HEADER_REFERRER, "http-header-referrer", vars.HTTP_HEADER_REFERRER, "Http header referrer")
	flags.StringVar(&vars.HTTP_HEADER_LANG, "http-header-lang", vars.HTTP_HEADER_LANG, "Http header lang")
	flags.StringVar(&vars.HTTP_HEADER_FINGERPRINT, "http-header-fingerprint", vars.HTTP_HEADER_FINGERPRINT, "Http header fingerprint")
	flags.StringVar(&vars.HTTP_HEADER_COUNTRY, "http-header-country", vars.HTTP_HEADER_COUNTRY, "Http header country")
	flags.StringVar(&vars.HTTP_HEADER_PROVIDER, "http-header-provider", vars.HTTP_HEADER_PROVIDER, "Http header provider")

	flags.StringVar(&vars.KAFKA_BROKERS, "kafka-brokers", vars.KAFKA_BROKERS, "Kafka brokers")
	flags.StringVar(&vars.KAFKA_CLIENT_ID, "kafka-client-id", vars.KAFKA_CLIENT_ID, "Kafka client id")
	flags.IntVar(&vars.KAFKA_PRODUCER_FLUSH_FREQUENCY, "kafka-producer-flush-frequency", vars.KAFKA_PRODUCER_FLUSH_FREQUENCY, "Kafka Producer flush frequency")
	flags.IntVar(&vars.KAFKA_PRODUCER_FLUSH_MAX_MESSAGES, "kafka-producer-flush-max-messages", vars.KAFKA_PRODUCER_FLUSH_MAX_MESSAGES, "Kafka Producer flush max messages")
	flags.IntVar(&vars.KAFKA_NET_MAX_OPEN_REQUESTS, "kafka-net-max-open-requests", vars.KAFKA_NET_MAX_OPEN_REQUESTS, "Kafka Net max open requests")
	flags.IntVar(&vars.KAFKA_NET_DIAL_TIMEOUT, "kafka-net-dial-timeout", vars.KAFKA_NET_DIAL_TIMEOUT, "Kafka Net dial timeout")
	flags.IntVar(&vars.KAFKA_NET_READ_TIMEOUT, "kafka-net-read-timeout", vars.KAFKA_NET_READ_TIMEOUT, "Kafka Net read timeout")
	flags.IntVar(&vars.KAFKA_NET_WRITE_TIMEOUT, "kafka-net-write-timeout", vars.KAFKA_NET_WRITE_TIMEOUT, "Kafka Net write timeout")

	flags.StringVar(&vars.KAFKA_DEFAULT_V1_TOPIC_PART, "kafka-default-v1-topic-part", vars.KAFKA_DEFAULT_V1_TOPIC_PART, "Kafka messages default v1 topic part")
	flags.StringVar(&vars.KAFKA_MESSAGES_V1_TOPIC, "kafka-messages-v1-topic", vars.KAFKA_MESSAGES_V1_TOPIC, "Kafka messages v1 topic")
	flags.StringVar(&vars.KAFKA_AGENTS_V1_TOPIC, "kafka-agents-v1-topic", vars.KAFKA_AGENTS_V1_TOPIC, "Kafka agents v1 topic")
	flags.StringVar(&vars.KAFKA_EVENTS_V1_TOPIC, "kafka-events-v1-topic", vars.KAFKA_EVENTS_V1_TOPIC, "Kafka events v1 topic")
	flags.StringVar(&vars.KAFKA_ATTRIBUTES_V1_TOPIC, "kafka-attributes-v1-topic", vars.KAFKA_ATTRIBUTES_V1_TOPIC, "Kafka attributes v1 topic")

	interceptSyscall()

	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(VERSION)
		},
	})

	if err := rootCmd.Execute(); err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
