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

type rootOptions struct {
	LogFormat   string
	LogLevel    string
	LogTemplate string

	PrometheusURL    string
	PrometheusListen string
}

var rootOpts = rootOptions{

	LogFormat:   env.Get("FEEDER_LOG_FORMAT", "text").(string),
	LogLevel:    env.Get("FEEDER_LOG_LEVEL", "info").(string),
	LogTemplate: env.Get("FEEDER_LOG_TEMPLATE", "{{.func}} [{{.line}}]: {{.msg}}").(string),

	PrometheusURL:    env.Get("FEEDER_PROMETHEUS_URL", "/metrics").(string),
	PrometheusListen: env.Get("FEEDER_PROMETHEUS_LISTEN", "127.0.0.1:8080").(string),
}

var httpInputOptions = input.HttpInputOptions{

	Listen: env.Get("FEEDER_HTTP_LISTEN", ":80").(string),
	Tls:    env.Get("FEEDER_HTTP_TLS", false).(bool),
	Cert:   env.Get("FEEDER_HTTP_CERT", "").(string),
	Key:    env.Get("FEEDER_HTTP_KEY", "").(string),
	Chain:  env.Get("FEEDER_HTTP_CHAIN", "").(string),
	URLv1:  env.Get("FEEDER_HTTP_URL_V1", "/v1").(string),
	Cors:   env.Get("FEEDER_HTTP_CORS_ENABLE", false).(bool),
}

var processorOptions = processor.ProcessorOptions{
	HeaderOrigin:      env.Get("FEEDER_HTTP_HEADER_ORIGIN", "X-Forwarded-Origin").(string),
	HeaderIPv4:        env.Get("FEEDER_HTTP_HEADER_IPV4", "X-Forwarded-IPv4").(string),
	HeaderSession:     env.Get("FEEDER_HTTP_HEADER_SESSION", "X-Forwarded-Session").(string),
	HeaderUserAgent:   env.Get("FEEDER_HTTP_HEADER_USER_AGENT", "X-Forwarded-UserAgent").(string),
	HeaderReferrer:    env.Get("FEEDER_HTTP_HEADER_REFERRER", "X-Forwarded-Referrer").(string),
	HeaderLang:        env.Get("FEEDER_HTTP_HEADER_LANG", "X-Forwarded-Lang").(string),
	HeaderFingerprint: env.Get("FEEDER_HTTP_HEADER_FINGERPRINT", "X-Forwarded-Fingerprint").(string),
	HeaderCountry:     env.Get("FEEDER_HTTP_HEADER_COUNTRY", "X-Forwarded-Country").(string),
	HeaderProvider:    env.Get("FEEDER_HTTP_HEADER_PROVIDER", "X-Forwarded-Provider").(string),
	HeaderProperty:    env.Get("FEEDER_HTTP_HEADER_PROPERTY", "X-Forwarded-Property").(string),
}

var kafkaOutputOptions = output.KafkaOutputOptions{

	Brokers:            env.Get("FEEDER_KAFKA_BROKERS", "").(string),
	ClientID:           env.Get("FEEDER_KAFKA_CLIENT_ID", "feeder_kafka").(string),
	FlushFrequency:     env.Get("FEEDER_KAFKA_FLUSH_FREQUENCY", 1).(int),
	FlushMaxMessages:   env.Get("FEEDER_KAFKA_FLUSH_MAX_MESSAGES", 100).(int),
	NetMaxOpenRequests: env.Get("FEEDER_KAFKA_NET_MAX_OPEN_REQUESTS", 5).(int),
	NetDialTimeout:     env.Get("FEEDER_KAFKA_NET_DIAL_TIMEOUT", 30).(int),
	NetReadTimeout:     env.Get("FEEDER_KAFKA_NET_READ_TIMEOUT", 30).(int),
	NetWriteTimeout:    env.Get("FEEDER_KAFKA_NET_WRITE_TIMEOUT", 30).(int),
}

var kafkaOutputTopicsV1 = output.KafkaOutputTopicsV1{
	DefaultPart: env.Get("FEEDER_KAFKA_TOPIC_DEFAULT_PART_V1", "").(string),
	Messages:    env.Get("FEEDER_KAFKA_TOPIC_MESSAGES_V1", "messages.v1").(string),
	Agents:      env.Get("FEEDER_KAFKA_TOPIC_AGENTS_V1", "agents.v1").(string),
	Events:      env.Get("FEEDER_KAFKA_TOPIC_EVENTS_V1", "events.v1").(string),
	Attributes:  env.Get("FEEDER_KAFKA_TOPIC_ATTRIBUTES_V1", "attributes.v1").(string),
}

func startMetrics(wg *sync.WaitGroup) {

	wg.Add(1)

	go func(wg *sync.WaitGroup) {

		defer wg.Done()

		log.Info("Start metrics...")

		http.Handle(rootOpts.PrometheusURL, promhttp.Handler())

		listener, err := net.Listen("tcp", rootOpts.PrometheusListen)
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
			log.Init(rootOpts.LogFormat, rootOpts.LogLevel, rootOpts.LogTemplate)

		},
		Run: func(cmd *cobra.Command, args []string) {

			log.Info("Booting...")

			var wg sync.WaitGroup

			startMetrics(&wg)

			var httpInput common.Input = input.NewHttpInput(httpInputOptions, processorOptions)

			if reflect.ValueOf(httpInput).IsNil() {
				log.Panic("Http input is invalid. Terminating...")
			}

			inputs := common.NewInputs()
			inputs.Add(&httpInput)

			var kafkaOutput common.Output = output.NewKafkaOutput(&wg, kafkaOutputOptions, kafkaOutputTopicsV1)

			if reflect.ValueOf(kafkaOutput).IsNil() {
				log.Panic("Kafka output is invalid. Terminating...")
			}

			outputs := common.NewOutputs()
			outputs.Add(&kafkaOutput)

			inputs.Start(&wg, outputs)

			wg.Wait()
		},
	}

	flags := rootCmd.PersistentFlags()

	flags.StringVar(&rootOpts.LogFormat, "log-format", rootOpts.LogFormat, "Log format: json, text, stdout")
	flags.StringVar(&rootOpts.LogLevel, "log-level", rootOpts.LogLevel, "Log level: info, warn, error, debug, panic")
	flags.StringVar(&rootOpts.LogTemplate, "log-template", rootOpts.LogTemplate, "Log template")

	flags.StringVar(&rootOpts.PrometheusURL, "prometheus-url", rootOpts.PrometheusURL, "Prometheus endpoint url")
	flags.StringVar(&rootOpts.PrometheusListen, "prometheus-listen", rootOpts.PrometheusListen, "Prometheus listen")

	flags.StringVar(&httpInputOptions.Listen, "http-listen", httpInputOptions.Listen, "Http listen")
	flags.BoolVar(&httpInputOptions.Tls, "http-tls", httpInputOptions.Tls, "Http TLS")
	flags.StringVar(&httpInputOptions.Cert, "http-cert", httpInputOptions.Cert, "Http cert file or content")
	flags.StringVar(&httpInputOptions.Key, "http-key", httpInputOptions.Key, "Http key file or content")
	flags.StringVar(&httpInputOptions.Chain, "http-chain", httpInputOptions.Chain, "Http CA chain file or content")
	flags.StringVar(&httpInputOptions.URLv1, "http-url-v1", httpInputOptions.URLv1, "Http url")
	flags.BoolVar(&httpInputOptions.Cors, "http-cors-enable", httpInputOptions.Cors, "Http CORS true/false")

	flags.StringVar(&processorOptions.HeaderOrigin, "http-header-origin", processorOptions.HeaderOrigin, "Http header origin")
	flags.StringVar(&processorOptions.HeaderIPv4, "http-header-ipv4", processorOptions.HeaderIPv4, "Http header ipv4")
	flags.StringVar(&processorOptions.HeaderSession, "http-header-session", processorOptions.HeaderSession, "Http header session")
	flags.StringVar(&processorOptions.HeaderUserAgent, "http-header-user-agent", processorOptions.HeaderUserAgent, "Http header user agent")
	flags.StringVar(&processorOptions.HeaderReferrer, "http-header-referrer", processorOptions.HeaderReferrer, "Http header referrer")
	flags.StringVar(&processorOptions.HeaderLang, "http-header-lang", processorOptions.HeaderLang, "Http header lang")
	flags.StringVar(&processorOptions.HeaderFingerprint, "http-header-fingerprint", processorOptions.HeaderFingerprint, "Http header fingerprint")
	flags.StringVar(&processorOptions.HeaderCountry, "http-header-country", processorOptions.HeaderCountry, "Http header country")
	flags.StringVar(&processorOptions.HeaderProvider, "http-header-provider", processorOptions.HeaderProvider, "Http header provider")

	flags.StringVar(&kafkaOutputOptions.Brokers, "kafka-brokers", kafkaOutputOptions.Brokers, "Kafka brokers")
	flags.StringVar(&kafkaOutputOptions.ClientID, "kafka-client-id", kafkaOutputOptions.ClientID, "Kafka client id")
	flags.IntVar(&kafkaOutputOptions.FlushFrequency, "kafka-flush-frequency", kafkaOutputOptions.FlushFrequency, "Kafka Producer flush frequency")
	flags.IntVar(&kafkaOutputOptions.FlushMaxMessages, "kafka-flush-max-messages", kafkaOutputOptions.FlushMaxMessages, "Kafka Producer flush max messages")
	flags.IntVar(&kafkaOutputOptions.NetMaxOpenRequests, "kafka-net-max-open-requests", kafkaOutputOptions.NetMaxOpenRequests, "Kafka Net max open requests")
	flags.IntVar(&kafkaOutputOptions.NetDialTimeout, "kafka-net-dial-timeout", kafkaOutputOptions.NetDialTimeout, "Kafka Net dial timeout")
	flags.IntVar(&kafkaOutputOptions.NetReadTimeout, "kafka-net-read-timeout", kafkaOutputOptions.NetReadTimeout, "Kafka Net read timeout")
	flags.IntVar(&kafkaOutputOptions.NetWriteTimeout, "kafka-net-write-timeout", kafkaOutputOptions.NetWriteTimeout, "Kafka Net write timeout")

	flags.StringVar(&kafkaOutputTopicsV1.DefaultPart, "kafka-topic-default-part-v1", kafkaOutputTopicsV1.DefaultPart, "Kafka messages default v1 topic part")
	flags.StringVar(&kafkaOutputTopicsV1.Messages, "kafka-topic-messages-v1", kafkaOutputTopicsV1.Messages, "Kafka messages v1 topic")
	flags.StringVar(&kafkaOutputTopicsV1.Agents, "kafka-topic-agents-v1", kafkaOutputTopicsV1.Agents, "Kafka agents v1 topic")
	flags.StringVar(&kafkaOutputTopicsV1.Events, "kafka-topic-events-v1", kafkaOutputTopicsV1.Events, "Kafka events v1 topic")
	flags.StringVar(&kafkaOutputTopicsV1.Attributes, "kafka-topic-attributes-v1", kafkaOutputTopicsV1.Attributes, "Kafka attributes v1 topic")

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
