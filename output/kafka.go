package output

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/click-stream/feeder/common"
	"github.com/devopsext/utils"
	"github.com/prometheus/client_golang/prometheus"
)

var kafkaOutputCount = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "feeder_kafka_output_count",
	Help: "Count of all kafka output count",
}, []string{"feeder_kafka_output_brokers", "feeder_kafka_output_topic"})

type KafkaOutputTopicsV1 struct {
	DefaultPart string
	Messages    string
	Agents      string
	Events      string
	Attributes  string
}

type KafkaOutputOptions struct {
	Brokers            string
	ClientID           string
	FlushFrequency     int
	FlushMaxMessages   int
	NetMaxOpenRequests int
	NetDialTimeout     int
	NetReadTimeout     int
	NetWriteTimeout    int
}

type KafkaOutput struct {
	wg       *sync.WaitGroup
	producer *sarama.SyncProducer
	options  KafkaOutputOptions
	topicsV1 KafkaOutputTopicsV1
}

type KafkaMessageV1 struct {
	Timestamp   uint64   `json:"timestamp,omitempty"`
	Origin      string   `json:"origin,omitempty"`
	IPv4        string   `json:"ipv4,omitempty"`
	Agent       uint32   `json:"agent,omitempty"`
	Session     string   `json:"session,omitempty"`
	Referrer    string   `json:"referrer,omitempty"`
	Country     string   `json:"country,omitempty"`
	Lang        string   `json:"lang,omitempty"`
	Fingerprint string   `json:"fingerprint,omitempty"`
	Provider    string   `json:"provider,omitempty"`
	Property    string   `json:"property,omitempty"`
	Events      []uint32 `json:"events,omitempty"`
	Attributes  []uint32 `json:"attributes,omitempty"`
}

type KafkaAgentV1 struct {
	Timestamp uint64 `json:"timestamp,omitempty"`
	Agent     uint32 `json:"agent,omitempty"`
	Property  string `json:"property,omitempty"`
	Raw       string `json:"raw,omitempty"`
	Json      string `json:"json,omitempty"`
}

type KafkaEventV1 struct {
	Timestamp uint64 `json:"timestamp,omitempty"`
	Event     uint32 `json:"event,omitempty"`
	Property  string `json:"property,omitempty"`
	Json      string `json:"json,omitempty"`
	Total     uint64 `json:"total,omitempty"`
}

type KafkaAttributeV1 struct {
	Timestamp uint64 `json:"timestamp,omitempty"`
	Attribute uint32 `json:"attribute,omitempty"`
	Property  string `json:"property,omitempty"`
	Json      string `json:"json,omitempty"`
	Total     uint64 `json:"total,omitempty"`
}

func byteHash32(b []byte) uint32 {
	h := fnv.New32a()
	h.Write(b)
	return h.Sum32()
}

func byteSha256(b []byte) []byte {
	hasher := sha256.New()
	hasher.Write(b)
	return hasher.Sum(nil)
}

func hasElement(s uint32, array []uint32) bool {

	for i := range array {
		if array[i] == s {
			return true
		}
	}
	return false
}

func (k *KafkaOutput) pushV1(bytes []byte, topic string, variables map[string]string) bool {

	//log.Debug("variables => %s", variables)
	//log.Debug("options => %s", k.options)

	if len(variables) > 0 {

		if feederId, exist := variables["feeder_id"]; exist {
			topic = fmt.Sprintf(topic, feederId)
		} else {
			topic = fmt.Sprintf(topic, k.topicsV1.DefaultPart)
		}
	}

	log.Debug("Message to Kafka (topic: %s) => %s", topic, string(bytes[:]))

	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(bytes)}
	_, _, err := (*k.producer).SendMessage(msg)
	if err != nil {

		log.Error(err)
		return false
	}

	kafkaOutputCount.WithLabelValues(k.options.Brokers, topic).Inc()

	return true
}

func (k *KafkaOutput) sendV1(m *common.Message, variables map[string]string) {

	if m == nil || m.Object == nil {
		log.Debug("Message or its object is not found")
		return
	}

	if reflect.TypeOf(m.Object) != reflect.TypeOf(&common.ObjectV1{}) {

		log.Debug("Message contains none V1 object.")
		return
	}

	v1 := (m.Object).(*common.ObjectV1)

	var agentJson = ""
	agentHash := byteHash32(byteSha256([]byte(v1.AgentString)))

	if v1.AgentObject != nil {

		agentBytes, err := json.Marshal(v1.AgentObject)
		if err == nil {

			agentJson = string(agentBytes[:])
		} else {
			log.Error(err)
		}
	}

	eventCount := make(map[uint32]uint64)
	eventValues := make(map[uint32]string)
	var eventHashes []uint32

	for _, event := range v1.Events {

		if event != nil {

			eventBytes, err := json.Marshal(event)
			if err == nil {

				eventString := string(eventBytes[:])
				eventHash := byteHash32(byteSha256([]byte(eventString)))

				if !hasElement(eventHash, eventHashes) {

					eventValues[eventHash] = eventString
					eventHashes = append(eventHashes, eventHash)
				}

				eventCount[eventHash] = eventCount[eventHash] + 1
			} else {
				log.Error(err)
			}
		}
	}

	attributeCount := make(map[uint32]uint64)
	attributeValues := make(map[uint32]string)
	var attributeHashes []uint32

	for _, attribute := range v1.Attributes {

		if attribute != nil {

			attributeBytes, err := json.Marshal(attribute)
			if err == nil {

				attributeString := string(attributeBytes[:])
				attributeHash := byteHash32(byteSha256([]byte(attributeString)))

				if !hasElement(attributeHash, attributeHashes) {

					attributeValues[attributeHash] = attributeString
					attributeHashes = append(attributeHashes, attributeHash)
				}

				attributeCount[attributeHash] = attributeCount[attributeHash] + 1
			} else {
				log.Error(err)
			}
		}
	}

	if utils.IsEmpty(k.topicsV1.Messages) {
		return
	}

	if !utils.IsEmpty(k.topicsV1.Agents) {

		agent := KafkaAgentV1{
			Timestamp: m.TimeMs,
			Agent:     agentHash,
			Property:  v1.Property,
			Raw:       v1.AgentString,
			Json:      agentJson,
		}

		agentBytes, err := json.Marshal(agent)
		if err == nil {
			k.pushV1(agentBytes, k.topicsV1.Agents, variables)
		} else {
			log.Error(err)
		}
	}

	if !utils.IsEmpty(k.topicsV1.Events) {

		for key, value := range eventValues {

			event := KafkaEventV1{
				Timestamp: m.TimeMs,
				Event:     key,
				Property:  v1.Property,
				Json:      value,
				Total:     eventCount[key],
			}

			eventBytes, err := json.Marshal(event)
			if err == nil {
				k.pushV1(eventBytes, k.topicsV1.Events, variables)
			} else {
				log.Error(err)
			}
		}
	}

	if !utils.IsEmpty(k.topicsV1.Attributes) {

		for key, value := range attributeValues {

			attribute := KafkaAttributeV1{
				Timestamp: m.TimeMs,
				Attribute: key,
				Property:  v1.Property,
				Json:      value,
				Total:     attributeCount[key],
			}

			attributeBytes, err := json.Marshal(attribute)
			if err == nil {
				k.pushV1(attributeBytes, k.topicsV1.Attributes, variables)
			} else {
				log.Error(err)
			}
		}
	}

	message := KafkaMessageV1{
		Timestamp:   m.TimeMs,
		Origin:      v1.Origin,
		IPv4:        v1.IPv4,
		Agent:       agentHash,
		Session:     v1.Session,
		Referrer:    v1.Referrer,
		Country:     v1.Country,
		Lang:        v1.Lang,
		Fingerprint: v1.Fingerprint,
		Provider:    v1.Provider,
		Property:    v1.Property,
		Events:      eventHashes,
		Attributes:  attributeHashes,
	}

	messageBytes, err := json.Marshal(message)
	if err != nil {
		log.Error(err)
		return
	}

	k.pushV1(messageBytes, k.topicsV1.Messages, variables)
}

func (k *KafkaOutput) Send(m *common.Message, variables map[string]string) {

	k.wg.Add(1)
	go func() {
		defer k.wg.Done()

		if k.producer == nil {
			return
		}

		if m == nil {

			log.Debug("Message to Kafka is empty")
			return
		}

		switch m.Version {
		case common.V1:
			k.sendV1(m, variables)
		default:
			log.Debug("Version is not found")
		}

	}()
}

func makeKafkaProducer(wg *sync.WaitGroup, brokers string, config *sarama.Config) *sarama.SyncProducer {

	brks := strings.Split(brokers, ",")
	if len(brks) == 0 || utils.IsEmpty(brokers) {

		log.Debug("Kafka brokers are not defined. Skipped.")
		return nil
	}

	log.Info("Start %s...", config.ClientID)

	producer, err := sarama.NewSyncProducer(brks, config)
	if err != nil {
		log.Error(err)
		return nil
	}

	return &producer
}

func NewKafkaOutput(wg *sync.WaitGroup, options KafkaOutputOptions, topicsV1 KafkaOutputTopicsV1) *KafkaOutput {

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_1_0

	if !utils.IsEmpty(options.ClientID) {
		cfg.ClientID = options.ClientID
	}

	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	cfg.Producer.Flush.Frequency = time.Second * time.Duration(options.FlushFrequency)
	cfg.Producer.Flush.MaxMessages = options.FlushMaxMessages

	cfg.Net.MaxOpenRequests = options.NetMaxOpenRequests
	cfg.Net.DialTimeout = time.Second * time.Duration(options.NetDialTimeout)
	cfg.Net.ReadTimeout = time.Second * time.Duration(options.NetReadTimeout)
	cfg.Net.WriteTimeout = time.Second * time.Duration(options.NetWriteTimeout)

	producer := makeKafkaProducer(wg, options.Brokers, cfg)
	if producer == nil {
		return nil
	}

	return &KafkaOutput{
		wg:       wg,
		producer: producer,
		options:  options,
		topicsV1: topicsV1,
	}
}

func init() {
	prometheus.Register(kafkaOutputCount)
}
