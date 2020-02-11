package output

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/click-stream/feeder/common"
	"github.com/prometheus/client_golang/prometheus"
)

var kafkaOutputCount = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "feeder_kafka_output_count",
	Help: "Count of all kafka output count",
}, []string{"feeder_kafka_output_brokers", "feeder_kafka_output_topic"})

type KafkaConfig struct {
	FlushFrequency     int
	FlushMaxMessages   int
	NetMaxOpenRequests int
	NetDialTimeout     int
	NetReadTimeout     int
	NetWriteTimeout    int
	DefaultV1TopicPart string
	MessagesV1Topic    string
	AgentsV1Topic      string
	EventsV1Topic      string
	AttributesV1Topic  string
}

type KafkaOutput struct {
	wg       *sync.WaitGroup
	brokers  string
	producer *sarama.SyncProducer
	config   *KafkaConfig
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

func hasElement(s uint32, array []uint32) bool {

	for i := range array {
		if array[i] == s {
			return true
		}
	}
	return false
}

func (k *KafkaOutput) push(bytes []byte, topic string, variables map[string]string) bool {

	log.Debug("variables => %s", variables)
	log.Debug("config => %s", k.config)

	if idValue, exist := variables["id"]; exist {
		topic = fmt.Sprintf(topic, idValue)
	} else {
		topic = fmt.Sprintf(topic, k.config.DefaultV1TopicPart)
	}

	log.Debug("Message to Kafka (topic: %s) => %s", topic, string(bytes[:]))

	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(bytes)}
	_, _, err := (*k.producer).SendMessage(msg)
	if err != nil {

		log.Error(err)
		return false
	}

	kafkaOutputCount.WithLabelValues(k.brokers, topic).Inc()

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
	agentHash := byteHash32([]byte(v1.AgentString))

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
				eventHash := byteHash32([]byte(eventString))

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
				attributeHash := byteHash32([]byte(attributeString))

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

	if k.config != nil && !common.IsEmpty(k.config.MessagesV1Topic) {

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

		if !k.push(messageBytes, k.config.MessagesV1Topic, variables) {
			return
		}

		if !common.IsEmpty(k.config.AgentsV1Topic) {

			agent := KafkaAgentV1{
				Timestamp: m.TimeMs,
				Agent:     agentHash,
				Property:  v1.Property,
				Raw:       v1.AgentString,
				Json:      agentJson,
			}

			agentBytes, err := json.Marshal(agent)
			if err == nil {
				k.push(agentBytes, k.config.AgentsV1Topic, variables)
			} else {
				log.Error(err)
			}
		}

		if !common.IsEmpty(k.config.EventsV1Topic) {

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
					k.push(eventBytes, k.config.EventsV1Topic, variables)
				} else {
					log.Error(err)
				}
			}
		}

		if !common.IsEmpty(k.config.AttributesV1Topic) {

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
					k.push(attributeBytes, k.config.AttributesV1Topic, variables)
				} else {
					log.Error(err)
				}
			}
		}

	}
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
	if len(brks) == 0 || common.IsEmpty(brokers) {

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

func NewKafkaOutput(wg *sync.WaitGroup, clientID string, brokers string, config *KafkaConfig) *KafkaOutput {

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_1_1_0

	if !common.IsEmpty(clientID) {
		cfg.ClientID = clientID
	}

	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	if config != nil {

		cfg.Producer.Flush.Frequency = time.Second * time.Duration(config.FlushFrequency)
		cfg.Producer.Flush.MaxMessages = config.FlushMaxMessages

		cfg.Net.MaxOpenRequests = config.NetMaxOpenRequests
		cfg.Net.DialTimeout = time.Second * time.Duration(config.NetDialTimeout)
		cfg.Net.ReadTimeout = time.Second * time.Duration(config.NetReadTimeout)
		cfg.Net.WriteTimeout = time.Second * time.Duration(config.NetWriteTimeout)
	}

	producer := makeKafkaProducer(wg, brokers, cfg)
	if producer == nil {
		return nil
	}

	return &KafkaOutput{
		wg:       wg,
		brokers:  brokers,
		producer: producer,
		config:   config,
	}
}

func init() {
	prometheus.Register(kafkaOutputCount)
}
