# Feeder

Http service which consumes Clickstream data and pushs it to Kafka in a proper Clickhouse format.

[![GoDoc](https://godoc.org/github.com/click-stream/feeder?status.svg)](https://godoc.org/github.com/click-stream/feeder)
[![build status](https://img.shields.io/travis/click-stream/feeder/master.svg?style=flat-square)](https://travis-ci.org/click-stream/feeder)

## Features

- Support json requests with mixed events and attributes
- Determine device type, browser name, browser version, OS platform, OS name and version
- Provide Prometheus endpoint, input & output metrics
- Use hashes to pack events, atrributes and agents

## Build

```sh
git clone https://github.com/click-stream/feeder.git
cd feeder/
go build
```

## Example

### Run Feeder

```sh
./feeder --http-listen ":1081" --kafka-brokers "kafka:9092" \
         --log-template "{{.msg}}" --log-format stdout --log-level debug 
```

### Prepare v1.json

```json
{
  "origin": "https://www.somewebsite.com",
  "agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 11_4_1 like Mac OS X)",
  "session": "sdfdfwdf33451345345",
  "cookie": "PHPSESSID",
  "referrer": "https://google.com/?text=some",
  "lang": "en",
  "fingerprint": "2",
  "country": "US",
  "property": "other",
  "events": [
    {"object":"player","event":"resume","url":"https://videohub.com/some_video.mp4","position":"30:41"},
    {"event":"load","object":"document"}
  ],
  "attributes": [
    {"name":"attr1","value":"some","when":"01.01.2019"}
  ]
}
```

### Push v1.json to Feeder

```sh
curl -H "Content-Type: application/json" -d @v1.json http://127.0.0.1:1081/v1
```

### Get result from Feeder

```
Body => {  "origin": "https://www.somewebsite.com",  "agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 11_4_1 like Mac OS X)",  "session": "sdfdfwdf33451345345",  "cookie": "PHPSESSID",  "referrer": "https://google.com/?text=some",  "lang": "en",  "fingerprint": "2",  "country": "US",  "property": "other",  "events": [    {"object":"player","event":"resume","url":"https://videohub.com/some_video.mp4","position":"30:41"},    {"event":"load","object":"document"}  ],  "attributes": [    {"name":"attr1","value":"some","when":"01.01.2019"}  ]}
Original event => {"version":"v1","timeMs":1581613922641,"method":"POST","object":{"origin":"https://www.somewebsite.com","ipv4":"127.0.0.1","agent":"Mozilla/5.0 (iPhone; CPU iPhone OS 11_4_1 like Mac OS X)","session":"sdfdfwdf33451345345","cookie":"PHPSESSID","referrer":"https://google.com/?text=some","country":"US","lang":"en","fingerprint":"2","property":"other","events":[{"event":"resume","object":"player","position":"30:41","url":"https://videohub.com/some_video.mp4"},{"event":"load","object":"document"}],"attributes":[{"name":"attr1","value":"some","when":"01.01.2019"}],"AgentObject":{"deviceType":"Phone","browserName":"Unknown","browserVersion":"0.0.0","osPlatform":"iPhone","osName":"iOS","osVersion":"11.4.1"}}}
Message to Kafka (topic: messages.v1) => {"timestamp":1581613922641,"origin":"https://www.somewebsite.com","ipv4":"127.0.0.1","agent":1420460528,"session":"sdfdfwdf33451345345","referrer":"https://google.com/?text=some","country":"US","lang":"en","fingerprint":"2","property":"other","events":[958374024,3984410905],"attributes":[3801366925]}
Message to Kafka (topic: agents.v1) => {"timestamp":1581613922641,"agent":1420460528,"property":"other","raw":"Mozilla/5.0 (iPhone; CPU iPhone OS 11_4_1 like Mac OS X)","json":"{\"deviceType\":\"Phone\",\"browserName\":\"Unknown\",\"browserVersion\":\"0.0.0\",\"osPlatform\":\"iPhone\",\"osName\":\"iOS\",\"osVersion\":\"11.4.1\"}"}
Message to Kafka (topic: events.v1) => {"timestamp":1581613922641,"event":3984410905,"property":"other","json":"{\"event\":\"load\",\"object\":\"document\"}","total":1}
Message to Kafka (topic: events.v1) => {"timestamp":1581613922641,"event":958374024,"property":"other","json":"{\"event\":\"resume\",\"object\":\"player\",\"position\":\"30:41\",\"url\":\"https://videohub.com/some_video.mp4\"}","total":1}
Message to Kafka (topic: attributes.v1) => {"timestamp":1581613922641,"attribute":3801366925,"property":"other","json":"{\"name\":\"attr1\",\"value\":\"some\",\"when\":\"01.01.2019\"}","total":1}
```

## Usage

```
Feeder command

Usage:
  feeder [flags]
  feeder [command]

Available Commands:
  help        Help about any command
  version     Print the version number

Flags:
  -h, --help                                 help for feeder
      --http-cert string                     Http cert file or content
      --http-chain string                    Http CA chain file or content
      --http-header-country string           Http header country (default "X-Forwarded-Country")
      --http-header-fingerprint string       Http header fingerprint (default "X-Forwarded-Fingerprint")
      --http-header-ipv4 string              Http header ipv4 (default "X-Forwarded-IPv4")
      --http-header-lang string              Http header lang (default "X-Forwarded-Lang")
      --http-header-origin string            Http header origin (default "X-Forwarded-Origin")
      --http-header-provider string          Http header provider (default "X-Forwarded-Provider")
      --http-header-referrer string          Http header referrer (default "X-Forwarded-Referrer")
      --http-header-session string           Http header session (default "X-Forwarded-Session")
      --http-header-user-agent string        Http header user agent (default "X-Forwarded-UserAgent")
      --http-key string                      Http key file or content
      --http-listen string                   Http listen (default ":80")
      --http-tls                             Http TLS
      --http-url-v1 string                   Http url (default "/v1")
      --kafka-brokers string                 Kafka brokers
      --kafka-client-id string               Kafka client id (default "feeder_kafka")
      --kafka-flush-frequency int            Kafka Producer flush frequency (default 1)
      --kafka-flush-max-messages int         Kafka Producer flush max messages (default 100)
      --kafka-net-dial-timeout int           Kafka Net dial timeout (default 30)
      --kafka-net-max-open-requests int      Kafka Net max open requests (default 5)
      --kafka-net-read-timeout int           Kafka Net read timeout (default 30)
      --kafka-net-write-timeout int          Kafka Net write timeout (default 30)
      --kafka-topic-agents-v1 string         Kafka agents v1 topic (default "agents.v1")
      --kafka-topic-attributes-v1 string     Kafka attributes v1 topic (default "attributes.v1")
      --kafka-topic-default-part-v1 string   Kafka messages default v1 topic part
      --kafka-topic-events-v1 string         Kafka events v1 topic (default "events.v1")
      --kafka-topic-messages-v1 string       Kafka messages v1 topic (default "messages.v1")
      --log-format string                    Log format: json, text, stdout (default "text")
      --log-level string                     Log level: info, warn, error, debug, panic (default "info")
      --log-template string                  Log template (default "{{.func}} [{{.line}}]: {{.msg}}")
      --prometheus-listen string             Prometheus listen (default "127.0.0.1:8080")
      --prometheus-url string                Prometheus endpoint url (default "/metrics")
```

## Environment variables

For containerization purpose all command switches have environment variables analogs.

- FEEDER_LOG_FORMAT
- FEEDER_LOG_LEVEL
- FEEDER_LOG_TEMPLATE
- FEEDER_PROMETHEUS_URL
- FEEDER_PROMETHEUS_LISTEN
- FEEDER_HTTP_LISTEN
- FEEDER_HTTP_TLS
- FEEDER_HTTP_CERT
- FEEDER_HTTP_KEY
- FEEDER_HTTP_CHAIN
- FEEDER_HTTP_URL_V1
- FEEDER_HTTP_HEADER_ORIGIN
- FEEDER_HTTP_HEADER_IPV4
- FEEDER_HTTP_HEADER_SESSION
- FEEDER_HTTP_HEADER_USER_AGENT
- FEEDER_HTTP_HEADER_REFERRER
- FEEDER_HTTP_HEADER_LANG
- FEEDER_HTTP_HEADER_FINGERPRINT
- FEEDER_HTTP_HEADER_COUNTRY
- FEEDER_HTTP_HEADER_PROVIDER
- FEEDER_HTTP_HEADER_PROPERTY
- FEEDER_KAFKA_BROKERS
- FEEDER_KAFKA_CLIENT_ID
- FEEDER_KAFKA_FLUSH_FREQUENCY
- FEEDER_KAFKA_FLUSH_MAX_MESSAGES
- FEEDER_KAFKA_NET_MAX_OPEN_REQUESTS
- FEEDER_KAFKA_NET_DIAL_TIMEOUT
- FEEDER_KAFKA_NET_READ_TIMEOUT
- FEEDER_KAFKA_NET_WRITE_TIMEOUT
- FEEDER_KAFKA_TOPIC_DEFAULT_PART_V1
- FEEDER_KAFKA_TOPIC_MESSAGES_V1
- FEEDER_KAFKA_TOPIC_AGENTS_V1
- FEEDER_KAFKA_TOPIC_EVENTS_V1
- FEEDER_KAFKA_TOPIC_ATTRIBUTES_V1