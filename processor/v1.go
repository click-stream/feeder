package processor

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/devopsext/utils"
	"github.com/gorilla/mux"

	"github.com/avct/uasurfer"
	"github.com/click-stream/feeder/common"
	"github.com/prometheus/client_golang/prometheus"
)

var processorRequestsV1 = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "feeder_processor_requests_v1",
	Help: "Count of all v1 processor requests",
}, []string{})

type ProcessorV1 struct {
	outputs *common.Outputs
	options *ProcessorOptions
}

func (p *ProcessorV1) parseAgent(agent string) *common.AgentV1 {

	ua := uasurfer.Parse(agent)
	if ua == nil {

		log.Warn("Can't parse user agent")
		return nil
	}

	return &common.AgentV1{
		DeviceType:     ua.DeviceType.StringTrimPrefix(),
		BrowserName:    ua.Browser.Name.StringTrimPrefix(),
		BrowserVersion: fmt.Sprintf("%d.%d.%d", ua.Browser.Version.Major, ua.Browser.Version.Minor, ua.Browser.Version.Patch),
		OSPlatform:     ua.OS.Platform.StringTrimPrefix(),
		OSName:         ua.OS.Name.StringTrimPrefix(),
		OSVersion:      fmt.Sprintf("%d.%d.%d", ua.OS.Version.Major, ua.OS.Version.Minor, ua.OS.Version.Patch),
	}
}

func SetupCors(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, Cookie")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
}

func (p *ProcessorV1) HandleHttpRequest(w http.ResponseWriter, r *http.Request) {

	requestVariables := mux.Vars(r)

	SetupCors(w, r)
	if r.Method == "OPTIONS" {
		w.WriteHeader(200)
		return
	}

	var body []byte

	if r.Body != nil {

		if data, err := ioutil.ReadAll(r.Body); err == nil {
			body = data
		}
	}

	if len(body) == 0 {

		log.Error("Empty body")
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	log.Debug("Body => %s", body)

	supportedTypes := []interface{}{"application/json", "application/x-base64"}
	contentType := r.Header.Get("Content-Type")

	if !utils.Contains(supportedTypes, contentType) {

		log.Error("Content-Type=%s, expect %s", contentType, supportedTypes)
		http.Error(w, "invalid Content-Type", http.StatusUnsupportedMediaType)
		return
	}

	if contentType == "application/x-base64" {

		data := make([]byte, base64.StdEncoding.DecodedLen(len(body)))

		l, err := base64.StdEncoding.Decode(data, body)

		if err != nil {
			log.Error("Expect application/x-base64")
			http.Error(w, "invalid Content-Type", http.StatusUnsupportedMediaType)
			return
		}

		body = data[:l]
	}

	var v1 *common.ObjectV1

	err := json.Unmarshal(body, &v1)
	if err != nil {
		log.Error("Can't unmarshal v1: %v", err)
		http.Error(w, fmt.Sprintf("could not unmarshall v1: %v", err), http.StatusInternalServerError)
		return
	}

	var origin = ""
	var ipv4 = ""
	var session = ""

	var agent = ""
	var referrer = ""
	var lang = ""
	var fingerprint = ""
	var country = ""
	var provider = ""
	var property = ""

	if p.options != nil {

		origin = r.Header.Get(p.options.HeaderOrigin)
		ipv4 = r.Header.Get(p.options.HeaderIPv4)
		session = r.Header.Get(p.options.HeaderSession)
		agent = r.Header.Get(p.options.HeaderUserAgent)
		referrer = r.Header.Get(p.options.HeaderReferrer)
		lang = r.Header.Get(p.options.HeaderLang)
		fingerprint = r.Header.Get(p.options.HeaderFingerprint)
		country = r.Header.Get(p.options.HeaderCountry)
		provider = r.Header.Get(p.options.HeaderProvider)
		property = r.Header.Get(p.options.HeaderProperty)
	}

	if utils.IsEmpty(v1.Origin) {
		v1.Origin = origin
	}

	if utils.IsEmpty(v1.IPv4) {
		v1.IPv4 = ipv4
	}

	if utils.IsEmpty(v1.IPv4) {
		v1.IPv4 = r.RemoteAddr
	}

	if !utils.IsEmpty(v1.IPv4) {

		IPPort := strings.Split(v1.IPv4, ":")
		if len(IPPort) > 0 {
			v1.IPv4 = IPPort[0]
		}
	}

	if utils.IsEmpty(v1.Session) {
		v1.Session = session
	}

	if utils.IsEmpty(v1.Session) {

		if cookie, _ := r.Cookie(v1.Cookie); cookie != nil {

			v1.Session = cookie.Value
		}
	}

	agentV1 := v1.AgentString

	if utils.IsEmpty(agentV1) {
		agentV1 = agent
	}

	if utils.IsEmpty(agentV1) {
		agentV1 = r.Header.Get("User-Agent")
	}

	v1.AgentString = agentV1
	v1.AgentObject = p.parseAgent(agentV1)

	if utils.IsEmpty(v1.Referrer) {
		v1.Referrer = referrer
	}

	if utils.IsEmpty(v1.Lang) {
		v1.Lang = lang
	}

	if utils.IsEmpty(v1.Fingerprint) {
		v1.Fingerprint = fingerprint
	}

	if utils.IsEmpty(v1.Country) {
		v1.Country = country
	}

	if utils.IsEmpty(v1.Provider) {
		v1.Provider = provider
	}

	if utils.IsEmpty(v1.Property) {
		v1.Property = property
	}

	err = p.outputs.Send(&common.Message{

		Version: common.V1,
		TimeMs:  uint64(time.Now().UTC().UnixNano() / (1000 * 1000)),
		Method:  r.Method,
		Object:  v1,
	}, requestVariables)

	if err != nil {
		log.Error("Can't send message: %v", err)
		http.Error(w, fmt.Sprintf("could not send message: %v", err), http.StatusInternalServerError)
		return
	}

	if _, err := w.Write([]byte("OK\n")); err != nil {

		log.Error("Can't write response: %v", err)
		http.Error(w, fmt.Sprintf("could not write response: %v", err), http.StatusInternalServerError)
		return
	}

}

func NewProcessorV1(outputs *common.Outputs, options *ProcessorOptions) *ProcessorV1 {
	return &ProcessorV1{
		outputs: outputs,
		options: options,
	}
}

func init() {
	prometheus.Register(processorRequestsV1)
}
