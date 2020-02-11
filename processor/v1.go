package processor

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/avct/uasurfer"
	"github.com/click-stream/feeder/common"
	"github.com/prometheus/client_golang/prometheus"
)

var v1ProcessorRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "feeder_v1_processor_requests",
	Help: "Count of all v1 processor requests",
}, []string{})

type V1ProcessorConfig struct {
	HeaderOrigin      string
	HeaderIPv4        string
	HeaderSession     string
	HeaderUserAgent   string
	HeaderReferrer    string
	HeaderLang        string
	HeaderFingerprint string
	HeaderCountry     string
	HeaderProvider    string
	HeaderProperty    string
}

type V1Processor struct {
	outputs *common.Outputs
	config  *V1ProcessorConfig
}

func (p *V1Processor) parseAgent(agent string) *common.AgentV1 {

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

func (p *V1Processor) HandleHttpRequest(w http.ResponseWriter, r *http.Request) {

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

	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {

		log.Error("Content-Type=%s, expect application/json", contentType)
		http.Error(w, "invalid Content-Type, expect application/json", http.StatusUnsupportedMediaType)
		return
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

	if p.config != nil {

		origin = r.Header.Get(p.config.HeaderOrigin)
		ipv4 = r.Header.Get(p.config.HeaderIPv4)
		session = r.Header.Get(p.config.HeaderSession)
		agent = r.Header.Get(p.config.HeaderUserAgent)
		referrer = r.Header.Get(p.config.HeaderReferrer)
		lang = r.Header.Get(p.config.HeaderLang)
		fingerprint = r.Header.Get(p.config.HeaderFingerprint)
		country = r.Header.Get(p.config.HeaderCountry)
		provider = r.Header.Get(p.config.HeaderProvider)
		property = r.Header.Get(p.config.HeaderProperty)
	}

	if common.IsEmpty(v1.Origin) {
		v1.Origin = origin
	}

	if common.IsEmpty(v1.IPv4) {
		v1.IPv4 = ipv4
	}

	if common.IsEmpty(v1.IPv4) {
		v1.IPv4 = r.RemoteAddr
	}

	if !common.IsEmpty(v1.IPv4) {

		IpPort := strings.Split(v1.IPv4, ":")
		if len(IpPort) > 0 {
			v1.IPv4 = IpPort[0]
		}
	}

	if common.IsEmpty(v1.Session) {
		v1.Session = session
	}

	if common.IsEmpty(v1.Session) {

		if cookie, _ := r.Cookie(v1.Cookie); cookie != nil {

			v1.Session = cookie.Value
		}
	}

	v1Agent := v1.AgentString

	if common.IsEmpty(v1Agent) {
		v1Agent = agent
	}

	if common.IsEmpty(v1Agent) {
		v1Agent = r.Header.Get("User-Agent")
	}

	v1.AgentString = v1Agent
	v1.AgentObject = p.parseAgent(v1Agent)

	if common.IsEmpty(v1.Referrer) {
		v1.Referrer = referrer
	}

	if common.IsEmpty(v1.Lang) {
		v1.Lang = lang
	}

	if common.IsEmpty(v1.Fingerprint) {
		v1.Fingerprint = fingerprint
	}

	if common.IsEmpty(v1.Country) {
		v1.Country = country
	}

	if common.IsEmpty(v1.Provider) {
		v1.Provider = provider
	}

	if common.IsEmpty(v1.Property) {
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

func NewV1Processor(outputs *common.Outputs, config *V1ProcessorConfig) *V1Processor {
	return &V1Processor{
		outputs: outputs,
		config:  config,
	}
}

func init() {
	prometheus.Register(v1ProcessorRequests)
}
