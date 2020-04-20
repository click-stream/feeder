package input

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/click-stream/feeder/common"
	"github.com/click-stream/feeder/processor"
	"github.com/click-stream/ratecounter"
	"github.com/devopsext/utils"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
)

var httpInputRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "feeder_http_input_requests",
	Help: "Count of all http input requests",
}, []string{"feeder_http_input_url"})

var httpInputRequestsRPS = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "feeder_http_input_requests_rps",
	Help: "RPS of all http input requests per url",
}, []string{"feeder_http_input_url_rps"})

var httpInputRequestsBPS = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "feeder_http_input_requests_bps",
	Help: "BPS of all http input requests per url",
}, []string{"feeder_http_input_url_bps"})

type RequestsRate struct {
	rps *ratecounter.RateCounter
	bps *ratecounter.RateCounter
}

func newRequestsRate(requestsCount int64, bytesCount int64) *RequestsRate {
	rr := &RequestsRate{
		rps: ratecounter.NewRateCounter(1 * time.Second).WithResolution(60),
		bps: ratecounter.NewRateCounter(1 * time.Second).WithResolution(60),
	}
	rr.incr(requestsCount, bytesCount)
	return rr
}

func (rr *RequestsRate) incr(requestsCount int64, bytesCount int64) {
	rr.rps.Incr(requestsCount)
	rr.bps.Incr(bytesCount)
}

var httpInputRequestsRates = make(map[string]*RequestsRate)

func getRequestsRateByFeederId(httpInputRequestsRates map[string]*RequestsRate, r *http.Request, rCount int64) *RequestsRate {
	requestVariables := mux.Vars(r)
	if feederId, exist := requestVariables["feeder_id"]; exist {
		return getCustomRequestsRate(feederId, httpInputRequestsRates, r, rCount)
	}
	return nil
}

func getCustomRequestsRate(custom string, httpInputRequestsRates map[string]*RequestsRate, r *http.Request, rCount int64) *RequestsRate {
	var rLength int64 = 0
	if rCount > 0 {
		rLength = r.ContentLength
	}
	if rr, ok := httpInputRequestsRates[custom]; ok {
		rr.incr(rCount, rLength)
		log.Debug(custom+" | "+r.URL.Path+" | rps: %s, bps: %s", rr.rps.String(), rr.bps.String())
		return rr
	} else {
		httpInputRequestsRates[custom] = newRequestsRate(rCount, rLength)
		return httpInputRequestsRates[custom]
	}
}

type HttpInputOptions struct {
	Listen          string
	Tls             bool
	Cert            string
	Key             string
	Chain           string
	URLv1           string
	Cors            bool
	FeederIdPattern string
}

type HttpInput struct {
	options          HttpInputOptions
	processorOptions processor.ProcessorOptions
}

func (h *HttpInput) SetupCors(w http.ResponseWriter, r *http.Request) {
	if h.options.Cors {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, Cookie")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
}

func (h *HttpInput) Start(wg *sync.WaitGroup, outputs *common.Outputs) {

	wg.Add(1)

	go func(wg *sync.WaitGroup) {

		defer wg.Done()

		log.Info("Start http input...")

		var caPool *x509.CertPool
		var certificates []tls.Certificate

		if h.options.Tls {

			// load certififcate
			var cert []byte
			if _, err := os.Stat(h.options.Cert); err == nil {

				cert, err = ioutil.ReadFile(h.options.Cert)
				if err != nil {
					log.Panic(err)
				}
			} else {
				cert = []byte(h.options.Cert)
			}

			// load key
			var key []byte
			if _, err := os.Stat(h.options.Key); err == nil {

				key, err = ioutil.ReadFile(h.options.Key)
				if err != nil {
					log.Panic(err)
				}
			} else {
				key = []byte(h.options.Key)
			}

			// make pair from certificate and pair
			pair, err := tls.X509KeyPair(cert, key)
			if err != nil {
				log.Panic(err)
			}

			certificates = append(certificates, pair)

			// load CA chain
			var chain []byte
			if _, err := os.Stat(h.options.Chain); err == nil {

				chain, err = ioutil.ReadFile(h.options.Chain)
				if err != nil {
					log.Panic(err)
				}
			} else {
				chain = []byte(h.options.Chain)
			}

			// make pool of chains
			caPool = x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(chain) {
				log.Debug("CA chain is invalid")
			}
		}

		//mux := http.NewServeMux()

		router := mux.NewRouter()

		if !utils.IsEmpty(h.options.URLv1) {

			router.HandleFunc(h.options.URLv1, func(w http.ResponseWriter, r *http.Request) {
				httpInputRequests.WithLabelValues(r.URL.Path).Inc()
				h.SetupCors(w, r)
				processor.NewProcessorV1(outputs, &h.processorOptions).HandleHttpRequest(w, r)
				rr := getCustomRequestsRate(h.options.URLv1, httpInputRequestsRates, r, 1)
				if rr != nil {
					rps := float64(rr.rps.Rate())
					bps := float64(rr.bps.Rate())
					httpInputRequestsRPS.WithLabelValues(r.URL.Path).Set(rps)
					httpInputRequestsBPS.WithLabelValues(r.URL.Path).Set(bps)
				}
			})

			router.HandleFunc(h.options.URLv1+"/rate", func(w http.ResponseWriter, r *http.Request) {
				h.SetupCors(w, r)
				if r.Method == "OPTIONS" {
					w.WriteHeader(200)
					return
				}
				rr := getCustomRequestsRate(h.options.URLv1, httpInputRequestsRates, r, 0)
				if rr != nil {
					rate := &struct {
						Rps int64 `json:"rps"`
						Bps int64 `json:"bps"`
					}{
						Rps: rr.rps.Rate(),
						Bps: rr.bps.Rate(),
					}
					str, err := json.Marshal(rate)
					if err != nil {
						log.Error("Can't marshal rate: %v", err)
						return
					}
					log.Debug(string(str))
					if _, err := w.Write(str); err != nil {
						log.Error("Can't write response: %v", err)
						http.Error(w, fmt.Sprintf("could not write response: %v", err), http.StatusInternalServerError)
						return
					}
				}
			})

			if len(strings.TrimSpace(h.options.FeederIdPattern)) > 0 {
				router.HandleFunc(h.options.URLv1+"/"+h.options.FeederIdPattern, func(w http.ResponseWriter, r *http.Request) {
					httpInputRequests.WithLabelValues(r.URL.Path).Inc()

					h.SetupCors(w, r)
					processor.NewProcessorV1(outputs, &h.processorOptions).HandleHttpRequest(w, r)
					rr := getRequestsRateByFeederId(httpInputRequestsRates, r, 1)
					if rr != nil {
						rps := float64(rr.rps.Rate())
						bps := float64(rr.bps.Rate())
						httpInputRequestsRPS.WithLabelValues(r.URL.Path).Set(rps)
						httpInputRequestsBPS.WithLabelValues(r.URL.Path).Set(bps)
					}

				})
				router.HandleFunc(h.options.URLv1+"/"+h.options.FeederIdPattern+"/rate", func(w http.ResponseWriter, r *http.Request) {
					h.SetupCors(w, r)
					if r.Method == "OPTIONS" {
						w.WriteHeader(200)
						return
					}
					rr := getRequestsRateByFeederId(httpInputRequestsRates, r, 0)
					if rr != nil {

						rate := &struct {
							Rps int64 `json:"rps"`
							Bps int64 `json:"bps"`
						}{
							Rps: rr.rps.Rate(),
							Bps: rr.bps.Rate(),
						}
						str, err := json.Marshal(rate)
						if err != nil {
							log.Error("Can't marshal rate: %v", err)
							return
						}
						log.Debug(string(str))
						if _, err := w.Write(str); err != nil {
							log.Error("Can't write response: %v", err)
							http.Error(w, fmt.Sprintf("could not write response: %v", err), http.StatusInternalServerError)
							return
						}
					}
				})
			}

		}

		listener, err := net.Listen("tcp", h.options.Listen)
		if err != nil {
			log.Panic(err)
		}

		log.Info("Http input is up. Listening...")

		//srv := &http.Server{Handler: mux}
		srv := &http.Server{Handler: router}

		if h.options.Tls {

			srv.TLSConfig = &tls.Config{
				Certificates: certificates,
				RootCAs:      caPool,
			}

			err = srv.ServeTLS(listener, "", "")
			if err != nil {
				log.Panic(err)
			}
		} else {
			err = srv.Serve(listener)
			if err != nil {
				log.Panic(err)
			}
		}

	}(wg)
}

func NewHttpInput(options HttpInputOptions, processorOptions processor.ProcessorOptions) *HttpInput {

	return &HttpInput{
		options:          options,
		processorOptions: processorOptions,
	}
}

func init() {
	prometheus.Register(httpInputRequests)
	prometheus.Register(httpInputRequestsRPS)
	prometheus.Register(httpInputRequestsBPS)
}
