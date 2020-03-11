package input

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/click-stream/feeder/common"
	"github.com/click-stream/feeder/processor"
	"github.com/devopsext/utils"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/click-stream/feeder/thirdparty/ratecounter"
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

func getRequestsRate(httpInputRequestsRates map[string]*RequestsRate, r *http.Request, rCount int64) *RequestsRate {
	requestVariables := mux.Vars(r)
	var rLength int64 = 0
	if rCount > 0 {
		rLength = r.ContentLength
	}
	if feederId, exist := requestVariables["feeder_id"]; exist {
		if rr, ok := httpInputRequestsRates[feederId]; ok {
			rr.incr(rCount, rLength)
			return rr
		} else {
			httpInputRequestsRates[feederId] = newRequestsRate(rCount, rLength)
			return httpInputRequestsRates[feederId]
		}
	}
	return nil
}

type HttpInputOptions struct {
	Listen string
	Tls    bool
	Cert   string
	Key    string
	Chain  string
	URLv1  string
	Cors   bool
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
			})

			router.HandleFunc(h.options.URLv1+"/{feeder_id:[a-z0-9]{8,8}}", func(w http.ResponseWriter, r *http.Request) {
				httpInputRequests.WithLabelValues(r.URL.Path).Inc()

				rr := getRequestsRate(httpInputRequestsRates, r, 1)
				if rr != nil {
					rps := float64(rr.rps.Rate())
					bps := float64(rr.bps.Rate())
					fmt.Println("rps: ", rps, " bps: ", bps)
					httpInputRequestsRPS.WithLabelValues(r.URL.Path).Set(rps)
					httpInputRequestsBPS.WithLabelValues(r.URL.Path).Set(bps)
				}

				h.SetupCors(w, r)
				processor.NewProcessorV1(outputs, &h.processorOptions).HandleHttpRequest(w, r)
			})
			router.HandleFunc(h.options.URLv1+"/{feeder_id:[a-z0-9]{8,8}}/rps", func(w http.ResponseWriter, r *http.Request) {
				h.SetupCors(w, r)
				rr := getRequestsRate(httpInputRequestsRates, r, 0)
				if rr != nil {
					w.Write([]byte(rr.rps.String()))
				}
			})
			router.HandleFunc(h.options.URLv1+"/{feeder_id:[a-z0-9]{8,8}}/bps", func(w http.ResponseWriter, r *http.Request) {
				h.SetupCors(w, r)
				rr := getRequestsRate(httpInputRequestsRates, r, 0)
				if rr != nil {
					w.Write([]byte(rr.bps.String()))
				}
			})
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
