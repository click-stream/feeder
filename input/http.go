package input

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/click-stream/feeder/common"
	"github.com/click-stream/feeder/processor"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
)

var httpInputRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "feeder_http_input_requests",
	Help: "Count of all http input requests",
}, []string{"feeder_http_input_url"})

type HttpInput struct {
	v1Url    string
	listen   string
	tls      bool
	cert     string
	key      string
	chain    string
	v1Config *processor.V1ProcessorConfig
}

func (h *HttpInput) Start(wg *sync.WaitGroup, outputs *common.Outputs) {

	wg.Add(1)

	go func(wg *sync.WaitGroup) {

		defer wg.Done()

		log.Info("Start http input...")

		var caPool *x509.CertPool
		var certificates []tls.Certificate

		if h.tls {

			// load certififcate
			var cert []byte
			if _, err := os.Stat(h.cert); err == nil {

				cert, err = ioutil.ReadFile(h.cert)
				if err != nil {
					log.Panic(err)
				}
			} else {
				cert = []byte(h.cert)
			}

			// load key
			var key []byte
			if _, err := os.Stat(h.key); err == nil {

				key, err = ioutil.ReadFile(h.key)
				if err != nil {
					log.Panic(err)
				}
			} else {
				key = []byte(h.key)
			}

			// make pair from certificate and pair
			pair, err := tls.X509KeyPair(cert, key)
			if err != nil {
				log.Panic(err)
			}

			certificates = append(certificates, pair)

			// load CA chain
			var chain []byte
			if _, err := os.Stat(h.chain); err == nil {

				chain, err = ioutil.ReadFile(h.chain)
				if err != nil {
					log.Panic(err)
				}
			} else {
				chain = []byte(h.chain)
			}

			// make pool of chains
			caPool = x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(chain) {
				log.Debug("CA chain is invalid")
			}
		}

		//mux := http.NewServeMux()

		router := mux.NewRouter()

		if !common.IsEmpty(h.v1Url) {

			//mux.HandleFunc(h.v1Url, func(w http.ResponseWriter, r *http.Request) {
			router.HandleFunc(h.v1Url, func(w http.ResponseWriter, r *http.Request) {
				httpInputRequests.WithLabelValues(r.URL.Path).Inc()
				processor.NewV1Processor(outputs, h.v1Config).HandleHttpRequest(w, r)
			})
			router.HandleFunc(h.v1Url+"/{id:[a-z0-9]{8,8}}", func(w http.ResponseWriter, r *http.Request) {
				httpInputRequests.WithLabelValues(r.URL.Path).Inc()
				processor.NewV1Processor(outputs, h.v1Config).HandleHttpRequest(w, r)
			})
		}

		listener, err := net.Listen("tcp", h.listen)
		if err != nil {
			log.Panic(err)
		}

		log.Info("Http input is up. Listening...")

		//srv := &http.Server{Handler: mux}
		srv := &http.Server{Handler: router}

		if h.tls {

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

func NewHttpInput(v1Url string, listen string, tls bool,
	cert string, key string, chain string,
	v1Config *processor.V1ProcessorConfig) *HttpInput {

	return &HttpInput{
		v1Url:    v1Url,
		listen:   listen,
		tls:      tls,
		cert:     cert,
		key:      key,
		chain:    chain,
		v1Config: v1Config,
	}
}

func init() {
	prometheus.Register(httpInputRequests)
}
