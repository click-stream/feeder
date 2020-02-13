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
	"github.com/devopsext/utils"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
)

var httpInputRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "feeder_http_input_requests",
	Help: "Count of all http input requests",
}, []string{"feeder_http_input_url"})

type HttpInputOptions struct {
	Listen string
	Tls    bool
	Cert   string
	Key    string
	Chain  string
	URLv1  string
}

type HttpInput struct {
	options          HttpInputOptions
	processorOptions processor.ProcessorOptions
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
				processor.NewProcessorV1(outputs, &h.processorOptions).HandleHttpRequest(w, r)
			})

			router.HandleFunc(h.options.URLv1+"/{id:[a-z0-9]{8,8}}", func(w http.ResponseWriter, r *http.Request) {
				httpInputRequests.WithLabelValues(r.URL.Path).Inc()
				processor.NewProcessorV1(outputs, &h.processorOptions).HandleHttpRequest(w, r)
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
}
