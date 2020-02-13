package processor

import "github.com/devopsext/utils"

type ProcessorOptions struct {
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

var log = utils.GetLog()
