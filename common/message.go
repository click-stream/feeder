package common

type AgentV1 struct {
	DeviceType     string `json:"deviceType"`
	BrowserName    string `json:"browserName"`
	BrowserVersion string `json:"browserVersion"`
	OSPlatform     string `json:"osPlatform"`
	OSName         string `json:"osName"`
	OSVersion      string `json:"osVersion"`
}

type ObjectV1 struct {
	Origin      string        `json:"origin,omitempty"`
	IPv4        string        `json:"ipv4,omitempty"`
	AgentString string        `json:"agent,omitempty"`
	Session     string        `json:"session,omitempty"`
	Cookie      string        `json:"cookie,omitempty"`
	Referrer    string        `json:"referrer,omitempty"`
	Country     string        `json:"country,omitempty"`
	Lang        string        `json:"lang,omitempty"`
	Fingerprint string        `json:"fingerprint,omitempty"`
	Provider    string        `json:"provider,omitempty"`
	Property    string        `json:"property,omitempty"`
	Events      []interface{} `json:"events,omitempty"`
	Attributes  []interface{} `json:"attributes,omitempty"`
	AgentObject *AgentV1
}

const V1 = "v1"

type Message struct {
	Version string      `json:"version"`
	TimeMs  uint64      `json:"timeMs"`
	Method  string      `json:"method"`
	Object  interface{} `json:"object,omitempty"`
}
