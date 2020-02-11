package common

type Output interface {
	Send(m *Message, variables map[string]string)
}
