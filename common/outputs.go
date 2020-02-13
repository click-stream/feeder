package common

import (
	"encoding/json"
	"errors"
	"time"
)

type Outputs struct {
	list []*Output
}

func (ots *Outputs) Add(o *Output) {

	ots.list = append(ots.list, o)
}

func (ots *Outputs) Send(m *Message, variables map[string]string) error {

	if m == nil {

		return errors.New("Message is not found")
	}

	if m.TimeMs == 0 {
		m.TimeMs = uint64(time.Now().UTC().UnixNano() / (1000 * 1000))
	}

	bytes, err := json.Marshal(m)
	if err != nil {
		log.Error(err)
		return err
	}

	log.Debug("Original event => %s", string(bytes))

	var object interface{}

	if err := json.Unmarshal(bytes, &object); err != nil {

		log.Error(err)
		return err
	}

	for _, o := range ots.list {

		if o != nil {

			(*o).Send(m, variables)

		} else {
			log.Warn("Output is not defined")
		}
	}

	return nil
}

func NewOutputs() *Outputs {
	return &Outputs{}
}
