package web

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime/debug"

	"github.com/brancz/thanos-remote-receive/receive"
	"github.com/golang/snappy"
)

func (h *Handler) receive(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("panic recovered:", r, string(debug.Stack()))
		}
	}()

	compressed, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		fmt.Println("snappy decode error")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	wreq := &receive.PartialWriteRequest{}
	if err := wreq.Unmarshal(reqBuf); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	//for _, t := range wreq.Timeseries {
	//if err := t.UnmarshalLabels(); err != nil {
	//http.Error(w, err.Error(), http.StatusBadRequest)
	//return
	//}
	//}

	err = h.receiver.Receive(wreq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
