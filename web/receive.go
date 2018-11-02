package web

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime/debug"

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
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.receiver.Receive(reqBuf)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
