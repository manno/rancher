package logserver

import (
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/rancher/rancher/pkg/log"
)

var (
	DefaultSocketLocation = "/tmp/log.sock"
)

// Server structure is used to the store backend information
type Server struct {
	SocketLocation string
	Debug          bool
}

// StartServerWithDefaults starts the server with default values
func StartServerWithDefaults() {
	s := Server{
		SocketLocation: DefaultSocketLocation,
	}
	s.Start()
}

// Start the server
func (s *Server) Start() {
	os.Remove(s.SocketLocation)
	go s.ListenAndServe()
}

// ListenAndServe is used to setup handlers and
// start listening on the specified location
func (s *Server) ListenAndServe() error {
	log.Info("Listening on", "socket", s.SocketLocation)
	server := http.Server{}
	http.HandleFunc("/v1/loglevel", s.loglevel)
	socketListener, err := net.Listen("unix", s.SocketLocation)
	if err != nil {
		return err
	}
	return server.Serve(socketListener)
}

func (s *Server) loglevel(rw http.ResponseWriter, req *http.Request) {
	// curl -X POST -d "level=debug" localhost:12345/v1/loglevel
	log.Debug("Received loglevel request")
	if req.Method == http.MethodGet {
		level := log.GetLevel()
		rw.Write([]byte(fmt.Sprintf("%s\n", level)))
	}

	if req.Method == http.MethodPost {
		if err := req.ParseForm(); err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			rw.Write([]byte(fmt.Sprintf("Failed to parse form: %v\n", err)))
		}
		levelStr := req.Form.Get("level")
		switch levelStr {
		case "trace", "debug", "info", "warn", "warning", "error":
			log.SetLevel(levelStr)
			rw.Write([]byte("OK\n"))
		default:
			rw.WriteHeader(http.StatusInternalServerError)
			rw.Write([]byte(fmt.Sprintf("Failed to parse loglevel: %v\n", levelStr)))
		}
	}
}
