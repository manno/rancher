package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/mattn/go-colorable"
	"github.com/rancher/rancher/pkg/agent/clean"
	"github.com/rancher/rancher/pkg/agent/clean/adunmigration"
	"github.com/rancher/rancher/pkg/agent/cluster"
	"github.com/rancher/rancher/pkg/agent/rancher"
	"github.com/rancher/rancher/pkg/controllers/managementuser/cavalidator"
	"github.com/rancher/rancher/pkg/features"
	rancherlog "github.com/rancher/rancher/pkg/log"
	"github.com/rancher/rancher/pkg/logserver"
	"github.com/rancher/rancher/pkg/utils"
	"github.com/rancher/remotedialer"
	"github.com/rancher/wrangler/v3/pkg/signals"
)

var (
	VERSION = "dev"
)

const (
	Token          = "X-API-Tunnel-Token"
	Params         = "X-API-Tunnel-Params"
	caFileLocation = "/etc/kubernetes/ssl/certs/serverca"
)

func main() {
	var err error
	ctx := context.Background()

	configureLog()

	logserver.StartServerWithDefaults()

	initFeatures()

	// The cleanup is only performed by the cattle-cluster-agent,
	// in whose template the CATTLE_CREDENTIAL_NAME environment variable is set
	if os.Getenv("CATTLE_CREDENTIAL_NAME") != "" {
		rancherlog.Info("Starting cattle-credential-cleanup goroutine in the background")
		go clean.UnusedCattleCredentials()
	}

	if os.Getenv("CLUSTER_CLEANUP") == "true" {
		err = clean.Cluster()
	} else if os.Getenv("BINDING_CLEANUP") == "true" {
		err = errors.Join(
			clean.DuplicateBindings(nil),
			clean.OrphanBindings(nil),
		)
	} else if os.Getenv("AD_GUID_CLEANUP") == "true" {
		dryrun := os.Getenv("DRY_RUN") == "true"
		deleteMissingUsers := os.Getenv("AD_DELETE_MISSING_GUID_USERS") == "true"
		err = adunmigration.UnmigrateAdGUIDUsers(nil, dryrun, deleteMissingUsers)
	} else {
		err = run(ctx)
	}

	if err != nil {
		rancherlog.Fatal("Agent failed to initialize", "error", err)
	}
}

func initFeatures() {
	features.InitializeFeatures(nil, os.Getenv("CATTLE_FEATURES"))
}

func getParams() (map[string]interface{}, error) {
	return cluster.Params()
}

func getTokenAndURL() (string, string, error) {
	return cluster.TokenAndURL()
}

func isConnect() bool {
	if os.Getenv("CATTLE_AGENT_CONNECT") == "true" {
		return true
	}
	_, err := os.Stat("connected")
	return err == nil
}

func connected() {
	f, err := os.Create("connected")
	if err != nil {
		f.Close()
	}
}

func run(ctx context.Context) error {
	topContext := signals.SetupSignalContext()

	rancherlog.Info("Rancher agent is starting", "version", VERSION)
	params, err := getParams()
	if err != nil {
		return err
	}
	writeCertsOnly := os.Getenv("CATTLE_WRITE_CERT_ONLY") == "true"
	bytes, err := json.Marshal(params)
	if err != nil {
		return err
	}

	token, server, err := getTokenAndURL()
	if err != nil {
		return err
	}

	headers := http.Header{
		Token:  {token},
		Params: {base64.StdEncoding.EncodeToString(bytes)},
	}

	serverURL, err := url.Parse(server)
	if err != nil {
		return err
	}

	topContext = context.WithValue(topContext, cavalidator.CacertsValid, false)

	// Perform root CA verification
	var transport *http.Transport
	systemStoreConnectionCheckRequired := true
	transport = rootCATransport()
	if transport != nil {
		rancherlog.Info("Testing connection to server using trusted certificate authorities", "server", server, "ca_file_location", caFileLocation)
		var httpClient = &http.Client{
			Timeout:   time.Second * 5,
			Transport: transport,
		}
		if _, err = httpClient.Get(server); err != nil {
			if cluster.CAStrictVerify() {
				rancherlog.Error("Could not securely connect to server", "server", server, "error", err)
				os.Exit(1)
			}
			// onConnect will use the transport later on, so discard it as it doesn't work and fallback to the system store.
			transport = nil
		} else {
			topContext = context.WithValue(topContext, cavalidator.CacertsValid, true)
			systemStoreConnectionCheckRequired = false
		}
	} else if cluster.CAStrictVerify() {
		rancherlog.Error("Strict CA verification is enabled but encountered error finding root CA")
		os.Exit(1)
	}

	if systemStoreConnectionCheckRequired {
		// Check if secure connection can be made successfully
		var httpClient = &http.Client{
			Timeout: time.Second * 5,
		}
		_, err = httpClient.Get(server)
		if err != nil {
			if strings.Contains(err.Error(), "x509:") {
				certErr := err
				if strings.Contains(err.Error(), "certificate signed by unknown authority") {
					certErr = fmt.Errorf("Certificate chain is not complete, please check if all needed intermediate certificates are included in the server certificate (in the correct order) and if the cacerts setting in Rancher either contains the correct CA certificate (in the case of using self signed certificates) or is empty (in the case of using a certificate signed by a recognized CA). Certificate information is displayed above. error: %s", err)
				}
				if strings.Contains(err.Error(), "certificate has expired or is not yet valid") {
					certErr = fmt.Errorf("Server certificate is not valid, please check if the host has the correct time configured and if the server certificate has a notAfter date and time in the future. Certificate information is displayed above. error: %s", err)
				}
				if strings.Contains(err.Error(), "because it doesn't contain any IP SANs") || strings.Contains(err.Error(), "certificate is not valid for any names, but wanted to match") || strings.Contains(err.Error(), "cannot validate certificate for") {
					certErr = fmt.Errorf("Server certificate does not contain correct DNS and/or IP address entries in the Subject Alternative Names (SAN). Certificate information is displayed above. error: %s", err)
				}
				insecureClient := &http.Client{
					Timeout: time.Second * 5,
					Transport: &http.Transport{
						TLSClientConfig: &tls.Config{
							InsecureSkipVerify: true,
						},
					},
				}
				res, err := insecureClient.Get(server)
				if err != nil {
					rancherlog.Error("Could not connect to server", "server", server, "error", err)
					os.Exit(1)
				}
				var lastFoundIssuer string
				if res.TLS != nil && len(res.TLS.PeerCertificates) > 0 {
					rancherlog.Info("Certificate details from server", "server_url", serverURL)
					var previouscert *x509.Certificate
					for i := range res.TLS.PeerCertificates {
						cert := res.TLS.PeerCertificates[i]
						rancherlog.Info("Certificate info", "index", i, "server_url", serverURL)
						certinfo(cert)
						if i > 0 {
							if previouscert.Issuer.String() != cert.Subject.String() {
								rancherlog.Error("Certificate subject does not match previous certificate issuer. Please check if the configured server certificate contains all needed intermediate certificates and make sure they are in the correct order (server certificate first, intermediates after)", "subject", cert.Subject.String(), "previous_issuer", previouscert.Issuer.String())
							}
						}
						previouscert = cert
						lastFoundIssuer = cert.Issuer.String()
					}
				}
				if _, err := os.Stat(caFileLocation); err == nil {
					caFile, err := ioutil.ReadFile(caFileLocation)
					if err != nil {
						return err
					}
					var blocks [][]byte
					for {
						var certDERBlock *pem.Block
						certDERBlock, caFile = pem.Decode(caFile)
						if certDERBlock == nil {
							break
						}

						if certDERBlock.Type == "CERTIFICATE" {
							blocks = append(blocks, certDERBlock.Bytes)
						}
					}
					if len(blocks) > 1 {
						rancherlog.Warn("Found multiple certificates at CA file location, should be one", "count", len(blocks), "ca_file_location", caFileLocation)
					}
					rancherlog.Info("Certificate details for CA file", "ca_file_location", caFileLocation)

					blockcount := 0
					var lastCACert *x509.Certificate
					for _, block := range blocks {
						cert, err := x509.ParseCertificate(block)
						if err != nil {
							rancherlog.Error("Failed to parse certificate", "error", err)
							continue
						}

						rancherlog.Info("Certificate info", "index", blockcount, "ca_file_location", caFileLocation)
						certinfo(cert)

						blockcount = blockcount + 1
						lastCACert = cert
					}
					if lastFoundIssuer != lastCACert.Issuer.String() {
						rancherlog.Error("Issuer of last certificate in chain does not match CA certificate issuer. Please check if the configured server certificate contains all needed intermediate certificates and make sure they are in the correct order (server certificate first, intermediates after)", "last_found_issuer", lastFoundIssuer, "ca_certificate_issuer", lastCACert.Issuer.String())
					}
				}
				return certErr
			}
		}
	}

	onConnect := func(ctx context.Context, _ *remotedialer.Session) error {
		connected()

		if writeCertsOnly {
			exitCertWriter(ctx)
		}

		err = rancher.Run(topContext)
		if err != nil {
			rancherlog.Fatal("Failed to run rancher agent", "error", err)
		}
		return nil
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	for {
		safeHost := serverURL.Host
		if utils.IsPlainIPV6(safeHost) {
			safeHost = fmt.Sprintf("[%s]", safeHost)
		}
		wsURL := fmt.Sprintf("wss://%s/v3/connect", safeHost)
		if !isConnect() {
			wsURL += "/register"
		}

		rancherlog.Info("Connecting to server", "url", wsURL, "token_prefix", token[:len(token)/2])
		rancherlog.Trace("Connecting to server", "url", wsURL, "token", token)
		remotedialer.ClientConnect(ctx, wsURL, headers, nil, func(proto, address string) bool {
			switch proto {
			case "tcp":
				return true
			case "unix":
				return address == "/var/run/docker.sock"
			case "npipe":
				return address == "//./pipe/docker_engine"
			}
			return false
		}, onConnect)
		time.Sleep(5 * time.Second)
	}
}

func exitCertWriter(ctx context.Context) {
	// share-mnt process needs an always restart policy and to be killed so it can restart on startup
	// this functionality is really only needed for OSes with ephemeral /etc like RancherOS
	// everything here will just exit(0) with errors as we need to bail out completely.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	// trap SIGTERM here so that container can exit with 0
	go func() {
		<-sigs
		os.Exit(0)
	}()

	rancherlog.Info("Attempting to stop share-mnt container")
	c, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation(), client.FromEnv)
	if err != nil {
		rancherlog.Error("Operation failed", "error", err)
		os.Exit(0)
	}

	args := filters.NewArgs()
	args.Add("label", "io.rancher.rke.container.name=share-mnt")
	containers, err := c.ContainerList(ctx, types.ContainerListOptions{
		All:     true,
		Filters: args,
	})
	if err != nil {
		rancherlog.Error("Operation failed", "error", err)
		os.Exit(0)
	}

	for _, container := range containers {
		if len(container.Names) > 0 && strings.Contains(container.Names[0], "share-mnt") {
			err := c.ContainerKill(ctx, container.ID, "SIGTERM")
			if err != nil {
				rancherlog.Error("Operation failed", "error", err)
				os.Exit(0) // only need to write certs so exit cleanly
			}
		}
	}
	// wait for itself to be kill with SIGTERM so it can return exit 0
	select {}
}

func certinfo(cert *x509.Certificate) {
	rancherlog.Info("Certificate subject", "subject", cert.Subject)
	rancherlog.Info("Certificate issuer", "issuer", cert.Issuer)
	rancherlog.Info("Certificate is CA", "is_ca", cert.IsCA)
	if len(cert.DNSNames) > 0 {
		rancherlog.Info("Certificate DNS names", "dns_names", cert.DNSNames)
	} else {
		rancherlog.Info("Certificate DNS names", "dns_names", "<none>")
	}
	if len(cert.IPAddresses) > 0 {
		rancherlog.Info("Certificate IP addresses", "ip_addresses", cert.IPAddresses)
	} else {
		rancherlog.Info("Certificate IP addresses", "ip_addresses", "<none>")
	}
	rancherlog.Info("Certificate not before", "not_before", cert.NotBefore)
	rancherlog.Info("Certificate not after", "not_after", cert.NotAfter)
	rancherlog.Info("Certificate signature algorithm", "signature_algorithm", cert.SignatureAlgorithm)
	rancherlog.Info("Certificate public key algorithm", "public_key_algorithm", cert.PublicKeyAlgorithm)
}

func configureLog() {
	level := "info"
	if os.Getenv("CATTLE_TRACE") == "true" || os.Getenv("RANCHER_TRACE") == "true" {
		level = "trace"
	} else if os.Getenv("CATTLE_DEBUG") == "true" || os.Getenv("RANCHER_DEBUG") == "true" {
		level = "debug"
	}

	// Use text format for agent (colorable output)
	rancherlog.Init("text", level, colorable.NewColorableStdout())
}

// rootCATransport generates a http.Transport that contains the contents of the CA file as the Root CA for strict validation.
func rootCATransport() *http.Transport {
	caFile, err := os.ReadFile(caFileLocation)
	if err != nil {
		rancherlog.Error("Unable to read CA file", "location", caFileLocation, "error", err)
		return nil
	}
	certPool := x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM(caFile); !ok {
		rancherlog.Error("Unable to parse CA file", "location", caFileLocation)
		return nil
	}
	return &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: certPool,
		},
	}
}
