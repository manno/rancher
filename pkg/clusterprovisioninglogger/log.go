package clusterprovisioninglogger

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/rancher/norman/condition"
	v1 "github.com/rancher/rancher/pkg/generated/norman/core/v1"
	v3 "github.com/rancher/rancher/pkg/generated/norman/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/kontainer-engine/logstream"
	"github.com/rancher/rancher/pkg/log"
	"google.golang.org/grpc/metadata"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	configMapName = "provisioning-log"
)

type logger struct {
	Cluster    *v3.Cluster
	Clusters   v3.ClusterInterface
	ConfigMaps v1.ConfigMapInterface
	done       chan struct{}
	buffer     bytes.Buffer
	bufferLock sync.Mutex
}

func NewLogger(clusters v3.ClusterInterface, configMaps v1.ConfigMapInterface, cluster *v3.Cluster, cond condition.Cond) (context.Context, io.Closer) {
	l := &logger{
		Cluster:    cluster,
		Clusters:   clusters,
		ConfigMaps: configMaps,
		done:       make(chan struct{}),
	}

	_, ctx, logger := l.getCtx(cluster, cond)
	go l.saveInterval()
	return ctx, logger
}

func (p *logger) saveMessage() {
	p.bufferLock.Lock()
	defer p.bufferLock.Unlock()

	logContent := p.buffer.String()
	if logContent == "" {
		return
	}
	cm, err := p.ConfigMaps.GetNamespaced(p.Cluster.Name, configMapName, v12.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err := p.ConfigMaps.Create(&corev1.ConfigMap{
			ObjectMeta: v12.ObjectMeta{
				Name:      configMapName,
				Namespace: p.Cluster.Name,
			},
			Data: map[string]string{
				"log": logContent,
			},
		})
		log.Error("Failed to save provisioning log", "operation", "save_message", "config_map", configMapName, "error", err)
	} else if err != nil {
		log.Error("Failed to get provisioning log", "operation", "save_message", "config_map", configMapName, "error", err)
	} else if logContent != cm.Data["log"] {
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data["log"] = logContent
		_, err := p.ConfigMaps.Update(cm)
		if err != nil {
			log.Error("Failed to update provisioning log", "operation", "save_message", "config_map", configMapName, "error", err)
		}
	}
}

func (p *logger) saveInterval() {
	timer := time.NewTicker(2 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			p.saveMessage()
		case <-p.done:
			p.saveMessage()
			return
		}
	}
}

func (p *logger) logEvent(cluster *v3.Cluster, event logstream.LogEvent, cond condition.Cond) *v3.Cluster {
	p.bufferLock.Lock()
	defer p.bufferLock.Unlock()

	if event.Error {
		log.Error("Cluster provisioning", "operation", "log_event", "cluster", cluster.Name, "message", event.Message)
	} else {
		log.Info("Cluster provisioning", "operation", "log_event", "cluster", cluster.Name, "message", event.Message)
	}
	p.buffer.WriteString(time.Now().Format(time.RFC3339))
	p.buffer.WriteString(" ")
	if event.Error {
		p.buffer.WriteString("[ERROR] ")
	} else {
		p.buffer.WriteString("[INFO ] ")
	}
	p.buffer.WriteString(event.Message)
	p.buffer.WriteString("\n")
	return cluster
}

func (p *logger) getCtx(cluster *v3.Cluster, cond condition.Cond) (string, context.Context, io.Closer) {
	logger := logstream.NewLogStream()
	logID := logger.ID()
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(map[string]string{
		"log-id": logID,
	}))
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for event := range logger.Stream() {
			cluster = p.logEvent(cluster, event, cond)
		}
	}()

	return logID, ctx, closerFunc(func() error {
		logger.Close()
		wg.Wait()
		close(p.done)
		return nil
	})
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }
