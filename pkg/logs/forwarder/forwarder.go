package forwarder

import (
	"fmt"
	"github.com/DataDog/datadog-agent/pkg/logs/auditor"
	"github.com/DataDog/datadog-agent/pkg/logs/client"
	"github.com/DataDog/datadog-agent/pkg/logs/config"
	"github.com/DataDog/datadog-agent/pkg/logs/diagnostic"
	"github.com/DataDog/datadog-agent/pkg/logs/message"
	"github.com/DataDog/datadog-agent/pkg/logs/pipeline"
	"github.com/DataDog/datadog-agent/pkg/logs/restart"
	"github.com/DataDog/datadog-agent/pkg/util/log"
)

const (
	DbQueryTrackID = "dbquery"
)

type EventPlatformForwarder interface {
	SendEventPlatformEvent(e *message.Message, track string) error
	Start()
	Stop()
}

type DefaultEventPlatformForwarder struct {
	providers       []pipeline.Provider
	trackChans      map[string]chan *message.Message
	destinationsCtx *client.DestinationsContext
}

func (s *DefaultEventPlatformForwarder) SendEventPlatformEvent(e *message.Message, track string) error {
	c, ok := s.trackChans[track]
	if !ok {
		return fmt.Errorf("track does not exist: %s", track)
	}
	c <- e
	return nil
}

func (s *DefaultEventPlatformForwarder) Start() {
	for _, p := range s.providers {
		p.Start()
	}
}

func (s *DefaultEventPlatformForwarder) Stop() {
	stopper := restart.NewParallelStopper()
	for _, p := range s.providers {
		stopper.Add(p)
	}
	stopper.Stop()
	// TODO: wait on stop and cancel context only after timeout like logs agent
	s.destinationsCtx.Stop()
}

func newDbQueryPipeline(destinationsContext *client.DestinationsContext) (pipeline.Provider, error) {
	configKeys := config.LogsConfigKeys{
		CompressionLevel:        "database_monitoring.compression_level",
		ConnectionResetInterval: "database_monitoring.connection_reset_interval",
		LogsDDURL:               "database_monitoring.logs_dd_url",
		DDURL:                   "database_monitoring.dd_url",
		DevModeNoSSL:            "database_monitoring.dev_mode_no_ssl",
		AdditionalEndpoints:     "database_monitoring.additional_endpoints",
		BatchWait:               "database_monitoring.batch_wait",
	}
	endpoints, err := config.BuildHTTPEndpointsWithConfig(configKeys, "dbquery-http-intake.logs.")
	if err != nil {
		return nil, err
	}

	provider := pipeline.NewProvider(1, auditor.NewNullAuditor(), &diagnostic.NoopMessageReceiver{}, nil, endpoints, destinationsContext)

	return provider, nil
}

func NewEventPlatformForwarder() EventPlatformForwarder {
	var pipelines []pipeline.Provider
	trackChans := make(map[string]chan *message.Message)

	destinationsCtx := client.NewDestinationsContext()
	destinationsCtx.Start()

	// dbquery
	p, err := newDbQueryPipeline(destinationsCtx)
	if err != nil {
		log.Errorf("Failed to initialize dbquery event pipeline: %s", err)
	} else {
		trackChans[DbQueryTrackID] = p.NextPipelineChan()
		pipelines = append(pipelines, p)
		log.Debugf("Initialized event platform forwarder pipeline. track=%s", DbQueryTrackID)
	}

	// dbmetrics
	// TODO

	return &DefaultEventPlatformForwarder{
		trackChans:      trackChans,
		destinationsCtx: destinationsCtx,
	}
}
