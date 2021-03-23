package forwarder

import (
	"fmt"
	"github.com/DataDog/datadog-agent/pkg/logs/auditor"
	"github.com/DataDog/datadog-agent/pkg/logs/client"
	"github.com/DataDog/datadog-agent/pkg/logs/client/http"
	"github.com/DataDog/datadog-agent/pkg/logs/config"
	"github.com/DataDog/datadog-agent/pkg/logs/message"
	"github.com/DataDog/datadog-agent/pkg/logs/restart"
	"github.com/DataDog/datadog-agent/pkg/logs/sender"
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
	pipelines       map[string]*PassthroughPipeline
	destinationsCtx *client.DestinationsContext
}

func (s *DefaultEventPlatformForwarder) SendEventPlatformEvent(e *message.Message, track string) error {
	p, ok := s.pipelines[track]
	if !ok {
		return fmt.Errorf("track does not exist: %s", track)
	}
	// TODO: should this be non-blocking write to avoid blocking the whole aggregator?
	p.in <- e
	return nil
}

func (s *DefaultEventPlatformForwarder) Start() {
	s.destinationsCtx.Start()
	for _, p := range s.pipelines {
		p.Start()
	}
}

func (s *DefaultEventPlatformForwarder) Stop() {
	stopper := restart.NewParallelStopper()
	for _, p := range s.pipelines {
		stopper.Add(p)
	}
	stopper.Stop()
	// TODO: wait on stop and cancel context only after timeout like logs agent
	s.destinationsCtx.Stop()
}

type PassthroughPipeline struct {
	// TODO: do we need to parallelize sending? If a single agent has some massive number of checks is this necessary?
	sender  *sender.Sender
	in      chan *message.Message
	auditor auditor.Auditor
}

func NewHTTPPassthroughPipeline(endpoints *config.Endpoints, destinationsContext *client.DestinationsContext, streaming bool) (p *PassthroughPipeline, err error) {
	if !endpoints.UseHTTP {
		return p, fmt.Errorf("endpoints must be http")
	}
	main := http.NewDestination(endpoints.Main, http.JSONContentType, destinationsContext)
	additionals := []client.Destination{}
	for _, endpoint := range endpoints.Additionals {
		additionals = append(additionals, http.NewDestination(endpoint, http.JSONContentType, destinationsContext))
	}
	destinations := client.NewDestinations(main, additionals)
	inputChan := make(chan *message.Message, config.ChanSize)
	var strategy sender.Strategy
	if streaming {
		strategy = sender.NewBatchStrategy(sender.ArraySerializer, endpoints.BatchWait)
	} else {
		strategy = sender.StreamStrategy
	}
	a := auditor.NewNullAuditor()
	return &PassthroughPipeline{
		sender:  sender.NewSender(inputChan, a.Channel(), destinations, strategy),
		in:      inputChan,
		auditor: a,
	}, nil
}

func (p *PassthroughPipeline) Start() {
	p.auditor.Start()
	p.sender.Start()
}


func (p *PassthroughPipeline) Stop() {
	p.sender.Stop()
	p.auditor.Stop()
}

func newDbQueryPipeline(destinationsContext *client.DestinationsContext, streaming bool) (*PassthroughPipeline, error) {
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
	return NewHTTPPassthroughPipeline(endpoints, destinationsContext, streaming)
}

func NewEventPlatformForwarder(streaming bool) EventPlatformForwarder {
	destinationsCtx := client.NewDestinationsContext()
	destinationsCtx.Start()
	pipelines := make(map[string]*PassthroughPipeline)

	// dbquery
	p, err := newDbQueryPipeline(destinationsCtx, streaming)
	if err != nil {
		log.Errorf("Failed to initialize dbquery event pipeline: %s", err)
	} else {
		pipelines[DbQueryTrackID] = p
		log.Debugf("Initialized event platform forwarder pipeline. track=%s", DbQueryTrackID)
	}

	// dbmetrics
	// TODO

	return &DefaultEventPlatformForwarder{
		pipelines:      pipelines,
		destinationsCtx: destinationsCtx,
	}
}
