package broker

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/83codes/octar/internal/auth"
	"github.com/83codes/octar/internal/config"
	"github.com/83codes/octar/internal/db"
	"github.com/83codes/octar/internal/metrics"
	"github.com/83codes/octar/internal/queue"
	"github.com/83codes/octar/internal/scheduler"
	"github.com/83codes/octar/internal/server"
	stg "github.com/83codes/octar/internal/storage"
	"github.com/83codes/octar/internal/storage/recovery"
	"github.com/83codes/octar/internal/xtime"
)

func loadTLSConfig(cfg config.TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS cert: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

type Broker struct {
	Config    *config.Config
	DB        *db.Store
	Auth      *auth.Service
	Scheduler *scheduler.Scheduler
	WAL       *stg.WAL
	TCPServer *server.TCPServer

	registry         *subRegistry
	quota            *brokerQuota
	dispatchChs      []chan *queue.Message
	dispatchStop     chan struct{}
	dispatchOnce     sync.Once
	dispatchStopOnce sync.Once
	dispatchWG       sync.WaitGroup
	logger           *slog.Logger
	recoveryQueues   []recovery.QueueRecoveryInfo
	globalMsgs       atomic.Int64
}

func New(cfg *config.Config, store *db.Store, authSvc *auth.Service) (*Broker, error) {
	walDir := cfg.Storage.DataDir + "/wal"

	queueInfos, err := recovery.NewBootstrap(walDir).DiscoverQueues()
	if err != nil {
		slog.Default().Warn("failed to discover queues for recovery", "error", err)
	}

	wal, err := stg.NewWAL(walDir, stg.WALConfig{
		FlushInterval:    cfg.Storage.WAL.FlushInterval,
		FlushMaxMessages: cfg.Storage.WAL.FlushMaxMessages,
		SegmentMaxBytes:  cfg.Storage.WAL.SegmentMaxBytes,
		Sync:             cfg.Storage.WAL.Sync,
		SnapshotInterval: cfg.Storage.WAL.SnapshotInterval,
	})
	if err != nil {
		return nil, err
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}

	b := &Broker{
		Config:         cfg,
		DB:             store,
		Auth:           authSvc,
		Scheduler:      scheduler.NewScheduler(),
		WAL:            wal,
		registry:       newSubRegistry(),
		quota:          newBrokerQuota(cfg.Server.Inflight.GlobalMax),
		dispatchChs:    make([]chan *queue.Message, workers),
		dispatchStop:   make(chan struct{}),
		logger:         slog.Default().With("component", "broker"),
		recoveryQueues: queueInfos,
	}

	for i := range b.dispatchChs {
		b.dispatchChs[i] = make(chan *queue.Message, 1024)
	}

	tlsCfg, err := loadTLSConfig(cfg.Server.TLS)
	if err != nil {
		return nil, fmt.Errorf("server TLS: %w", err)
	}

	b.TCPServer = server.NewTCPServer(
		cfg.Server.Host,
		cfg.Server.Port,
		store,
		authSvc,
		b.handleConnection,
		cfg.Server.Inflight,
		int32(cfg.Server.MaxConnections),
		cfg.Server.ReadTimeout,
		cfg.Server.WriteTimeout,
		tlsCfg,
		cfg.Server.ConnRateLimit,
	)

	return b, nil
}

func (b *Broker) Start() error {
	if err := b.recoverQueues(); err != nil {
		b.logger.Warn("queue recovery failed", "error", err)
	}
	if err := b.TCPServer.Start(); err != nil {
		return err
	}
	b.startDispatchWorkers()
	b.startLeaseSweeper()
	b.startMetricsCollector()
	b.Scheduler.Run(b.enqueueDispatch)
	return nil
}

func (b *Broker) startLeaseSweeper() {
	b.dispatchWG.Add(1)
	go func() {
		defer b.dispatchWG.Done()
		defer func() {
			if r := recover(); r != nil {
				b.logger.Error("lease sweeper panicked", "panic", r)
			}
		}()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-b.dispatchStop:
				return
			case <-ticker.C:
				b.sweepLeases(xtime.Now())
			}
		}
	}()
}

func (b *Broker) sweepLeases(now time.Time) {
	for _, q := range b.Scheduler.ListQueues() {
		expired := q.SweepExpiredLeases(now)
		for _, lease := range expired {
			b.logger.Info("lease expired — returning message to pending",
				"namespace", lease.Namespace,
				"queue", lease.QueueName,
				"group", lease.GroupKey,
				"msgID", lease.MsgID,
			)
			_ = b.WAL.Append(stg.Event{
				Type:      stg.EventExpire,
				Namespace: lease.Namespace,
				Queue:     lease.QueueName,
				Group:     lease.GroupKey,
				MsgID:     lease.MsgID,
			})
			if activeQ := b.Scheduler.GetQueue(lease.Namespace, lease.QueueName); activeQ != nil {
				b.Scheduler.Activate(activeQ, lease.GroupKey)
			}
		}
	}
}

func (b *Broker) startMetricsCollector() {
	b.dispatchWG.Add(1)
	go func() {
		defer b.dispatchWG.Done()
		defer func() {
			if r := recover(); r != nil {
				b.logger.Error("metrics collector panicked", "panic", r)
			}
		}()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-b.dispatchStop:
				return
			case <-ticker.C:
				metrics.ActiveConnsGauge.Set(float64(b.TCPServer.ActiveConns()))
				metrics.ActiveMessagesGauge.Set(float64(b.GlobalMsgs()))
				b.WAL.VisitQueues(func(qw *stg.QueueWAL) {
					val := 0.0
					if qw.Err() != nil {
						val = 1.0
					}
					metrics.WALErrored.WithLabelValues(qw.Namespace, qw.Queue).Set(val)
				})
			}
		}
	}()
}

func (b *Broker) IncGlobalMsgs() bool {
	max := b.Config.Server.GlobalMaxMsgs
	if max <= 0 {
		b.globalMsgs.Add(1)
		return true
	}
	for {
		cur := b.globalMsgs.Load()
		if cur >= max {
			return false
		}
		if b.globalMsgs.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (b *Broker) DecGlobalMsgs() {
	b.globalMsgs.Add(-1)
}

func (b *Broker) GlobalMsgs() int64 {
	return b.globalMsgs.Load()
}

func (b *Broker) CheckDiskSpace() (bool, string) {
	const minFree = 500 << 20
	dataDir := b.Config.Storage.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var freeSpace uint64
	if err := freeDiskSpace(dataDir, &freeSpace); err != nil {
		return false, fmt.Sprintf("disk check error: %v", err)
	}
	if freeSpace < minFree {
		return false, fmt.Sprintf("low disk space: %d bytes free (min %d)", freeSpace, minFree)
	}
	return true, ""
}

func (b *Broker) TriggerSnapshot(namespace, queue string) {
	q := b.WAL.GetQueue(namespace, queue)
	if q == nil {
		return
	}
	if err := q.SaveSnapshot(); err != nil {
		b.logger.Error("manual snapshot failed",
			"namespace", namespace, "queue", queue, "error", err)
	}
}

func (b *Broker) Stop() error {
	b.dispatchStopOnce.Do(func() {
		close(b.dispatchStop)
		b.dispatchWG.Wait()
	})
	b.Scheduler.Stop()

	if err := b.TCPServer.Stop(); err != nil {
		b.logger.Error("tcp server stop error", "error", err)
	}

	if err := b.WAL.Close(); err != nil {
		b.logger.Error("wal close error", "error", err)
	}
	return nil
}
