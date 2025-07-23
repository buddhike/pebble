package consumer

import (
	"fmt"
	"time"

	etcdembed "go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

type EtcdServer struct {
	cfg              *PopConfig
	done             chan struct{}
	stop             chan struct{}
	startStopTimeout time.Duration
	logger           *zap.Logger
}

func NewEtcdServer(cfg *PopConfig, stop chan struct{}, logger *zap.Logger) *EtcdServer {
	return &EtcdServer{
		cfg:              cfg,
		done:             make(chan struct{}),
		stop:             stop,
		startStopTimeout: time.Second * time.Duration(cfg.EtcdStartTimeoutSeconds),
		logger:           logger.Named("etcdserver"),
	}
}

func (s *EtcdServer) Start() error {
	listenClientUrls, err := s.cfg.GetListenClientUrls()
	if err != nil {
		return err
	}

	advertiseClientUrls, err := s.cfg.GetAdvertiseClientUrls()
	if err != nil {
		return err
	}

	listenPeerUrls, err := s.cfg.GetListenPeerUrls()
	if err != nil {
		return err
	}

	advertisePeerUrls, err := s.cfg.GetAdvertisePeerUrls()
	if err != nil {
		return err
	}

	cfg := etcdembed.NewConfig()
	cfg.Name = s.cfg.GetEtcdPeerName()
	cfg.Dir = fmt.Sprintf("%s.etcd", cfg.Name)
	cfg.Logger = "zap"
	cfg.ListenClientUrls = listenClientUrls
	cfg.AdvertiseClientUrls = advertiseClientUrls
	cfg.ListenPeerUrls = listenPeerUrls
	cfg.AdvertisePeerUrls = advertisePeerUrls
	cfg.InitialCluster = s.cfg.GetInitialCluster()

	go func() {
		defer close(s.done)
		e, err := etcdembed.StartEtcd(cfg)
		if err != nil {
			s.logger.Error("embedded etcd server failed to start", zap.Error(err))
			return
		}
		select {
		case <-e.Server.ReadyNotify():
			s.logger.Info("embedded etcd server is ready")
		case <-time.After(s.startStopTimeout):
			s.logger.Error("embedded etcd server took too long to start")
		}

		<-s.stop
		s.logger.Info("shutting down etcd server")
		e.Close()
		s.logger.Info("etcdserver closed")

		select {
		case <-e.Server.StopNotify():
			s.logger.Info("embedded etcd server stopped")
		case <-time.After(s.startStopTimeout):
			s.logger.Error("embedded etcd server took too long to stop")
		}
	}()

	return nil
}
