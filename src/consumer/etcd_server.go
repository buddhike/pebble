package consumer

import (
	"fmt"
	"time"

	etcdembed "go.etcd.io/etcd/server/v3/embed"
)

type EtcdServer struct {
	cfg              *ConsumerConfig
	done             chan struct{}
	stop             chan struct{}
	startStopTimeout time.Duration
}

func NewEtcdServer(cfg *ConsumerConfig, stop chan struct{}) *EtcdServer {
	return &EtcdServer{
		cfg:              cfg,
		done:             make(chan struct{}),
		stop:             stop,
		startStopTimeout: time.Second * time.Duration(cfg.EtcdStartTimeoutSeconds),
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
	cfg.LogOutputs = []string{}
	cfg.ListenClientUrls = listenClientUrls
	cfg.AdvertiseClientUrls = advertiseClientUrls
	cfg.ListenPeerUrls = listenPeerUrls
	cfg.AdvertisePeerUrls = advertisePeerUrls
	cfg.InitialCluster = s.cfg.GetInitialCluster()
	cfg.LogFormat = "console"

	go func() {
		defer close(s.done)
		e, err := etcdembed.StartEtcd(cfg)
		if err != nil {
			fmt.Println(err)
			return
		}
		select {
		case <-e.Server.ReadyNotify():
			fmt.Println("Embedded etcd server is ready!")
		case <-time.After(s.startStopTimeout):
			fmt.Println("Embedded etcd server took too long to start")
		}

		<-s.stop
		fmt.Println("Shutting down etcd server")
		e.Close()
		fmt.Println("EtcdServer closed")

		select {
		case <-e.Server.StopNotify():
			fmt.Println("Embedded etcd server stopped")
		case <-time.After(s.startStopTimeout):
			fmt.Println("Embedded etcd server took too long to stop")
		}
	}()

	return nil
}
