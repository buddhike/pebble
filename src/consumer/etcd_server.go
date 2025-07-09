package consumer

import (
	"fmt"
	"time"

	etcdembed "go.etcd.io/etcd/server/v3/embed"
)

type EtcdServer struct {
	cfg  *ConsumerConfig
	done chan struct{}
	stop chan struct{}
}

func NewEtcdServer(cfg *ConsumerConfig, stop chan struct{}) *EtcdServer {
	return &EtcdServer{
		cfg:  cfg,
		done: make(chan struct{}),
		stop: stop,
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

	e, err := etcdembed.StartEtcd(cfg)
	if err != nil {
		return err
	}

	go func() {
		select {
		case <-e.Server.ReadyNotify():
			fmt.Println("Embedded etcd server is ready!")
		case <-time.After(60 * time.Second):
			fmt.Println("Embedded etcd server took too long to start")
			close(s.done)
		}

		<-s.stop
		e.Server.Stop()

		select {
		case <-e.Server.StopNotify():
			fmt.Println("Embedded etcd server stopped")
		case <-time.After(60 * time.Second):
			fmt.Println("Embedded etcd server took too long to stop")
		}
		close(s.done)
	}()

	return nil
}
