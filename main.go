package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	etcdembed "go.etcd.io/etcd/server/v3/embed"
)

var (
	managerName          string
	stream               string
	streamConsumerArn    string
	managerPort          int
	advertisedClientURLs string
	listenClientURLs     string
	listenPeerURLs       string
	advertisedPeerURLs   string
	initialCluster       string
	clientConnectionURLs string
)

func init() {
	flag.StringVar(&managerName, "manager-name", "", "")
	flag.IntVar(&managerPort, "manager-port", 9000, "TCP/IP port that manager service binds to. Workers communicate with the manager via this port.")
	flag.StringVar(&stream, "stream", "", "Name of KDS stream")
	flag.StringVar(&streamConsumerArn, "stream-consumer-arn", "", "EFO consumer ARN")
	flag.StringVar(&listenClientURLs, "listen-client-urls", "", "")
	flag.StringVar(&advertisedClientURLs, "advertised-client-urls", "", "")
	flag.StringVar(&listenPeerURLs, "listen-peer-urls", "", "")
	flag.StringVar(&advertisedPeerURLs, "advertised-peer-urls", "", "")
	flag.StringVar(&initialCluster, "initial-cluster", "", "")
	flag.StringVar(&clientConnectionURLs, "client-connection-urls", "", "")
}

func main() {
	flag.Parse()

	e := startEtcd()
	defer e.Close()

	workerID := uuid.NewString()

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}

	kds := kinesis.NewFromConfig(cfg)
	// Create etcd client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(clientConnectionURLs, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("failed to create etcd client: %v", err)
	}

	mgr := NewManagerService(managerPort, stream, streamConsumerArn, kds, cli)
	go mgr.Start()

	worker := NewWorker(workerID, managerPort, streamConsumerArn, kds)
	go worker.Start()

	// Start leader election
	go electLeader(mgr)

	// Block forever
	select {}
}

func startEtcd() *etcdembed.Etcd {
	cfg := etcdembed.NewConfig()
	cfg.Name = managerName
	cfg.Dir = "default.etcd"
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"stderr"}
	cfg.ListenClientUrls = parsedURLs(listenClientURLs)
	cfg.AdvertiseClientUrls = parsedURLs(advertisedClientURLs)
	cfg.ListenPeerUrls = parsedURLs(listenPeerURLs)
	cfg.AdvertisePeerUrls = parsedURLs(advertisedPeerURLs)
	cfg.InitialCluster = initialCluster

	e, err := etcdembed.StartEtcd(cfg)
	if err != nil {
		log.Fatalf("failed to start embedded etcd: %v", err)
	}

	select {
	case <-e.Server.ReadyNotify():
		fmt.Println("Embedded etcd server is ready!")
	case <-time.After(60 * time.Second):
		log.Fatalf("etcd server took too long to start")
	}
	return e
}

func parsedURLs(from string) []url.URL {
	var l []url.URL
	for _, s := range strings.Split(from, ",") {
		p, err := url.Parse(s)
		if err != nil {
			panic(err)
		}
		l = append(l, *p)
	}
	return l
}

func electLeader(mgr *ManagerService) {
	// Create etcd client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(clientConnectionURLs, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("failed to create etcd client: %v", err)
	}
	defer cli.Close()
	for {
		// Create a session for the election
		session, err := concurrency.NewSession(cli)
		if err != nil {
			log.Fatalf("failed to create session: %v", err)
		}

		// Generate a unique node ID (using hostname and PID)
		hostname, _ := os.Hostname()
		nodeID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

		// Create an election
		election := concurrency.NewElection(session, "/leader-election")

		log.Printf("Node %s attempting to become leader...", nodeID)

		// Campaign to become leader
		ctx := context.Background()
		if err := election.Campaign(ctx, nodeID); err != nil {
			log.Fatalf("failed to campaign for leadership: %v", err)
		}

		log.Printf("Node %s successfully became leader!", nodeID)

		// Perform leader work
		log.Printf("Node %s is performing leader work...", nodeID)
		mgr.SetInService(true)

		// Observe leadership changes (optional - for monitoring)
		observeCh := election.Observe(ctx)
		go func() {
			for {
				select {
				case resp := <-observeCh:
					if len(resp.Kvs) > 0 {
						currentLeader := string(resp.Kvs[0].Value)
						log.Printf("Current leader is: %s", currentLeader)
					} else {
						log.Printf("No current leader")
					}
				case <-session.Done():
					log.Printf("Session ended, node %s lost leadership", nodeID)
					return
				}
			}
		}()

		// Keep the leader alive until session ends
		<-session.Done()
		mgr.SetInService(false)
		log.Printf("Node %s is no longer the leader", nodeID)
		session.Close()
	}
}
