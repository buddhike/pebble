package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

var (
	name                    string
	streamName              string
	id                      int
	popUrls                 string
	popListenAddress        string
	etcdPeerUrls            string
	etcdPeerListenAddress   string
	etcdClientUrls          string
	etcdClientListenAddress string
)

func init() {
	flag.StringVar(&name, "name", "pop", "pop name")
	flag.StringVar(&streamName, "stream-name", "", "stream name")
	flag.IntVar(&id, "id", 1, "pop id")
	flag.StringVar(&popUrls, "pop-urls", "http://localhost:13001", "pop urls")
	flag.StringVar(&popListenAddress, "pop-listen-address", "localhost", "pop listen address")
	flag.StringVar(&etcdPeerUrls, "etcd-peer-urls", "http://localhost:12001", "etcd peer urls")
	flag.StringVar(&etcdPeerListenAddress, "etcd-peer-listen-address", "localhost", "etcd peer listen address")
	flag.StringVar(&etcdClientUrls, "etcd-client-urls", "http://localhost:11001", "etcd client urls")
	flag.StringVar(&etcdClientListenAddress, "etcd-client-listen-address", "localhost", "etcd client listen address")
}

func main() {
	parseArgs()
	pop := NewPop(name, streamName, WithID(id), WithPopUrls(popUrls), WithPopListenAddress(popListenAddress), WithEtcdPeerUrls(etcdPeerUrls), WithEtcdListenPeerAddress(etcdPeerListenAddress), WithEtcdClientUrls(etcdClientUrls), WithEtcdListenClientAddress(etcdClientListenAddress))
	stopchan := make(chan os.Signal, 1)
	signal.Notify(stopchan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-stopchan
		pop.Stop()
	}()
	go pop.Start()
	<-pop.Done()
}

func parseArgs() {
	flag.Parse()
	if name == "" {
		fmt.Println("error: --name is required")
		flag.Usage()
		os.Exit(1)
	}
	if streamName == "" {
		fmt.Println("error: --stream-name is required")
		flag.Usage()
		os.Exit(1)
	}
	if popUrls == "" {
		fmt.Println("error: --pop-urls is required")
		flag.Usage()
		os.Exit(1)
	}
	if etcdPeerUrls == "" {
		fmt.Println("error: --etcd-peer-urls is required")
		flag.Usage()
		os.Exit(1)
	}
	if etcdClientUrls == "" {
		fmt.Println("error: --etcd-client-urls is required")
		flag.Usage()
		os.Exit(1)
	}
}
