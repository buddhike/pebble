package consumer

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type KVS interface {
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	Txn(ctx context.Context) clientv3.Txn
}
