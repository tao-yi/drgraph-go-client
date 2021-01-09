package dgraph

import (
	"context"
	"log"
	"time"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/micro/go-micro/v2/logger"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding/gzip"
)

type CancelFunc func()

type Client struct {
	dgraphClient *dgo.Dgraph
	Cancel       CancelFunc
}

func NewClient(host string) *Client {
	const MaxReceiveMessageSize = 256 << 20

	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffLinear(100 * time.Millisecond)),
		grpc_retry.WithMax(3),
		grpc_retry.WithCodes(codes.Aborted, codes.Unavailable, codes.Unknown, codes.ResourceExhausted, codes.Internal, codes.DeadlineExceeded),
	}

	// Dial a gRPC connection. The address to dial to can be configured when
	// setting up the dgraph cluster.
	dialOpts := append([]grpc.DialOption{},
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name), grpc.MaxCallRecvMsgSize(MaxReceiveMessageSize)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)),
	)
	conn, err := grpc.Dial(host, dialOpts...)
	if err != nil {
		logger.Fatal("while trying to dial gRPC")
	}

	log.Printf("connected to dgraph at host: %s", host)

	dc := api.NewDgraphClient(conn)
	dg := dgo.NewDgraphClient(dc)

	return &Client{
		dgraphClient: dg,
		Cancel: func() {
			if err := conn.Close(); err != nil {
				log.Printf("Error while closing connection:%v", err)
			}
		},
	}
}

func CloseTxn(txn *dgo.Txn, ctx context.Context) {
	err := txn.Discard(ctx)
	if err != nil {
		log.Print(err.Error())
	}
}

func (c *Client) Mutate(mutation *api.Mutation) (*api.Response, error) {
	txn, ctx := c.dgraphClient.NewTxn(), context.Background()
	defer CloseTxn(txn, ctx)
	response, err := txn.Mutate(ctx, mutation)
	if err != nil {
		return nil, errors.Wrap(err, "failed to commit create cells transaction")
	}
	return response, nil
}

func (c *Client) QueryWithVars(q string, vars map[string]string) ([]byte, error) {
	txn, ctx := c.dgraphClient.NewReadOnlyTxn(), context.Background()
	defer CloseTxn(txn, ctx)
	response, err := txn.QueryWithVars(ctx, q, vars)
	if err != nil {
		return nil, err
	}

	return response.GetJson(), nil
}
