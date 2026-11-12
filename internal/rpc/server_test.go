package rpc

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/clock"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/ids"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/lab"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/rpc/labpb"
)

func TestUnaryAndStream(t *testing.T) {
	clk := clock.New(time.Date(2026, 11, 12, 16, 0, 0, 0, time.UTC))
	service, err := lab.Wire(clk, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Close)
	token, err := service.Token(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	lis, gs, err := Listen(service)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := labpb.NewWorkClient(conn)
	_, err = client.Submit(context.Background(), &labpb.JobRequest{Id: "SYN-JOB-1", TenantId: ids.Tenant})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token %v", err)
	}
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)
	reply, err := client.Submit(ctx, &labpb.JobRequest{Id: "SYN-JOB-1", TenantId: ids.Tenant, Kind: "light", Payload: []byte("a")})
	if err != nil || reply.GetReason() != reason.Done {
		t.Fatalf("submit %+v %v", reply, err)
	}
	again, err := client.Submit(ctx, &labpb.JobRequest{Id: "SYN-JOB-1", TenantId: ids.Tenant, Kind: "light", Payload: []byte("a")})
	if err != nil || again.GetReason() != reason.Duplicate {
		t.Fatalf("duplicate %+v %v", again, err)
	}
	stream, err := client.Stream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"SYN-JOB-2", "SYN-JOB-3"} {
		if err := stream.Send(&labpb.JobRequest{Id: id, TenantId: ids.Tenant, Kind: "light", Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	got := 0
	for {
		item, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if item.GetReason() != reason.Done {
			t.Fatalf("stream %s", item.GetReason())
		}
		got++
	}
	if got != 2 {
		t.Fatalf("stream replies %d", got)
	}
}
