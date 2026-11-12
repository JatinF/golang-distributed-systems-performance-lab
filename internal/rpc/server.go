// Package rpc serves the lab Work service on 127.0.0.1.
package rpc

import (
	"context"
	"io"
	"net"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/identity"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/lab"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/model"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/rpc/labpb"
)

// Server is the gRPC Work implementation.
type Server struct {
	labpb.UnimplementedWorkServer
	lab *lab.Lab
}

// New returns a server for an existing lab.
func New(l *lab.Lab) *Server { return &Server{lab: l} }

// Listen starts a plaintext gRPC server on 127.0.0.1:0.
func Listen(l *lab.Lab) (net.Listener, *grpc.Server, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	gs := grpc.NewServer()
	labpb.RegisterWorkServer(gs, New(l))
	go func() { _ = gs.Serve(lis) }()
	return lis, gs, nil
}

func (s *Server) Submit(ctx context.Context, req *labpb.JobRequest) (*labpb.JobReply, error) {
	out := s.lab.Handle(ctx, jobFrom(ctx, req))
	if code := Code(out.Reason); code != codes.OK {
		return nil, status.Error(code, out.Reason)
	}
	return reply(out), nil
}

func (s *Server) Stream(stream grpc.BidiStreamingServer[labpb.JobRequest, labpb.JobReply]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		out := s.lab.Handle(stream.Context(), jobFrom(stream.Context(), req))
		if err := stream.Send(reply(out)); err != nil {
			return err
		}
		if code := Code(out.Reason); code == codes.Unauthenticated || code == codes.InvalidArgument {
			return status.Error(code, out.Reason)
		}
	}
}

func jobFrom(ctx context.Context, req *labpb.JobRequest) model.Job {
	job := model.Job{Token: tokenFrom(ctx)}
	if req != nil {
		job.ID = req.GetId()
		job.TenantID = req.GetTenantId()
		job.Kind = req.GetKind()
		job.Payload = req.GetPayload()
	}
	return job
}

func tokenFrom(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return ""
	}
	return identity.Bearer(values[0])
}

func reply(out model.Outcome) *labpb.JobReply {
	return &labpb.JobReply{Id: out.ID, Accepted: out.Accepted, State: out.State, Reason: out.Reason}
}

// Code maps a lab reason onto a gRPC status code.
func Code(name string) codes.Code {
	switch name {
	case reason.Done, reason.Duplicate, reason.CacheHit, reason.Replayed, reason.Retried:
		return codes.OK
	case reason.Backpressure:
		return codes.ResourceExhausted
	case reason.Cancelled:
		return codes.Canceled
	case reason.DeadlineExceeded:
		return codes.DeadlineExceeded
	case reason.Unauthenticated, reason.TokenExpired, reason.TokenInvalid, reason.SignatureInvalid, reason.AudienceMismatch:
		return codes.Unauthenticated
	case reason.ShuttingDown, reason.StoreUnavailable, reason.MessagingUnavailable, reason.CacheUnavailable:
		return codes.Unavailable
	case reason.Rejected:
		return codes.InvalidArgument
	default:
		return codes.Unknown
	}
}

// Bearer is accepted with any capitalization of the scheme.
func HasBearer(header string) bool { return strings.TrimSpace(header) != "" }
