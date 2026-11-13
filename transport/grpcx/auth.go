package grpcx

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/auth"
)

// Bearer returns server interceptors that verify the "authorization: Bearer
// <token>" metadata and put the principal in the context, exactly as
// auth.Bearer does for HTTP: the same usecases and auth.Require then work
// on both transports. Calls without a token pass through unauthenticated.
//
//	unary, stream := grpcx.Bearer(verify)
//	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream))
func Bearer[P any](verify auth.Verifier[P]) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	authenticate := func(ctx context.Context) (context.Context, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		var token string
		if vals := md.Get("authorization"); len(vals) > 0 {
			token, _ = strings.CutPrefix(vals[0], "Bearer ")
		}
		if token == "" {
			return ctx, nil
		}
		p, err := verify(ctx, token)
		if err != nil {
			if kakarot.CodeOf(err) == kakarot.CodeInternal {
				err = kakarot.E(kakarot.CodeUnauthorized, "invalid credentials", err)
			}
			return ctx, Status(err)
		}
		return auth.With(ctx, p), nil
	}
	unary := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		ctx, err := authenticate(ctx)
		if err != nil {
			return nil, err
		}
		return h(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		ctx, err := authenticate(ss.Context())
		if err != nil {
			return err
		}
		return h(srv, &authStream{ServerStream: ss, ctx: ctx})
	}
	return unary, stream
}

type authStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authStream) Context() context.Context { return s.ctx }

// WithBearer returns a context whose outgoing calls carry token.
func WithBearer(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}
