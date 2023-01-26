/*
 *
 * Copyright 2022 puzzlecachedrightserver authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package cachedrightserver

import (
	"context"

	pb "github.com/dvaumoron/puzzlerightservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// callingServer implement puzzlerightservice.RightServer
// it call another server using gRPC
type callingServer struct {
	pb.UnimplementedRightServer
	rightServiceAddr string
}

func makeCallingServer(rightServiceAddr string) callingServer {
	return callingServer{rightServiceAddr: rightServiceAddr}
}

func (s *callingServer) AuthQuery(ctx context.Context, request *pb.RightRequest) (*pb.Response, error) {
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return pb.NewRightClient(conn).AuthQuery(ctx, request)
}

func (s *callingServer) ListRoles(ctx context.Context, request *pb.ObjectIds) (*pb.Roles, error) {
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return pb.NewRightClient(conn).ListRoles(ctx, request)
}

func (s *callingServer) RoleRight(ctx context.Context, request *pb.RoleRequest) (*pb.Actions, error) {
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return pb.NewRightClient(conn).RoleRight(ctx, request)
}

func (s *callingServer) UpdateUser(ctx context.Context, request *pb.UserRight) (*pb.Response, error) {
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return pb.NewRightClient(conn).UpdateUser(ctx, request)
}

func (s *callingServer) UpdateRole(ctx context.Context, request *pb.Role) (*pb.Response, error) {
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return pb.NewRightClient(conn).UpdateRole(ctx, request)
}

func (s *callingServer) ListUserRoles(ctx context.Context, request *pb.UserId) (*pb.Roles, error) {
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return pb.NewRightClient(conn).ListUserRoles(ctx, request)
}
