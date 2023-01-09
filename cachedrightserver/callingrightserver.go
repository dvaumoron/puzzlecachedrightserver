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
	"time"

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

func newCalling(rightServiceAddr string) *callingServer {
	return &callingServer{rightServiceAddr: rightServiceAddr}
}

func (s *callingServer) AuthQuery(ctx context.Context, request *pb.RightRequest) (*pb.Response, error) {
	var response *pb.Response
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		response, err = pb.NewRightClient(conn).AuthQuery(ctx, request)
	}
	return response, err
}

func (s *callingServer) ListRoles(ctx context.Context, request *pb.ObjectIds) (*pb.Roles, error) {
	var response *pb.Roles
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		response, err = pb.NewRightClient(conn).ListRoles(ctx, request)
	}
	return response, err
}

func (s *callingServer) RoleRight(ctx context.Context, request *pb.RoleRequest) (*pb.Actions, error) {
	var actions *pb.Actions
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		actions, err = pb.NewRightClient(conn).RoleRight(ctx, request)
	}
	return actions, err
}

func (s *callingServer) UpdateUser(ctx context.Context, request *pb.UserRight) (*pb.Response, error) {
	var response *pb.Response
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		response, err = pb.NewRightClient(conn).UpdateUser(ctx, request)
	}
	return response, err
}

func (s *callingServer) UpdateRole(ctx context.Context, request *pb.Role) (*pb.Response, error) {
	var response *pb.Response
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		response, err = pb.NewRightClient(conn).UpdateRole(ctx, request)
	}
	return response, err
}

func (s *callingServer) ListUserRoles(ctx context.Context, request *pb.UserId) (*pb.Roles, error) {
	var roles *pb.Roles
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		roles, err = pb.NewRightClient(conn).ListUserRoles(ctx, request)
	}
	return roles, err
}
