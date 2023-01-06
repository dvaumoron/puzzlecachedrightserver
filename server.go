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
package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	pb "github.com/dvaumoron/puzzlerightservice"
	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// server is used to implement puzzlerightservice.RightServer.
type server struct {
	pb.UnimplementedRightServer
	rightServiceAddr string
	rdb              *redis.Client
	dataTimeout      time.Duration
}

func (s *server) AuthQuery(ctx context.Context, request *pb.RightRequest) (*pb.Response, error) {
	var response *pb.Response
	// TODO check cache
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		response, err = pb.NewRightClient(conn).AuthQuery(ctx, request)
	}
	return response, err
}

func (s *server) ListRoles(ctx context.Context, request *pb.ObjectIds) (*pb.Roles, error) {
	var response *pb.Roles
	// TODO check cache
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		response, err = pb.NewRightClient(conn).ListRoles(ctx, request)
	}
	return response, err
}

func (s *server) RoleRight(ctx context.Context, request *pb.RoleRequest) (*pb.Actions, error) {
	var actions *pb.Actions
	// TODO check cache
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		actions, err = pb.NewRightClient(conn).RoleRight(ctx, request)
	}
	return actions, err
}

func (s *server) UpdateUser(ctx context.Context, request *pb.UserRight) (*pb.Response, error) {
	var response *pb.Response
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		response, err = pb.NewRightClient(conn).UpdateUser(ctx, request)
		// TODO invalidate cache
	}
	return response, err
}

func (s *server) UpdateRole(ctx context.Context, request *pb.Role) (*pb.Response, error) {
	var response *pb.Response
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		response, err = pb.NewRightClient(conn).UpdateRole(ctx, request)
		// TODO invalidate cache
	}
	return response, err
}

func (s *server) ListUserRoles(ctx context.Context, request *pb.UserId) (*pb.Roles, error) {
	var roles *pb.Roles
	// TODO check cache
	conn, err := grpc.Dial(s.rightServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		roles, err = pb.NewRightClient(conn).ListUserRoles(ctx, request)
	}
	return roles, err
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Failed to load .env file")
	}

	dataTimeoutSec, err := strconv.ParseInt(os.Getenv("UNUSED_DATA_TIMEOUT"), 10, 64)
	if err != nil {
		log.Fatal("Failed to parse UNUSED_DATA_TIMEOUT")
	}
	dataTimeout := time.Duration(dataTimeoutSec) * time.Second

	dbNum, err := strconv.Atoi(os.Getenv("REDIS_SERVER_DB"))
	if err != nil {
		log.Fatal("Failed to parse REDIS_SERVER_DB")
	}

	lis, err := net.Listen("tcp", ":"+os.Getenv("SERVICE_PORT"))
	if err != nil {
		log.Fatalf("Failed to listen : %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_SERVER_ADDR"),
		Username: os.Getenv("REDIS_SERVER_USERNAME"),
		Password: os.Getenv("REDIS_SERVER_PASSWORD"),
		DB:       dbNum,
	})

	s := grpc.NewServer()
	pb.RegisterRightServer(s, &server{
		rightServiceAddr: os.Getenv("RIGHT_SERVICE_ADDR"), rdb: rdb, dataTimeout: dataTimeout},
	)
	log.Printf("Listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve : %v", err)
	}
}
