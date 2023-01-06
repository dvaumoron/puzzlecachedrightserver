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
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/dvaumoron/puzzlecachedrightserver/cachedrightserver"
	pb "github.com/dvaumoron/puzzlerightservice"
	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

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
	pb.RegisterRightServer(s, cachedrightserver.New(
		os.Getenv("RIGHT_SERVICE_ADDR"), rdb, dataTimeout,
	))
	log.Printf("Listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve : %v", err)
	}
}
