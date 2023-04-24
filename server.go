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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dvaumoron/puzzlecachedrightserver/cachedrightserver"
	grpcserver "github.com/dvaumoron/puzzlegrpcserver"
	redisclient "github.com/dvaumoron/puzzleredisclient"
	pb "github.com/dvaumoron/puzzlerightservice"
)

func main() {
	// should start with this, to benefit from the call to godotenv
	s := grpcserver.Make()

	dataTimeoutSec, err := strconv.ParseInt(os.Getenv("UNUSED_DATA_TIMEOUT"), 10, 64)
	if err != nil {
		s.Logger.Fatal("Failed to parse UNUSED_DATA_TIMEOUT")
	}
	dataTimeout := time.Duration(dataTimeoutSec) * time.Second

	debug := strings.TrimSpace(os.Getenv("DEBUG_MODE")) != ""

	rdb := redisclient.Create(s.Logger)

	pb.RegisterRightServer(s, cachedrightserver.New(
		os.Getenv("RIGHT_SERVICE_ADDR"), rdb, dataTimeout, s.Logger, debug,
	))

	s.Start()
}
