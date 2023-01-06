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
	"github.com/go-redis/redis/v8"
)

// CacheServer implement puzzlerightservice.RightServer
// it add a cache management and delegate to a CallingServer
type CacheServer struct {
	pb.UnimplementedRightServer
	inner       pb.RightServer
	rdb         *redis.Client
	dataTimeout time.Duration
}

func New(rightServiceAddr string, rdb *redis.Client, dataTimeout time.Duration) *CacheServer {
	return &CacheServer{inner: NewCalling(rightServiceAddr), rdb: rdb, dataTimeout: dataTimeout}
}

func (s *CacheServer) AuthQuery(ctx context.Context, request *pb.RightRequest) (*pb.Response, error) {
	var response *pb.Response
	var err error
	// TODO check cache
	response, err = s.inner.AuthQuery(ctx, request)
	return response, err
}

func (s *CacheServer) ListRoles(ctx context.Context, request *pb.ObjectIds) (*pb.Roles, error) {
	var response *pb.Roles
	var err error
	// TODO check cache
	response, err = s.inner.ListRoles(ctx, request)
	return response, err
}

func (s *CacheServer) RoleRight(ctx context.Context, request *pb.RoleRequest) (*pb.Actions, error) {
	var actions *pb.Actions
	var err error
	// TODO check cache
	actions, err = s.inner.RoleRight(ctx, request)
	return actions, err
}

func (s *CacheServer) UpdateUser(ctx context.Context, request *pb.UserRight) (*pb.Response, error) {
	response, err := s.inner.UpdateUser(ctx, request)
	// TODO invalidate cache
	return response, err
}

func (s *CacheServer) UpdateRole(ctx context.Context, request *pb.Role) (*pb.Response, error) {
	response, err := s.inner.UpdateRole(ctx, request)
	// TODO invalidate cache
	return response, err
}

func (s *CacheServer) ListUserRoles(ctx context.Context, request *pb.UserId) (*pb.Roles, error) {
	var roles *pb.Roles
	var err error
	// TODO check cache
	roles, err = s.inner.ListUserRoles(ctx, request)
	return roles, err
}
