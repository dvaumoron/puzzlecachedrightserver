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
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/dvaumoron/puzzlerightservice"
	"github.com/go-redis/redis/v8"
	"golang.org/x/sync/singleflight"
)

const trueIndicator = "T"
const falseIndicator = "F"
const accessIndicator = 'A'
const createIndicator = 'C'
const updateIndicator = 'U'
const deleteIndicator = 'D'

const cacheStorageMsg = "Failed to store in cache :"

type empty = struct{}

// cacheServer implement puzzlerightservice.RightServer
// it add a cache management and delegate to a CallingServer
type cacheServer struct {
	pb.UnimplementedRightServer
	inner       *callingServer
	rdb         *redis.Client
	dataTimeout time.Duration
	mutex       sync.RWMutex
	roleToUser  map[string]map[uint64]empty
	sf          singleflight.Group
}

func New(rightServiceAddr string, rdb *redis.Client, dataTimeout time.Duration) pb.RightServer {
	return &cacheServer{
		inner: newCalling(rightServiceAddr), rdb: rdb, dataTimeout: dataTimeout,
		roleToUser: map[string]map[uint64]empty{},
	}
}

func (s *cacheServer) storeRoleToUser(roleKey string, userId uint64) {
	s.mutex.RLock()
	_, exists := s.roleToUser[roleKey][userId]
	s.mutex.RUnlock()
	if !exists {
		s.mutex.Lock()
		s.roleToUser[roleKey][userId] = empty{}
		s.mutex.Unlock()
	}
}

func (s *cacheServer) updateWithDefaultTTL(ctx context.Context, id string) {
	err := s.rdb.Expire(ctx, id, s.dataTimeout)
	if err != nil {
		log.Println("Failed to set TTL :", err)
	}
}

func (s *cacheServer) AuthQuery(ctx context.Context, request *pb.RightRequest) (*pb.Response, error) {
	userId := request.UserId
	userKey := getUserKey(userId)
	actionKey := getActionKey(request.ObjectId, request.Action)
	var builder strings.Builder
	builder.WriteString(userKey)
	builder.WriteRune('/')
	builder.WriteString(actionKey)
	requestKey := builder.String()
	untyped, err, _ := s.sf.Do(requestKey, func() (interface{}, error) {
		cacheRes, err := s.rdb.HGet(ctx, userKey, actionKey).Result()
		if err == nil {
			s.updateWithDefaultTTL(ctx, userKey)
			return &pb.Response{Success: cacheRes == trueIndicator}, nil
		}

		response, err := s.inner.AuthQuery(ctx, request)
		if err == nil {
			// this call serves to keep roleToUser up to date,
			// when the user roles are not in cache
			go s.ListUserRoles(ctx, &pb.UserId{Id: userId})

			value := ""
			if response.Success {
				value = trueIndicator
			} else {
				value = falseIndicator
			}
			err2 := s.rdb.HSet(ctx, userKey, actionKey, value).Err()
			if err2 == nil {
				s.updateWithDefaultTTL(ctx, userKey)
			} else {
				log.Println(cacheStorageMsg, err2)
			}
		}
		return response, err
	})
	response, _ := untyped.(*pb.Response)
	return response, err
}

func (s *cacheServer) ListRoles(ctx context.Context, request *pb.ObjectIds) (*pb.Roles, error) {
	// no cache for this call (only admin should use)
	roles, err := s.inner.ListRoles(ctx, request)
	if err == nil {
		pipe := s.rdb.TxPipeline()
		for _, role := range roles.List {
			roleKey := getRoleKey(role.Name, role.ObjectId)
			actionsStr, _ := actionsFromCall(role.List)
			pipe.Set(ctx, roleKey, actionsStr, s.dataTimeout)
		}
		_, err2 := pipe.Exec(ctx)
		if err2 != nil {
			log.Println(cacheStorageMsg, err2)
		}
	}
	return roles, err
}

func (s *cacheServer) RoleRight(ctx context.Context, request *pb.RoleRequest) (*pb.Actions, error) {
	roleKey := getRoleKey(request.Name, request.ObjectId)
	untyped, err, _ := s.sf.Do(roleKey, func() (interface{}, error) {
		cacheRes, err := s.rdb.Get(ctx, roleKey).Result()
		if err == nil {
			s.updateWithDefaultTTL(ctx, roleKey)
			return &pb.Actions{List: actionsFromCache(cacheRes)}, nil
		}

		actions, err := s.inner.RoleRight(ctx, request)
		if err == nil {
			actionsStr, _ := actionsFromCall(actions.List)
			err2 := s.rdb.Set(ctx, roleKey, actionsStr, s.dataTimeout).Err()
			if err2 == nil {
				s.updateWithDefaultTTL(ctx, roleKey)
			} else {
				log.Println(cacheStorageMsg, err2)
			}
		}
		return actions, err
	})
	actions, _ := untyped.(*pb.Actions)
	return actions, err
}

func (s *cacheServer) UpdateUser(ctx context.Context, request *pb.UserRight) (*pb.Response, error) {
	response, err := s.inner.UpdateUser(ctx, request)
	if err == nil && response.Success {
		// invalidate corresponding key in cache
		err2 := s.rdb.Del(ctx, getUserKey(request.UserId)).Err()
		if err2 != nil {
			log.Println(cacheStorageMsg, err2)
		}
	}
	return response, err
}

func (s *cacheServer) UpdateRole(ctx context.Context, request *pb.Role) (*pb.Response, error) {
	response, err := s.inner.UpdateRole(ctx, request)
	if err == nil && response.Success {
		// invalidate corresponding key in cache
		roleKey := getRoleKey(request.Name, request.ObjectId)
		keys := []string{roleKey}
		s.mutex.RLock()
		for userId := range s.roleToUser[roleKey] {
			keys = append(keys, getUserKey(userId))
		}
		s.mutex.RUnlock()
		err2 := s.rdb.Del(ctx, keys...).Err()
		if err2 == nil {
			actionsStr, _ := actionsFromCall(request.List)
			err2 := s.rdb.Set(ctx, roleKey, actionsStr, s.dataTimeout).Err()
			if err2 == nil {
				s.updateWithDefaultTTL(ctx, roleKey)
			}
		}
		if err2 != nil {
			log.Println(cacheStorageMsg, err2)
		}
	}
	return response, err
}

func (s *cacheServer) ListUserRoles(ctx context.Context, request *pb.UserId) (*pb.Roles, error) {
	userId := request.Id
	userKey := getUserKey(userId)
	untyped, err, _ := s.sf.Do(userKey, func() (interface{}, error) {
		cacheRes, err := s.rdb.HGetAll(ctx, userKey).Result()
		if err == nil {
			s.updateWithDefaultTTL(ctx, userKey)

			list := []*pb.Role{}
			for roleKey, actions := range cacheRes {
				if roleKey[:5] == "role:" {
					splitted := strings.Split(roleKey[5:], "/")
					objectId, _ := strconv.ParseUint(splitted[1], 10, 64)
					list = append(list, &pb.Role{
						Name: splitted[0], ObjectId: objectId,
						List: actionsFromCache(actions),
					})
				}
			}
			return &pb.Roles{List: list}, nil
		}

		roles, err := s.inner.ListUserRoles(ctx, request)
		if err == nil {
			userData := map[string]any{}
			actionSetByObject := map[uint64]map[pb.RightAction]empty{}
			for _, role := range roles.List {
				objectId := role.ObjectId
				roleKey := getRoleKey(role.Name, objectId)
				s.storeRoleToUser(roleKey, userId)

				actionsStr, actionSet := actionsFromCall(role.List)
				userData[roleKey] = actionsStr

				globalActionSet := actionSetByObject[objectId]
				if globalActionSet == nil {
					globalActionSet = actionSet
				} else {
					for action := range actionSet {
						globalActionSet[action] = empty{}
					}
				}
				actionSetByObject[objectId] = globalActionSet
			}

			pipe := s.rdb.TxPipeline()
			// here userData contains only the roles
			for roleKey, actionsStr := range userData {
				pipe.Set(ctx, roleKey, actionsStr, s.dataTimeout)
			}

			actionKeyIndicators := map[string]string{}
			for objectId, actionSet := range actionSetByObject {
				for action := range pb.RightAction_name {
					actionKey := getActionKey(objectId, pb.RightAction(action))
					actionKeyIndicators[actionKey] = falseIndicator
				}
				for action := range actionSet {
					actionKey := getActionKey(objectId, action)
					actionKeyIndicators[actionKey] = trueIndicator
				}
			}

			for actionKey, indicator := range actionKeyIndicators {
				userData[actionKey] = indicator
			}

			pipe.Del(ctx, userKey)
			pipe.HSet(ctx, userKey, userData)
			if _, err2 := pipe.Exec(ctx); err2 == nil {
				s.updateWithDefaultTTL(ctx, userKey)
			} else {
				log.Println(cacheStorageMsg, err2)
			}
		}
		return roles, err
	})
	roles, _ := untyped.(*pb.Roles)
	return roles, err
}

func getUserKey(userId uint64) string {
	return fmt.Sprintf("user:%d", userId)
}

func getActionKey(objectId uint64, action pb.RightAction) string {
	return fmt.Sprintf("%d/%c", objectId, actionFromCall(action))
}

func actionFromCall(action pb.RightAction) byte {
	switch action {
	case pb.RightAction_ACCESS:
		return accessIndicator
	case pb.RightAction_CREATE:
		return createIndicator
	case pb.RightAction_UPDATE:
		return updateIndicator
	case pb.RightAction_DELETE:
		return deleteIndicator
	}
	return 0
}

func getRoleKey(roleName string, objectId uint64) string {
	return fmt.Sprintf("role:%v/%d", roleName, objectId)
}

func actionsFromCache(cacheRes string) []pb.RightAction {
	list := []pb.RightAction{}
	for _, actionChar := range cacheRes {
		switch actionChar {
		case accessIndicator:
			list = append(list, pb.RightAction_ACCESS)
		case createIndicator:
			list = append(list, pb.RightAction_CREATE)
		case updateIndicator:
			list = append(list, pb.RightAction_UPDATE)
		case deleteIndicator:
			list = append(list, pb.RightAction_DELETE)
		}
	}
	return list
}

func actionsFromCall(callRes []pb.RightAction) (string, map[pb.RightAction]empty) {
	buffer := make([]byte, 0, 4)
	actionSet := uniqueActions(callRes)
	for action := range actionSet {
		buffer = append(buffer, actionFromCall(action))
	}
	return string(buffer), actionSet
}

func uniqueActions(actions []pb.RightAction) map[pb.RightAction]empty {
	res := map[pb.RightAction]empty{}
	for _, action := range actions {
		res[action] = empty{}
	}
	return res
}
