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
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/dvaumoron/puzzlerightservice"
	"github.com/redis/go-redis/v9"
	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

const CachedKey = "puzzleCachedRight"

const trueIndicator = "T"
const falseIndicator = "F"
const accessIndicator = 'A'
const createIndicator = 'C'
const updateIndicator = 'U'
const deleteIndicator = 'D'

const rightCallMsg = "Failed to call right service"
const cacheStorageMsg = "Failed to store in cache"

var errInternal = errors.New("internal service error")

type empty = struct{}

// cacheServer implement puzzlerightservice.RightServer
// it add a cache management and delegate to a callingServer
type cacheServer struct {
	callingServer
	rdb         *redis.Client
	dataTimeout time.Duration
	ttlUpdater  func(*redis.Client, otelzap.LoggerWithCtx, string, time.Duration)
	mutex       sync.RWMutex
	roleToUser  map[string]map[uint64]empty
	sf          singleflight.Group
	roleUpdater func(*cacheServer, otelzap.LoggerWithCtx, *pb.Roles, time.Duration)
	userUpdater func(*cacheServer, otelzap.LoggerWithCtx, string, map[string]any, map[string]string)
	logger      *otelzap.Logger
}

func New(rightServiceAddr string, rdb *redis.Client, dataTimeout time.Duration, logger *otelzap.Logger, tp trace.TracerProvider, debug bool) pb.RightServer {
	rolesUpdater := updateRolesTx
	userUpdater := updateUserTx
	if debug {
		ctx, initSpan := tp.Tracer(CachedKey).Start(context.Background(), "initialization")
		defer initSpan.End()

		logger.InfoContext(ctx, "Mode debug on")
		rolesUpdater = updateRoles
		userUpdater = updateUser
	}
	ttlUpdater := updateWithTTL
	if dataTimeout <= 0 {
		dataTimeout = 0
		ttlUpdater = noTTLUpdate
	}
	return &cacheServer{
		callingServer: makeCallingServer(rightServiceAddr), rdb: rdb, dataTimeout: dataTimeout,
		ttlUpdater: ttlUpdater, roleToUser: map[string]map[uint64]empty{}, roleUpdater: rolesUpdater,
		userUpdater: userUpdater, logger: logger,
	}
}

func (s *cacheServer) storeRoleToUser(roleKey string, userId uint64) {
	s.mutex.RLock()
	_, exists := s.roleToUser[roleKey][userId]
	s.mutex.RUnlock()
	if !exists {
		s.mutex.Lock()
		idSet := s.roleToUser[roleKey]
		if idSet == nil {
			idSet = map[uint64]empty{}
			s.roleToUser[roleKey] = idSet
		}
		idSet[userId] = empty{}
		s.mutex.Unlock()
	}
}

func updateWithTTL(rdb *redis.Client, logger otelzap.LoggerWithCtx, id string, dataTimeout time.Duration) {
	if err := rdb.Expire(logger.Context(), id, dataTimeout).Err(); err != nil {
		logger.Info("Failed to set TTL", zap.Error(err))
	}
}

func noTTLUpdate(rdb *redis.Client, logger otelzap.LoggerWithCtx, id string, dataTimeout time.Duration) {
}

func (s *cacheServer) updateWithTTL(logger otelzap.LoggerWithCtx, id string) {
	s.ttlUpdater(s.rdb, logger, id, s.dataTimeout)
}

func (s *cacheServer) AuthQuery(ctx context.Context, request *pb.RightRequest) (*pb.Response, error) {
	logger := s.logger.Ctx(ctx)
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
			s.updateWithTTL(logger, userKey)
			return &pb.Response{Success: cacheRes == trueIndicator}, nil
		}
		logCacheAccessError(logger, err)

		response, err := s.callingServer.AuthQuery(ctx, request)
		if err != nil {
			logger.Error(rightCallMsg, zap.Error(err))
			return nil, errInternal
		}

		value := ""
		if response.Success {
			value = trueIndicator
		} else {
			value = falseIndicator
		}
		err2 := s.rdb.HSet(ctx, userKey, actionKey, value).Err()
		if err2 == nil {
			s.updateWithTTL(logger, userKey)
		} else {
			logger.Error(cacheStorageMsg, zap.Error(err2))
		}
		return response, nil
	})
	response, _ := untyped.(*pb.Response)
	return response, err
}

func (s *cacheServer) ListRoles(ctx context.Context, request *pb.ObjectIds) (*pb.Roles, error) {
	logger := s.logger.Ctx(ctx)
	// no cache for this call (only admin should use)
	roles, err := s.callingServer.ListRoles(ctx, request)
	if err != nil {
		logger.Error(rightCallMsg, zap.Error(err))
		return nil, errInternal
	}
	s.roleUpdater(s, logger, roles, s.dataTimeout)
	return roles, nil
}

func (s *cacheServer) RoleRight(ctx context.Context, request *pb.RoleRequest) (*pb.Actions, error) {
	logger := s.logger.Ctx(ctx)
	roleKey := getRoleKey(request.Name, request.ObjectId)
	untyped, err, _ := s.sf.Do(roleKey, func() (interface{}, error) {
		cacheRes, err := s.rdb.Get(ctx, roleKey).Result()
		if err == nil {
			s.updateWithTTL(logger, roleKey)
			return &pb.Actions{List: actionsFromCache(cacheRes)}, nil
		}
		logCacheAccessError(logger, err)

		actions, err := s.callingServer.RoleRight(ctx, request)
		if err != nil {
			logger.Error(rightCallMsg, zap.Error(err))
			return nil, errInternal
		}

		actionsStr, _ := actionsFromCall(actions.List)
		err2 := s.rdb.Set(ctx, roleKey, actionsStr, s.dataTimeout).Err()
		if err2 == nil {
			s.updateWithTTL(logger, roleKey)
		} else {
			logger.Error(cacheStorageMsg, zap.Error(err2))
		}
		return actions, err
	})
	actions, _ := untyped.(*pb.Actions)
	return actions, err
}

func (s *cacheServer) UpdateUser(ctx context.Context, request *pb.UserRight) (*pb.Response, error) {
	logger := s.logger.Ctx(ctx)
	response, err := s.callingServer.UpdateUser(ctx, request)
	if err != nil {
		logger.Error(rightCallMsg, zap.Error(err))
		return nil, errInternal
	}
	if response.Success {
		// invalidate corresponding key in cache
		err2 := s.rdb.Del(ctx, getUserKey(request.UserId)).Err()
		if err2 != nil {
			logger.Error(cacheStorageMsg, zap.Error(err2))
		}
	}
	return response, nil
}

func (s *cacheServer) UpdateRole(ctx context.Context, request *pb.Role) (*pb.Response, error) {
	logger := s.logger.Ctx(ctx)
	response, err := s.callingServer.UpdateRole(ctx, request)
	if err != nil {
		logger.Error(rightCallMsg, zap.Error(err))
		return nil, errInternal
	}
	if response.Success {
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
				s.updateWithTTL(logger, roleKey)
			}
		}
		if err2 != nil {
			logger.Error(cacheStorageMsg, zap.Error(err2))
		}
	}
	return response, nil
}

func (s *cacheServer) ListUserRoles(ctx context.Context, request *pb.UserId) (*pb.Roles, error) {
	logger := s.logger.Ctx(ctx)
	userId := request.Id
	userKey := getUserKey(userId)
	untyped, err, _ := s.sf.Do(userKey, func() (interface{}, error) {
		cacheRes, err := s.rdb.HGetAll(ctx, userKey).Result()
		if err == nil {
			s.updateWithTTL(logger, userKey)

			list := []*pb.Role{}
			for roleKey, actions := range cacheRes {
				if len(roleKey) >= 5 && roleKey[:5] == "role:" {
					splitted := strings.Split(roleKey[5:], "/")
					objectId, _ := strconv.ParseUint(splitted[1], 10, 64)
					list = append(list, &pb.Role{
						Name: splitted[0], ObjectId: objectId,
						List: actionsFromCache(actions),
					})
				}
			}
			if len(list) != 0 {
				return &pb.Roles{List: list}, nil
			}
		} else {
			logCacheAccessError(logger, err)
		}

		roles, err := s.callingServer.ListUserRoles(ctx, request)
		if err != nil {
			logger.Error(rightCallMsg, zap.Error(err))
			return nil, errInternal
		}

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

		s.userUpdater(s, logger, userKey, userData, actionKeyIndicators)
		return roles, nil
	})
	roles, _ := untyped.(*pb.Roles)
	return roles, err
}

func getUserKey(userId uint64) string {
	return "user:" + strconv.FormatUint(userId, 10)
}

func getActionKey(objectId uint64, action pb.RightAction) string {
	var keyBuilder strings.Builder
	keyBuilder.WriteString(strconv.FormatUint(objectId, 10))
	keyBuilder.WriteByte('/')
	keyBuilder.WriteByte(actionFromCall(action))
	return keyBuilder.String()
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
	var keyBuilder strings.Builder
	keyBuilder.WriteString("role:")
	keyBuilder.WriteString(roleName)
	keyBuilder.WriteByte('/')
	keyBuilder.WriteString(strconv.FormatUint(objectId, 10))
	return keyBuilder.String()
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

func updateRolesTx(s *cacheServer, logger otelzap.LoggerWithCtx, roles *pb.Roles, dataTimeout time.Duration) {
	ctx := logger.Context()
	pipe := s.rdb.TxPipeline()
	for _, role := range roles.List {
		roleKey := getRoleKey(role.Name, role.ObjectId)
		actionsStr, _ := actionsFromCall(role.List)
		pipe.Set(ctx, roleKey, actionsStr, dataTimeout)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		logger.Error(cacheStorageMsg, zap.Error(err))
	}
}

func updateRoles(s *cacheServer, logger otelzap.LoggerWithCtx, roles *pb.Roles, dataTimeout time.Duration) {
	ctx := logger.Context()
	for _, role := range roles.List {
		roleKey := getRoleKey(role.Name, role.ObjectId)
		actionsStr, _ := actionsFromCall(role.List)
		if err := s.rdb.Set(ctx, roleKey, actionsStr, dataTimeout).Err(); err != nil {
			logger.Error(cacheStorageMsg, zap.Error(err))
			return
		}
	}
}

func updateUserTx(s *cacheServer, logger otelzap.LoggerWithCtx, userKey string, userData map[string]any, actionKeyIndicators map[string]string) {
	ctx := logger.Context()
	pipe := s.rdb.TxPipeline()
	// here userData contains only the roles
	for roleKey, actionsStr := range userData {
		pipe.Set(ctx, roleKey, actionsStr, s.dataTimeout)
	}

	for actionKey, indicator := range actionKeyIndicators {
		userData[actionKey] = indicator
	}

	pipe.Del(ctx, userKey)
	if len(userData) != 0 {
		pipe.HSet(ctx, userKey, userData)
	}
	if _, err := pipe.Exec(ctx); err == nil {
		s.updateWithTTL(logger, userKey)
	} else {
		logger.Error(cacheStorageMsg, zap.Error(err))
	}
}

func updateUser(s *cacheServer, logger otelzap.LoggerWithCtx, userKey string, userData map[string]any, actionKeyIndicators map[string]string) {
	ctx := logger.Context()
	// here userData contains only the roles
	for roleKey, actionsStr := range userData {
		if err := s.rdb.Set(ctx, roleKey, actionsStr, s.dataTimeout).Err(); err != nil {
			logger.Error(cacheStorageMsg, zap.Error(err))
			return
		}
	}

	for actionKey, indicator := range actionKeyIndicators {
		userData[actionKey] = indicator
	}

	if err := s.rdb.Del(ctx, userKey).Err(); err != nil {
		logger.Error(cacheStorageMsg, zap.Error(err))
		return
	}
	if len(userData) != 0 {
		if err := s.rdb.HSet(ctx, userKey, userData).Err(); err != nil {
			logger.Error(cacheStorageMsg, zap.Error(err))
			return
		}
		s.updateWithTTL(logger, userKey)
	}
}

func logCacheAccessError(logger otelzap.LoggerWithCtx, err error) {
	if err != redis.Nil {
		logger.WithOptions(zap.AddCallerSkip(1)).Error("Failed to access cache", zap.Error(err))
	}
}
