package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aruncs31s/goredis"
	"github.com/go-redis/redis/v8"
)

var redisClient *redis.Client

func init() {
	redisClient = redis.NewClient(
		&redis.Options{
			Addr:     "localhost:8998",
			Password: "greenIsBest",
		},
	)
}

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func (u User) Display() {
	fmt.Printf("ID: %d , Name: %s \n", u.ID, u.Name)
}

func main() {

	ctx := context.Background()
	// Cache a user under the "users" tag
	user, err := goredis.CacheGetOrFetch(ctx, redisClient, "user:42", 5*time.Minute,
		func() (*User, error) {
			return &User{ID: 42, Name: "Alice"}, nil
		},
		"users",
	)
	user, err = goredis.CacheGetOrFetch(ctx, redisClient, "user:43", 5*time.Minute,
		func() (*User, error) {
			return &User{ID: 43, Name: "Alice"}, nil
		},
		"users",
	)
	if err != nil {
		panic(err)
	}
	if user == nil {
		goredis.InvalidateByTags(ctx, redisClient, "users")
		return
	}
	user.Display()

	// After a user update, clear all cached data tagged "users"
	goredis.InvalidateByTags(ctx, redisClient, "users")
	user, err = goredis.CacheGetOrFetch(ctx, redisClient, "user:42", 5*time.Minute,
		func() (*User, error) {
			return nil, nil
		},
		"users",
	)
	if user != nil {
		user.Display()
		return
	}
	fmt.Println("User is nil")
}
