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
	cache := goredis.New(redisClient)

	user, err := goredis.CacheGetOrFetch(ctx, cache, "user:42", 5*time.Minute,
		func() (*User, error) {
			return &User{ID: 42, Name: "Alice"}, nil
		},
		"users",
	)
	if err != nil {
		panic(err)
	}

	_, err = goredis.CacheGetOrFetch(ctx, cache, "user:43", 5*time.Minute,
		func() (*User, error) {
			return &User{ID: 43, Name: "Alice"}, nil
		},
		"users",
	)
	if err != nil {
		panic(err)
	}

	if user == nil {
		cache.InvalidateByTags(ctx, "users")
		return
	}
	user.Display()

	cache.InvalidateByTags(ctx, "users")

	user, err = goredis.CacheGetOrFetch(ctx, cache, "user:42", 5*time.Minute,
		func() (*User, error) {
			return nil, nil
		},
		"users",
	)
	if err != nil {
		panic(err)
	}
	if user != nil {
		user.Display()
		return
	}
	fmt.Println("User is nil")
}
