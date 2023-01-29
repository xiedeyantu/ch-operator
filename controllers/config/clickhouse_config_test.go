package config

import (
	"fmt"
	"testing"
)

func TestGenerateChZkConfig(t *testing.T) {
	zks := []string{"zk1", "zk2", "zk3"}
	cfg := GenerateChZkConfig(zks)
	fmt.Println(cfg)
}

func TestGenerateChRemoteConfig(t *testing.T) {
	shards := int32(3)
	replica := int32(3)
	cfg := GenerateChRemoteConfig(shards, replica, "clickhouse")
	fmt.Println(cfg)
}

func TestGenerateChListenConfig(t *testing.T) {
	cfg := GenerateChListenConfig()
	fmt.Println(cfg)
}
