package config

import (
	"encoding/xml"
	"fmt"
	chcv1beta1 "github.com/xiedeyantu/ch-operator/api/v1beta1"
	"strconv"
)

type ZkConfig struct {
	XMLName   xml.Name `xml:"yandex"`
	Zookeeper struct {
		Nodes []Node `xml:"node"`
	} `xml:"zookeeper"`
	DistributedDDL struct {
		Path string `xml:"path"`
	} `xml:"distributed_ddl"`
}

type Node struct {
	XMLName xml.Name `xml:"node"`
	Host    string   `xml:"host"`
	Port    string   `xml:"port"`
}

type RemoteConfig struct {
	XMLName       xml.Name `xml:"yandex"`
	RemoteServers struct {
		XMLName xml.Name `xml:"remote_servers"`
		Default struct {
			XMLName xml.Name `xml:"default"`
			Shards  []Shard  `xml:"shard"`
		} `xml:"default"`
	} `xml:"remote_servers"`
}

type Shard struct {
	XMLName             xml.Name  `xml:"shard"`
	Replicas            []Replica `xml:"replica"`
	InternalReplication string    `xml:"internal_replication"`
}

type Replica struct {
	XMLName xml.Name `xml:"replica"`
	Host    string   `xml:"host"`
	Port    string   `xml:"port"`
}

type MacrosConfig struct {
	XMLName xml.Name `xml:"yandex"`
	Macros  Macros   `xml:"macros"`
}

type Macros struct {
	XMLName xml.Name `xml:"macros"`
	Cluster string   `xml:"cluster"`
	Shard   string   `xml:"shard"`
	Replica string   `xml:"replica"`
}

type ListenConfig struct {
	XMLName                 xml.Name `xml:"yandex"`
	ListenHosts             []string `xml:"listen_host"`
	ListenTry               string   `xml:"listen_try"`
	DisableInternalDnsCache string   `xml:"disable_internal_dns_cache"`
}

func GenerateChZkConfig(zks []string) string {
	var z = ZkConfig{}
	for _, zk := range zks {
		z.Zookeeper.Nodes = append(z.Zookeeper.Nodes, Node{
			Host: zk,
			Port: "2181",
		})
	}
	z.DistributedDDL.Path = "/clickhouse/task_queue/ddl"

	resXML, err := xml.MarshalIndent(z, "", "  ")
	if err != nil {
		fmt.Printf("marshal xml err :%vn", err)
		return ""
	}
	return string(resXML)
}

func GenerateChRemoteConfig(shard, replica int32, hostnamePre string) string {
	var r = RemoteConfig{}
	for i := int32(0); i < shard; i++ {
		var replicas []Replica
		for j := int32(0); j < replica; j++ {
			replicas = append(replicas, Replica{
				Host: hostnamePre + "-" + strconv.FormatInt(int64(i), 10) + "-" + strconv.FormatInt(int64(j), 10),
				Port: "9000",
			})
		}
		r.RemoteServers.Default.Shards = append(r.RemoteServers.Default.Shards, Shard{
			Replicas:            replicas,
			InternalReplication: "true",
		})
	}

	resXML, err := xml.MarshalIndent(r, "", "  ")
	if err != nil {
		fmt.Printf("marshal xml err :%vn", err)
		return ""
	}
	return string(resXML)
}

func GenerateChMacrosConfig(shards int32, hostname string) string {
	var m = MacrosConfig{
		Macros: Macros{
			Cluster: "default",
			Shard:   strconv.FormatInt(int64(shards), 10),
			Replica: hostname,
		},
	}

	resXML, err := xml.MarshalIndent(m, "", "  ")
	if err != nil {
		fmt.Printf("marshal xml err :%vn", err)
		return ""
	}
	return string(resXML)
}

func GenerateChListenConfig() string {
	var l = ListenConfig{
		ListenHosts:             []string{"0.0.0.0", "::"},
		ListenTry:               "1",
		DisableInternalDnsCache: "1",
	}

	resXML, err := xml.MarshalIndent(l, "", "  ")
	if err != nil {
		fmt.Printf("marshal xml err :%vn", err)
		return ""
	}

	return string(resXML)
}

func GenerateChInitStartCommand(c *chcv1beta1.ClickHouseCluster) string {
	cmd := fmt.Sprintf(`
	  while true; do
		for i in $(seq 1 3); do
		  domain=%s-$(($i-1)).%s-zk.default.svc.cluster.local
		  role=$(echo srvr | nc $domain 2181|grep Mode|awk '{print $2}');
		  echo $domain $role
		  if [[ $role = "leader" ]]; then
			exit 0;
		  fi
		done;
	  done;`,
		c.GetZkName(), c.GetZkName())
	return cmd
}

func GenerateChStartCommand() string {
	return "/entrypoint.sh"
}
