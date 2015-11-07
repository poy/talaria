package main

import (
	"fmt"
	"github.com/apoydence/talaria/kvstore"
	"github.com/hashicorp/consul/api"
	"sort"
	"strconv"
	"strings"
)

type fileInfo struct {
	num int
	uri string
}

type fileInfos []fileInfo

func main() {
	client, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		panic(err)
	}

	pairs, _, err := client.KV().List(kvstore.Prefix, nil)
	if err != nil {
		panic(err)
	}

	printInfos(buildAssociation(pairs))

	//for _, pair := range pairs {
	//println(name, num, string(pair.Value))
	//}
}

func printInfos(infos map[string][]fileInfo) {
	for k, v := range infos {
		fmt.Print(k)
		for _, info := range v {
			fmt.Printf(" -> %v", info)
		}
		println()
	}
}

func buildAssociation(pairs api.KVPairs) map[string][]fileInfo {
	result := make(map[string][]fileInfo)
	for _, pair := range pairs {
		name, num := splitKey(pair.Key)
		info := fileInfo{
			num: num,
			uri: string(pair.Value),
		}
		result[name] = append(result[name], info)
	}

	sortInfos(result)
	return result
}

func sortInfos(values map[string][]fileInfo) {
	for _, v := range values {
		sort.Sort(fileInfos(v))
	}
}

func splitKey(key string) (string, int) {
	parts := strings.Split(key, "~")
	if len(parts) != 2 {
		panic("Invalid key: " + key)
	}

	lastIdx := len(parts) - 1
	return removePrefix(concat(parts[:lastIdx])), convertToInt(parts[lastIdx])
}

func removePrefix(value string) string {
	prefixLen := len(kvstore.Prefix) + 1
	if len(value) < prefixLen {
		panic("Invalid key: " + value)
	}
	return value[prefixLen:]
}

func concat(parts []string) string {
	var result string
	for _, str := range parts {
		result += str
	}
	return result
}

func convertToInt(value string) int {
	i, err := strconv.Atoi(value)
	if err != nil {
		panic(err)
	}
	return i
}

func (f fileInfo) String() string {
	return fmt.Sprintf("%s(%d)", f.uri, f.num)
}

func (fs fileInfos) Len() int {
	return len(fs)
}

func (fs fileInfos) Less(i, j int) bool {
	return fs[i].num < fs[j].num
}

func (fs fileInfos) Swap(i, j int) {
	temp := fs[i]
	fs[i] = fs[j]
	fs[j] = temp
}
