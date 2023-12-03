package shardctrler

import "sort"

// 状态机应用请求，重复请求直接返回Ok
func (sc *ShardCtrler) handleQuery(args *QueryArgs, reply *QueryReply) {
	//DPrintf("SC[%v] handleQuery C[%v][%v]", sc.me, args.ClientId, args.SeqNum)
	// query只读请求不作重复过滤
	reply.Err = OK
	if args.Num < 0 || args.Num >= len(sc.configs) {
		reply.Config = sc.configs[len(sc.configs)-1]
		return
	}
	reply.Config = sc.configs[args.Num]
	return
}

// gid -> [shards]
// Group到shard的对应
func Group2Shards(cfg *Config) map[int][]int {
	var ret map[int][]int
	ret = make(map[int][]int)
	for gid, _ := range cfg.Groups {
		ret[gid] = make([]int, 0)
	}
	for shard, gid := range cfg.Shards {
		ret[gid] = append(ret[gid], shard)
	}
	return ret
}

func GetGIDWithMaximumShards(s2g *map[int][]int) int {
	rGid := 0
	rLen := -1
	tmp := make([]int, 0)
	for k, v := range *s2g {
		if k == 0 && len(v) != 0 {
			return 0
		}
		tmp = append(tmp, k)
	}
	sort.Ints(tmp)
	for _, k := range tmp {
		v := (*s2g)[k]
		if len(v) > rLen {
			rLen = len(v)
			rGid = k
		}
	}
	return rGid
}

func GetGIDWithMinimumShards(s2g *map[int][]int) int {
	rGid := 0
	rLen := 99
	tmp := make([]int, 0)
	for k, _ := range *s2g {
		tmp = append(tmp, k)
	}
	sort.Ints(tmp)
	for _, k := range tmp {
		v := (*s2g)[k]
		if k != 0 && len(v) < rLen {
			rLen = len(v)
			rGid = k
		}
	}
	return rGid
}

/*
增加若干个group，遵循给出原则进行分配。先计算平均情况每个group应该具有的shard数，然后多的给新来的
*/
func (sc *ShardCtrler) handleJoin(args *JoinArgs, reply *JoinReply) {
	if sc.ifDuplicated(args.ClientId, args.SeqNum) {
		reply.Err = OK
		return
	}
	sc.clientMap[args.ClientId] = args.SeqNum
	newConfig := DeepCopy(&sc.configs[len(sc.configs)-1])
	//lastConfig := sc.configs[len(sc.configs)-1]
	//newConfig := Config{len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	for gid, servers := range args.Servers {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	//for k, v := range args.Servers {
	//	newConfig.Groups[k] = v
	//}
	s2g := Group2Shards(&newConfig)
	for {
		source, target := GetGIDWithMaximumShards(&s2g), GetGIDWithMinimumShards(&s2g)
		DPrintf("source=%v target=%v", source, target)
		if source != 0 && len(s2g[source])-len(s2g[target]) <= 1 {
			break
		}
		s2g[target] = append(s2g[target], s2g[source][0])
		s2g[source] = s2g[source][1:]
	}
	var newShards [NShards]int
	for gid, shards := range s2g {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
	reply.Err = OK
	DPrintf("SC[%v] handleJoin C[%v][%v], new config%v", sc.me, args.ClientId, args.SeqNum, newConfig)
	return
}

/*
将离开的group的shard，均匀的分配到存在的group身上。
*/
func (sc *ShardCtrler) handleLeave(args *LeaveArgs, reply *LeaveReply) {
	DPrintf("SC[%v] handleLeave C[%v][%v]", sc.me, args.ClientId, args.SeqNum)
	if sc.ifDuplicated(args.ClientId, args.SeqNum) {
		reply.Err = OK
		return
	}
	sc.clientMap[args.ClientId] = args.SeqNum
	newConfig := DeepCopy(&sc.configs[len(sc.configs)-1])
	s2g := Group2Shards(&newConfig)
	orphanShards := make([]int, 0)
	for _, gid := range args.GIDs {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := s2g[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(s2g, gid)
		}
	}
	var newShards [NShards]int
	// load balancing is performed only when raft groups exist
	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards {
			target := GetGIDWithMinimumShards(&s2g)
			s2g[target] = append(s2g[target], shard)
		}
		for gid, shards := range s2g {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
	reply.Err = OK
	return
}

func (sc *ShardCtrler) handleMove(args *MoveArgs, reply *MoveReply) {
	DPrintf("SC[%v] handleMove C[%v][%v]", sc.me, args.ClientId, args.SeqNum)
	if sc.ifDuplicated(args.ClientId, args.SeqNum) {
		reply.Err = OK
		return
	}
	sc.clientMap[args.ClientId] = args.SeqNum
	newConfig := DeepCopy(&sc.configs[len(sc.configs)-1])
	newConfig.Shards[args.Shard] = args.GID

	sc.configs = append(sc.configs, newConfig)
	reply.Err = OK
	return
}
