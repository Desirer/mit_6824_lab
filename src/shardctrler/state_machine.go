package shardctrler

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
	// 1、添加group到新config
	for k, v := range args.Servers {
		newConfig.Groups[k] = v
	}
	// 2、计算平均情况下，每个group应该具有的shard数，向下取整
	totalGroupNum := len(newConfig.Groups)
	averageShardNum := NShards / totalGroupNum
	if averageShardNum < 1 {
		averageShardNum = 1 // 此时的group多于shard
	}
	// 3、遍历Shard数组，统计[可回收的shard]
	shardCount := make(map[int]int) // 每个gid具有的shard个数
	var recycledShards []int
	for sid, gid := range newConfig.Shards {
		if gid == 0 || shardCount[gid] >= averageShardNum { //0表示未分配
			recycledShards = append(recycledShards, sid)
			continue
		}
		shardCount[gid]++
	}
	// 4、分配回收的shard到groups中
	newGroups := getGroups(newConfig.Groups)
	sidx := 0
	gidx := 0
	// 遍历groups，将每个group补全至averageNum
	for gidx < len(newGroups) {
		gid := newGroups[gidx]
		if sidx < len(recycledShards) && shardCount[gid] < averageShardNum {
			newConfig.Shards[recycledShards[sidx]] = gid
			shardCount[gid]++
			sidx++
			continue
		}
		gidx++
	}
	// 如果recycledShards还有剩余，循环发给每个group
	gidx = 0
	for sidx < len(recycledShards) && gidx < len(newGroups) {
		gid := newGroups[gidx]
		newConfig.Shards[recycledShards[sidx]] = gid
		sidx++
		gidx++
	}
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
	// 1、删除group从config
	leavingGroups := make(map[int]bool)
	for _, gid := range args.GIDs {
		delete(newConfig.Groups, gid)
		leavingGroups[gid] = true
	}
	// 2、计算平均情况下，每个group应该具有的shard数，向下取整
	var averageShardNum int
	totalGroupNum := len(newConfig.Groups)
	if totalGroupNum == 0 {
		averageShardNum = 999
	} else if totalGroupNum > NShards {
		averageShardNum = 1
	} else {
		averageShardNum = NShards / totalGroupNum
	}
	// 3、遍历Shard数组，统计可回收的shard
	shardCount := make(map[int]int) // 每个gid具有的shard个数
	var recycledShards []int
	for sid, gid := range newConfig.Shards {
		// 如果gid在leavingGroup中，就回收它对应的shard
		if _, ok := leavingGroups[gid]; ok {
			newConfig.Shards[sid] = 0
			gid = 0
		}
		// 如果未分配或者超出了average的数额,就回收
		if gid == 0 || shardCount[gid] >= averageShardNum {
			recycledShards = append(recycledShards, sid)
			continue
		}
		shardCount[gid]++
	}
	//DPrintf("SC[%v]C[%v][%v], recycledShards%v, leftGroups%v, newConfig.Groups %v", sc.me, args.ClientId, args.SeqNum, recycledShards, leftGroups, newConfig.Groups)
	// 4、分配shard
	newGroups := getGroups(newConfig.Groups)
	sidx := 0
	gidx := 0
	// 遍历groups，将每个group补全至averageNum
	for gidx < len(newGroups) {
		gid := newGroups[gidx]
		if sidx < len(recycledShards) && shardCount[gid] < averageShardNum {
			newConfig.Shards[recycledShards[sidx]] = gid
			shardCount[gid]++
			sidx++
			continue
		}
		gidx++
	}
	// 如果recycledShards还有剩余，循环发给每个group
	gidx = 0
	for sidx < len(recycledShards) && gidx < len(newGroups) {
		gid := newGroups[gidx]
		newConfig.Shards[recycledShards[sidx]] = gid
		sidx++
		gidx++
	}

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
