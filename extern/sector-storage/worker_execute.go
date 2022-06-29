package sectorstorage

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"golang.org/x/xerrors"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	WARNCOUNT  = 3
	LIMITCOUNT = 1

	C2GPUNUM = 3 // c2 separator gpu limit number
	C2SRNUM  = 3 // per c2 separator reserve number

	P2RESERVESPACE    uint64 = 37536  // p2 reserve space : 37536 M
	TOTALRESERVESPACE uint64 = 529056 // p1 p2 reserve space : 463520M + 64G(65536M)

)

// sw loop request schedule
func (sw *schedWorker) loopSchedule(ctx context.Context) {

	var (
		taskTypes    []sealtasks.TaskType
		allTaskTypes = []sealtasks.TaskType{
			sealtasks.TTFinalize,
			sealtasks.TTFetch,
			sealtasks.TTUnseal,
			sealtasks.TTCommit1,
			sealtasks.TTCommit2,
			sealtasks.TTPreCommit2,
			sealtasks.TTPreCommit1,
			sealtasks.TTProveReplicaUpdate1,
			sealtasks.TTProveReplicaUpdate2,
			sealtasks.TTReplicaUpdate,
			sealtasks.TTAddPiece,
			sealtasks.TTRegenSectorKey,
		}
	)
	for _, taskType := range allTaskTypes {
		if _, ok := sw.taskTypes[taskType]; ok {
			taskTypes = append(taskTypes, taskType)
		}
	}

	log.Infof("remote worker:%+v -- support taskTypes:%+v", sw.cfg, taskTypes)

	_, apOk := sw.taskTypes[sealtasks.TTCommit2]
	_, p1Ok := sw.taskTypes[sealtasks.TTCommit2]
	_, p2Ok := sw.taskTypes[sealtasks.TTCommit2]
	info, err := sw.worker.workerRpc.Info(ctx)

	// check whether it is C2 separator machine
	if _, ok := sw.taskTypes[sealtasks.TTCommit2]; ok && (!apOk || !p1Ok || !p2Ok) {
		if err == nil && len(info.Resources.GPUs) >= C2GPUNUM {
			// record c2 separator machine :  *workerHandle
			C2SMap.Store(sw.worker, struct{}{})
		}
	}
	defer func() {
		sw.worker.enabled = false
		C2SMap.Delete(sw.worker)
	}()

	var (
		sign uint8     // make sure 3090 gpu priority
		poll uint8 = 3 // default : 2080 Ti
	)

	if len(info.Resources.GPUs) == 0 {
		log.Errorf("Note: WorkIp: %+v -- GPU crash\n", sw.cfg.IP)
		return
	} else {
		if strings.Contains(info.Resources.GPUs[0], "3080") {
			poll = 2
		}
	}

	for {

		// ping the worker and check session
		if !sw.checkSession(ctx) {
			return // invalid session / exiting
		}

		{
			sw.sched.workersLk.Lock()
			sw.worker.enabled = true
			sw.sched.workersLk.Unlock()
		}

		select {
		case <-ctx.Done():
			log.Warnf("warn: ctx.Done")
			return
		case <-sw.sched.closing:
			log.Info("warn: sw.sched.closing")
			return
		case <-sw.worker.closingMgr:
			log.Info("warn: sw.worker.closingMgr")
			return
		default:
			time.Sleep(6 * time.Second)
			for _, taskType := range taskTypes {
				if !sw.checkSession(ctx) {
					continue
				}
				// make sure 3090 gpu priority
				if taskType == sealtasks.TTCommit2 && len(info.Resources.GPUs) > 0 &&
					!strings.Contains(info.Resources.GPUs[0], "3090") && len(info.Resources.GPUs) < C2GPUNUM {
					if sign == 180 {
						sign = 0
					}
					sign++
					if sign%poll != 0 {
						continue
					}
				}
				sw.executeSeal(ctx, taskType, info)
			}
		}

	}

}

// read *workerRequest from chan
func (sw *schedWorker) executeSeal(ctx context.Context, taskType sealtasks.TaskType, info storiface.WorkerInfo) {
	// check limit concurrency number and max concurrency  number
	if sw.limit(taskType) {
		return
	}

	// exclude ApRunErrWorkers
	if taskType == sealtasks.TTAddPiece {
		// process ApRunErrWorkers
		if _, ok := ApRunErrWorkers.Load(sw.cfg.IP); ok {
			log.Warnf("notice: %+v do %+v is failure error, please check ... ", sw.cfg.IP, taskType)
			return
		}
	}

	// reserve c2 for c2 Separator machine
	if taskType == sealtasks.TTCommit2 && len(info.Resources.GPUs) < C2SRNUM {
		var (
			wip     string // find the ip address of p2 on the same machine as c2
			rp2Flag bool   // running and reserve p2 number
		)
		// find the ip address of the same machine for p2 task
		WIPP2Map.Range(func(key, value interface{}) bool {
			if ip, ok := key.(string); ok && len(strings.Split(sw.cfg.IP, ":")) > 0 &&
				strings.Contains(ip, strings.Split(sw.cfg.IP, ":")[0]) {
				wip = ip
				return false
			}
			return true
		})
		// judge whether there is reserved p2 on the same machine as c2
		if rp2Map, ok := WIPP2Map.Load(wip); ok {
			rp2M := rp2Map.(sync.Map)
			rp2M.Range(func(key, value interface{}) bool {
				rp2Flag = true
				return false
			})
		}
		if rp2Flag {
			var c2Rn int32 // c2 Separator machine reserve number
			C2SMap.Range(func(key, value interface{}) bool {
				if whl, ok := key.(*workerHandle); ok && whl.enabled && len(whl.info.Resources.GPUs) > 0 {
					if strings.Contains(whl.info.Resources.GPUs[0], "3090") {
						c2Rn += C2SRNUM << 1
					} else {
						c2Rn += C2SRNUM
					}
				}
				return true
			})
			if atomic.LoadInt32(&_rC2Num) <= c2Rn {
				return
			}
		}
	}

	// get SWSChan
	var swsc *SWSChan
	if taskType != sealtasks.TTCommit2 && taskType != sealtasks.TTAddPiece {
		if val, ok := SWGMap.Load(sw.cfg.IP); !ok || val.(*SWSChan) == nil {
			return
		} else {
			swsc = val.(*SWSChan)
		}
	}

	var (
		req     *workerRequest
		wrpChan chan *workerRequest // workerRequest pointer channel
	)

	switch taskType {
	case sealtasks.TTFinalize:
		wrpChan = swsc.finChan
	case sealtasks.TTFetch:
		wrpChan = swsc.fetChan
	case sealtasks.TTUnseal:
		wrpChan = swsc.unsChan
	case sealtasks.TTCommit1:
		wrpChan = swsc.c1Chan

	case sealtasks.TTCommit2:
		wrpChan = _c2Chan

	case sealtasks.TTPreCommit2:
		wrpChan = swsc.p2Chan
	case sealtasks.TTPreCommit1:
		wrpChan = swsc.p1Chan
	case sealtasks.TTProveReplicaUpdate1:
		wrpChan = swsc.pru1Chan
	case sealtasks.TTProveReplicaUpdate2:
		wrpChan = swsc.pru2Chan
	case sealtasks.TTReplicaUpdate:
		wrpChan = swsc.ruChan

	case sealtasks.TTAddPiece:
		wrpChan = _pledgeChan

	case sealtasks.TTRegenSectorKey:
		wrpChan = swsc.rskChan
	}

	if wrpChan == nil {
		return
	}

	select {
	case req = <-wrpChan:
	default: // corresponding chan is empty
	}
	if req == nil {
		return
	}

	if (req.taskType == sealtasks.TTAddPiece || req.taskType == sealtasks.TTPreCommit1 ||
		req.taskType == sealtasks.TTPreCommit2) && (!sw.selOk(req) || sw.dsFull(ctx, req)) {
		go func() {
			wrpChan <- req
			return
		}()
		return
	}

	log.Infof("wip: %v will do %v - sid: %v", sw.cfg.IP, req.taskType, req.sector.ID.Number)

	if req.taskType == sealtasks.TTAddPiece {
		atomic.AddInt32(&PledgeWait, -1)
		SIDMapWip.Store(uint64(req.sector.ID.Number), sw.cfg.IP)
	}

	if req.taskType == sealtasks.TTCommit2 {
		atomic.AddInt32(&_rC2Num, -1)
	}

	if err := sw.doTask(req); err != nil {
		log.Errorf("start do \"sid:%+v - %+v\" task error:%+v", req.sector.ID.Number, req.taskType, err)
		go req.respond(xerrors.Errorf("start do \"sid:%+v - %+v\" task error: %+v", req.sector.ID.Number, req.taskType, err))
	}

}

func (sw *schedWorker) doTask(req *workerRequest) error {
	w, sh := sw.worker, sw.sched

	needRes := w.info.Resources.ResourceSpec(req.sector.ProofType, req.taskType)

	w.lk.Lock()
	w.preparing.add(w.info.Resources, needRes)
	w.lk.Unlock()

	// make worker busy
	if req.taskType == sealtasks.TTAddPiece || req.taskType == sealtasks.TTPreCommit1 ||
		req.taskType == sealtasks.TTPreCommit2 || req.taskType == sealtasks.TTCommit2 {
		sw.lock.Lock()
		sw.runningTasks[req.SectorName()] = req
		sw.lock.Unlock()
	}

	go func() {
		// first run the prepare step (e.g. fetching sector data from other worker)
		tw := sh.workTracker.worker(sw.wid, w.info, w.workerRpc)
		tw.start()
		err := req.prepare(req.ctx, tw)
		w.lk.Lock()

		if err != nil {
			w.preparing.free(w.info.Resources, needRes)
			w.lk.Unlock()

			select {
			case sw.taskDone <- struct{}{}:
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			default: // there is a notification pending already
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond (prepare error: %+v)", err)
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}
			return
		}

		tw = sh.workTracker.worker(sw.wid, w.info, w.workerRpc)

		// start tracking work first early in case we need to wait for resources
		werr := make(chan error, 1)
		go func() {
			werr <- req.work(req.ctx, tw)
		}()

		// wait (if needed) for resources in the 'active' window
		err = w.active.withResources(sw.wid, w.info, needRes, &w.lk, func() error {
			w.preparing.free(w.info.Resources, needRes)
			w.lk.Unlock()
			defer w.lk.Lock() // we MUST return locked from this function

			select {
			case sw.taskDone <- struct{}{}:
			case <-sh.closing:
			default: // there is a notification pending already
			}

			// Do the work!
			tw.start()
			err = <-werr

			if req.taskType == sealtasks.TTAddPiece && err != nil {
				if _, ok := ApRunErrWorkers.Load(sw.cfg.IP); !ok {
					//ApRunErrWorkers.Store(sw.cfg.IP, struct{}{})
				}
			}

			if req.taskType == sealtasks.TTPreCommit1 {
				// delete rp1Map record
				if rp1Map, ok := WIPP1Map.Load(sw.cfg.IP); ok {
					val := rp1Map.(sync.Map)
					if _, exist := val.Load(uint64(req.sector.ID.Number)); exist {
						val.Delete(uint64(req.sector.ID.Number))
						WIPP1Map.Store(sw.cfg.IP, val)
					}
				}
			}

			if req.taskType == sealtasks.TTPreCommit2 {
				// delete rp1Map record
				if rp2Map, ok := WIPP2Map.Load(sw.cfg.IP); ok {
					val := rp2Map.(sync.Map)
					if _, exist := val.Load(uint64(req.sector.ID.Number)); exist {
						val.Delete(uint64(req.sector.ID.Number))
						WIPP2Map.Store(sw.cfg.IP, val)
					}
				}
			}

			if req.taskType == sealtasks.TTFinalize {
				SIDMapWip.Delete(uint64(req.sector.ID.Number))
			}

			// make worker free
			if req.taskType == sealtasks.TTAddPiece || req.taskType == sealtasks.TTPreCommit1 ||
				req.taskType == sealtasks.TTPreCommit2 || req.taskType == sealtasks.TTCommit2 {
				sw.lock.Lock()
				delete(sw.runningTasks, req.SectorName())
				sw.lock.Unlock()
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond")
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response")
			}

			return nil
		})

		w.lk.Unlock()

		// This error should always be nil, since nothing is setting it, but just to be safe:
		if err != nil {
			log.Errorf("error executing worker (withResources): %+v", err)
		}
	}()

	return nil
}

// reach max amount or limit concurrency number
func (sw *schedWorker) limit(typ sealtasks.TaskType) bool {
	// check max limit number and tasks parallel amount
	var (
		maxLimit     bool
		runningApNum uint8
		runningP1Num uint8
		runningP2Num uint8
		runningC2Num uint8
	)

	sw.lock.RLock()
	maxLimit = uint8(len(sw.runningTasks)) >= sw.cfg.Max
	for _, val := range sw.runningTasks {
		switch val.taskType {
		case sealtasks.TTAddPiece:
			runningApNum++
		case sealtasks.TTPreCommit1:
			runningP1Num++
		case sealtasks.TTPreCommit2:
			runningP2Num++
		case sealtasks.TTCommit2:
			runningC2Num++
		default:
		}
	}
	sw.lock.RUnlock()

	switch typ {
	case sealtasks.TTAddPiece:
		// set reconver
		defer func() {
			if err := recover(); err != nil {
				log.Warnf("note: rp1Map exist concurrent risk")
			}
		}()
		// running and reserve p1 number
		var rp1Num uint8
		if rp1Map, ok := WIPP1Map.Load(sw.cfg.IP); ok {
			rp1M := rp1Map.(sync.Map)
			rp1M.Range(func(key, value interface{}) bool {
				rp1Num++
				return true
			})
		}
		return maxLimit || (runningApNum >= sw.cfg.Ap || runningApNum+runningP1Num >= sw.cfg.P1 || rp1Num > sw.cfg.P1)
	case sealtasks.TTPreCommit1:
		return maxLimit || runningP1Num >= sw.cfg.P1
	case sealtasks.TTPreCommit2:
		return maxLimit || runningP2Num >= sw.cfg.P2
	case sealtasks.TTCommit2:
		return runningC2Num >= sw.cfg.C2
	default:
		return false
	}
}

func (sw *schedWorker) selOk(req *workerRequest) bool {
	rpcCtx, cancel := context.WithTimeout(req.ctx, SelectorTimeout)
	ok, err := req.sel.Ok(rpcCtx, req.taskType, req.sector.ProofType, sw.worker)
	cancel()
	if err != nil {
		log.Error(err)
	}
	return ok
}

// check remote worker disk free
func (sw *schedWorker) dsFull(ctx context.Context, req *workerRequest) bool {
	var (
		err error
		ds  storiface.DiskStatus
	)
	if ds, err = sw.worker.workerRpc.Disk(ctx); err != nil {
		log.Warnf("notice: %+v requesting disk space error: %+v", sw.cfg.IP, err)
		return true
	}

	var (
		p2Rs      = P2RESERVESPACE
		nRs       = TOTALRESERVESPACE // need reserve space
		acceptNum uint64
	)
	switch req.sector.ProofType {
	case abi.RegisteredSealProof_StackedDrg32GiBV1_1:
	case abi.RegisteredSealProof_StackedDrg64GiBV1_1:
		p2Rs = p2Rs << 1
		nRs = nRs << 1
	default:
	}

	var (
		addr      string
		entryName string

		// var p2
		p1Num uint64
		p2Num uint64
		p1Os  uint64 // p1OccupySize
		p2Os  uint64 // p2OccupySize

		// var p1
		//p1Num  uint64
		rp2Num uint64
		//p1Os   uint64
		rp2Os uint64 //  reserve and running p2OccupySize

		// var ap
		apNum  uint64
		rp1Num uint64
		//rp2Num uint64
		apOs  uint64 // apOccupySize
		rp1Os uint64
		//rp2Os uint64
	)

	if len([]byte(sw.cfg.MinerAddr)) > 1 {
		addr = string([]byte(sw.cfg.MinerAddr)[1:])
	}

	if ds.Sum != nil && len(ds.Sum) != 0 {
		for fileName, size := range ds.Sum {
			// check whether miner's node
			if !strings.Contains(fileName, addr) {
				continue
			}

			var sectorNumber uint64
			if len(strings.Split(fileName, "-")) > 2 {
				if sn, err := strconv.Atoi(strings.Split(fileName, "-")[2]); err != nil || sn == 0 {
					continue
				} else {
					sectorNumber = uint64(sn)
				}
			}

			if strings.Contains(fileName, req.sector.ID.Number.String()) {
				entryName = fileName
			}

			switch req.taskType {
			case sealtasks.TTPreCommit2:
				sw.lock.RLock()
				val, ok := sw.runningTasks[fileName]
				sw.lock.RUnlock()
				// running p1 number or running p2 number
				if ok {
					switch val.taskType {
					case sealtasks.TTPreCommit1:
						p1Num++
						p1Os += size
					case sealtasks.TTPreCommit2:
						p2Num++
						p2Os += size
					default:
					}
				}

			case sealtasks.TTPreCommit1:
				sw.lock.RLock()
				val, ok := sw.runningTasks[fileName]
				sw.lock.RUnlock()
				// running p1 number
				if ok && val.taskType == sealtasks.TTPreCommit1 {
					p1Num++
					p1Os += size
				}
				// running and reserve p2 number
				if rp2Map, ok := WIPP2Map.Load(sw.cfg.IP); ok {
					rp2M := rp2Map.(sync.Map)
					if _, rp2MapOk := rp2M.Load(sectorNumber); rp2MapOk {
						rp2Num++
						rp2Os += size
					}
				}

			case sealtasks.TTAddPiece:
				sw.lock.RLock()
				val, ok := sw.runningTasks[fileName]
				sw.lock.RUnlock()
				// running ap
				if ok && val.taskType == sealtasks.TTAddPiece {
					apNum++
					apOs += size
				}

				// running and reserve p1 number
				if rp1Map, ok := WIPP1Map.Load(sw.cfg.IP); ok {
					rp1M := rp1Map.(sync.Map)
					if _, rp1MapOk := rp1M.Load(sectorNumber); rp1MapOk {
						rp1Num++
						rp1Os += size
					}
				}
				// running and reserve p2 number
				if rp2Map, ok := WIPP2Map.Load(sw.cfg.IP); ok {
					rp2M := rp2Map.(sync.Map)
					if _, rp2MapOk := rp2M.Load(sectorNumber); rp2MapOk {
						rp2Num++
						rp2Os += size
					}
				}

			default:
			}
		}
	}

	var (
		fAs       = ds.Avail // fact avail space
		needSpace uint64     // need space
		ctNas     = nRs      // current task need all space
	)

	switch req.taskType {
	case sealtasks.TTPreCommit2:
		needSpace = p2Rs
		// nRs-p2Rs = unseal + p1Cache + sealed
		fAs = ds.Avail - ((nRs-p2Rs)*p1Num + nRs*p2Num - p1Os - p2Os)

	case sealtasks.TTPreCommit1:
		needSpace, ctNas = nRs-p2Rs, nRs-p2Rs
		fAs = ds.Avail - ((nRs-p2Rs)*p1Num + nRs*rp2Num - p1Os - rp2Os)

	case sealtasks.TTAddPiece:
		needSpace = nRs
		fAs = ds.Avail - ((nRs-p2Rs)*rp1Num + p2Rs*rp2Num + nRs*apNum - rp1Os - rp2Os - apOs)

	default:
	}

	if sos, ok := ds.Sum[entryName]; ok && len(ds.Sum) != 0 { // sector occupy size
		if sos < ctNas {
			ctNas -= sos
		} else {
			ctNas = 0
		}

	}

	if ctNas <= fAs {
		acceptNum = (fAs-ctNas)/needSpace + 1
	}

	if acceptNum < WARNCOUNT {
		taskType := "ap"
		log.Warnf("diskctl -- wip: %+v -- (disk) avail: %+v G - All: %+v G - used: %+v G - free: %+v G",
			sw.cfg.IP, convert(ds.Avail), convert(ds.All), convert(ds.Used), convert(ds.Free))
		switch req.taskType {
		case sealtasks.TTPreCommit2:
			taskType = "p2"
			log.Warnf("diskctl -- running p1/p2 number: %+v / %+v ", p1Num, p2Num)
		case sealtasks.TTPreCommit1:
			taskType = "p1"
			log.Warnf("diskctl -- running p1 number: %+v - running and reserve rp2 number: %+v", p1Num, rp2Num)
		case sealtasks.TTAddPiece:
			log.Warnf("diskctl -- running ap number: %+v - reserve rp1/rp2 number: %+v / %+v", apNum, rp1Num, rp2Num)
		default:
		}
		log.Warnf("diskctl -- current %+v (%+v) need: %+vG (occupy:%+vG) pk  %+vG (fact avail) => %+v can receive  %+v task number: %+v",
			req.sector.ID.Number, taskType, convert(ctNas), convert(needSpace), convert(fAs), sw.cfg.IP, taskType, acceptNum)
	}

	return acceptNum < LIMITCOUNT
}

func convert(u uint64) float64 {
	f, n := float64(u)/1024, 2
	n10 := math.Pow10(n)
	return math.Trunc((f+0.5/n10)*n10) / n10
}
