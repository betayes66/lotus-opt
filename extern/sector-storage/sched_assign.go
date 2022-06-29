package sectorstorage

import (
	"context"
	"errors"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/specs-storage/storage"
	"golang.org/x/xerrors"
	"sync"
	"sync/atomic"
	"time"
)

const CAP = 68 // chan capacity

var (
	_pledgeChan = make(chan *workerRequest, CAP<<3) // global addpiece channel

	_p1Chan  = make(chan *workerRequest, CAP<<3)
	_p2Chan  = make(chan *workerRequest, CAP<<3)
	_c1Chan  = make(chan *workerRequest, CAP<<3)
	_c2Chan  = make(chan *workerRequest, CAP<<3) // global commit2 channel
	_finChan = make(chan *workerRequest, CAP<<3)
	_fetChan = make(chan *workerRequest, CAP<<3)
	_unsChan = make(chan *workerRequest, CAP<<3)

	_ruChan   = make(chan *workerRequest, CAP<<3)
	_pru1Chan = make(chan *workerRequest, CAP<<3)
	_pru2Chan = make(chan *workerRequest, CAP<<3)
	_rskChan  = make(chan *workerRequest, CAP<<3)
	_fruChan  = make(chan *workerRequest, CAP<<3)

	PledgeWait int32 // pledge-sector judge
	_rC2Num    int32 // reserve c2 number
)

var (
	// SWGMap : (SchedWorkerGlobalMap) sw.cfg.IP -> *SWChan
	SWGMap = sync.Map{}

	// SIDMapWip : (SectorIDMapWorkerIp) uint64(sector.ID.Number) -> worker.IP
	SIDMapWip = sync.Map{}

	// ApRunErrWorkers : sw.cfg.IP -> struct{}
	ApRunErrWorkers = sync.Map{}

	// WIPP1Map :  sw.cfg.IP -> " RP1Map(reserve and run p1 number) : uint64(sector.ID.Number) -> struct{} "
	WIPP1Map = sync.Map{}

	// WIPP2Map :  sw.cfg.IP -> " RP2Map(reserve and run p2 number) : uint64(sector.ID.Number) -> struct{} "
	WIPP2Map = sync.Map{}

	// C2SMap : *workerHandle -> struct{}
	C2SMap = sync.Map{}

)

// Rwc : Remote worker config
type Rwc struct {
	IP        string
	MinerAddr string

	Max uint8 // need more than 0
	Ap  uint8
	P1  uint8
	P2  uint8
	C2  uint8
}

// SWSChan : SchedWorkerStructChan
type SWSChan struct {
	p1Chan chan *workerRequest
	p2Chan chan *workerRequest
	c1Chan chan *workerRequest

	finChan chan *workerRequest
	fetChan chan *workerRequest
	unsChan chan *workerRequest

	ruChan   chan *workerRequest
	pru1Chan chan *workerRequest
	pru2Chan chan *workerRequest
	rskChan  chan *workerRequest
	fruChan  chan *workerRequest
}

// write *workerRequest to chan
func (sh *scheduler) schedAssign(ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	var (
		fwChan chan *workerRequest // final written channel
		scp    *SWSChan
	)
	if taskType != sealtasks.TTAddPiece && taskType != sealtasks.TTCommit2 {
		scp = loadScp(sh, ctx, sector, taskType, sel)
	}

	switch taskType {
	case sealtasks.TTAddPiece:
		fwChan = _pledgeChan
		atomic.AddInt32(&PledgeWait, 1)
	case sealtasks.TTPreCommit1:
		if scp != nil {
			fwChan = scp.p1Chan
		} else {
			fwChan = _p1Chan
		}
	case sealtasks.TTPreCommit2:
		if scp != nil {
			fwChan = scp.p2Chan
		} else {
			fwChan = _p2Chan
		}
	case sealtasks.TTCommit1:
		if scp != nil {
			fwChan = scp.c1Chan
		} else {
			fwChan = _c1Chan
		}

	case sealtasks.TTCommit2:
		fwChan = _c2Chan
		atomic.AddInt32(&_rC2Num, 1)

	case sealtasks.TTFinalize:
		if scp != nil {
			fwChan = scp.finChan
		} else {
			fwChan = _finChan
		}
	case sealtasks.TTFetch:
		if scp != nil {
			fwChan = scp.fetChan
		} else {
			fwChan = _fetChan
		}
	case sealtasks.TTUnseal:
		if scp != nil {
			fwChan = scp.unsChan
		} else {
			fwChan = _unsChan
		}
	case sealtasks.TTReplicaUpdate:
		if scp != nil {
			fwChan = scp.ruChan
		} else {
			fwChan = _ruChan
		}
	case sealtasks.TTProveReplicaUpdate1:
		if scp != nil {
			fwChan = scp.pru1Chan
		} else {
			fwChan = _pru1Chan
		}
	case sealtasks.TTProveReplicaUpdate2:
		if scp != nil {
			fwChan = scp.pru2Chan
		} else {
			fwChan = _pru2Chan
		}
	case sealtasks.TTRegenSectorKey:
		if scp != nil {
			fwChan = scp.rskChan
		} else {
			fwChan = _rskChan
		}
	case sealtasks.TTFinalizeReplicaUpdate:
		if scp != nil {
			fwChan = scp.fruChan
		} else {
			fwChan = _fruChan
		}
	}

	if fwChan == nil {
		return errors.New("fwChan chan *workerRequest is nil")
	}

	ret := make(chan workerResponse)
	select {
	case fwChan <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

}

// sid -> wip -> *swsc
func loadScp(sh *scheduler, ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector) *SWSChan {
outerFlag:
	if wip, ok := SIDMapWip.Load(uint64(sector.ID.Number)); ok {
		recordRp1Rp2Num(taskType, sector, wip.(string)) // record worker ip corresponding to " reserve's p1 number "
		if swsc, exist := SWGMap.Load(wip.(string)); exist {
			return swsc.(*SWSChan)
		}
		return nil
	} else {
		sh.workersLk.Lock()
		for _, worker := range sh.workers {
			if find := sel.FindDataWoker(ctx, taskType, sector.ID, sector.ProofType, worker); find {
				SIDMapWip.Store(uint64(sector.ID.Number), worker.IP)
				sh.workersLk.Unlock()
				goto outerFlag
			}
		}
		sh.workersLk.Unlock()
		return nil
	}
}

// record worker ip corresponding to " reserve's p1 number and reserve's p2 number "
func recordRp1Rp2Num(taskType sealtasks.TaskType, sector storage.SectorRef, ip string) {
	switch taskType {
	// start record
	case sealtasks.TTPreCommit1:
		// rp1Map(reserve and run p1 number) : uint64(sector.ID.Number) -> struct{}
		if rp1Map, ok := WIPP1Map.Load(ip); ok {
			val := rp1Map.(sync.Map)
			if _, exist := val.Load(uint64(sector.ID.Number)); !exist {
				val.Store(uint64(sector.ID.Number), struct{}{})
				WIPP1Map.Store(ip, val)
			}
		} else {
			rp1M := sync.Map{}
			rp1M.Store(uint64(sector.ID.Number), struct{}{})
			WIPP1Map.Store(ip, rp1M)
		}

		// remove rp1Map record for p2 three times fail
		if rp2Map, ok := WIPP2Map.Load(ip); ok {
			val := rp2Map.(sync.Map)
			if _, exist := val.Load(uint64(sector.ID.Number)); exist {
				val.Delete(uint64(sector.ID.Number))
				WIPP2Map.Store(ip, val)
			}
		}

	case sealtasks.TTPreCommit2:
		// rp2Map(reserve and run p2 number) : uint64(sector.ID.Number) -> struct{}
		if rp2Map, ok := WIPP2Map.Load(ip); ok {
			val := rp2Map.(sync.Map)
			if _, exist := val.Load(uint64(sector.ID.Number)); !exist {
				val.Store(uint64(sector.ID.Number), struct{}{})
				WIPP2Map.Store(ip, val)
			}
		} else {
			rp2M := sync.Map{}
			rp2M.Store(uint64(sector.ID.Number), struct{}{})
			WIPP2Map.Store(ip, rp2M)
		}

		// remove rp1Map record
		if rp1Map, ok := WIPP1Map.Load(ip); ok {
			val := rp1Map.(sync.Map)
			if _, exist := val.Load(uint64(sector.ID.Number)); exist {
				val.Delete(uint64(sector.ID.Number))
				WIPP1Map.Store(ip, val)
			}
		}

	case sealtasks.TTCommit1:
		// remove rp2Map record
		if rp2Map, ok := WIPP2Map.Load(ip); ok {
			val := rp2Map.(sync.Map)
			if _, exist := val.Load(uint64(sector.ID.Number)); exist {
				val.Delete(uint64(sector.ID.Number))
				WIPP2Map.Store(ip, val)
			}
		}

	default:

	}

}
