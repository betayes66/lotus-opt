package sectorstorage

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"bytes"
	"fmt"
	gofstab "github.com/deniswernert/go-fstab"
	"github.com/gwaylib/errors"
	"io/ioutil"
	"net"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"

	"github.com/filecoin-project/go-state-types/proof"

	"github.com/elastic/go-sysinfo"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-statestore"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
)

var pathTypes = []storiface.SectorFileType{storiface.FTUnsealed, storiface.FTSealed, storiface.FTCache, storiface.FTUpdate, storiface.FTUpdateCache}

type WorkerConfig struct {

	TaskCfg Rwc

	TaskTypes []sealtasks.TaskType
	NoSwap    bool

	// IgnoreResourceFiltering enables task distribution to happen on this
	// worker regardless of its currently available resources. Used in testing
	// with the local worker.
	IgnoreResourceFiltering bool

	MaxParallelChallengeReads int           // 0 = no limit
	ChallengeReadTimeout      time.Duration // 0 = no timeout
}

// used do provide custom proofs impl (mostly used in testing)
type ExecutorFunc func() (ffiwrapper.Storage, error)
type EnvFunc func(string) (string, bool)

type LocalWorker struct {
	storage    stores.Store
	localStore *stores.Local
	sindex     stores.SectorIndex
	ret        storiface.WorkerReturn
	executor   ExecutorFunc
	noSwap     bool
	envLookup  EnvFunc

	// see equivalent field on WorkerConfig.
	ignoreResources bool

	ct          *workerCallTracker
	acceptTasks map[sealtasks.TaskType]struct{}
	running     sync.WaitGroup
	taskLk      sync.Mutex

	challengeThrottle    chan struct{}
	challengeReadTimeout time.Duration

	session     uuid.UUID
	testDisable int64
	closing     chan struct{}

	taskCfg Rwc

}

func newLocalWorker(executor ExecutorFunc, wcfg WorkerConfig, envLookup EnvFunc, store stores.Store, local *stores.Local, sindex stores.SectorIndex, ret storiface.WorkerReturn, cst *statestore.StateStore) *LocalWorker {
	acceptTasks := map[sealtasks.TaskType]struct{}{}
	for _, taskType := range wcfg.TaskTypes {
		acceptTasks[taskType] = struct{}{}
	}

	w := &LocalWorker{
		storage:    store,
		localStore: local,
		sindex:     sindex,
		ret:        ret,

		ct: &workerCallTracker{
			st: cst,
		},
		acceptTasks:          acceptTasks,
		executor:             executor,
		noSwap:               wcfg.NoSwap,
		envLookup:            envLookup,
		ignoreResources:      wcfg.IgnoreResourceFiltering,
		challengeReadTimeout: wcfg.ChallengeReadTimeout,
		session:              uuid.New(),
		closing:              make(chan struct{}),

		taskCfg: wcfg.TaskCfg,

	}

	if wcfg.MaxParallelChallengeReads > 0 {
		w.challengeThrottle = make(chan struct{}, wcfg.MaxParallelChallengeReads)
	}

	if w.executor == nil {
		w.executor = w.ffiExec
	}

	unfinished, err := w.ct.unfinished()
	if err != nil {
		log.Errorf("reading unfinished tasks: %+v", err)
		return w
	}

	go func() {
		for _, call := range unfinished {
			hostname, osErr := os.Hostname()
			if osErr != nil {
				log.Errorf("get hostname err: %+v", err)
				hostname = ""
			}

			err := storiface.Err(storiface.ErrTempWorkerRestart, xerrors.Errorf("worker [Hostname: %s] restarted", hostname))

			// TODO: Handle restarting PC1 once support is merged

			if doReturn(context.TODO(), call.RetType, call.ID, ret, nil, err) {
				if err := w.ct.onReturned(call.ID); err != nil {
					log.Errorf("marking call as returned failed: %s: %+v", call.RetType, err)
				}
			}
		}
	}()

	return w
}

func NewLocalWorker(wcfg WorkerConfig, store stores.Store, local *stores.Local, sindex stores.SectorIndex, ret storiface.WorkerReturn, cst *statestore.StateStore) *LocalWorker {
	return newLocalWorker(nil, wcfg, os.LookupEnv, store, local, sindex, ret, cst)
}

type localWorkerPathProvider struct {
	w  *LocalWorker
	op storiface.AcquireMode
}

func (l *localWorkerPathProvider) AcquireSector(ctx context.Context, sector storage.SectorRef, existing storiface.SectorFileType, allocate storiface.SectorFileType, sealing storiface.PathType) (storiface.SectorPaths, func(), error) {
	paths, storageIDs, err := l.w.storage.AcquireSector(ctx, sector, existing, allocate, sealing, l.op)
	if err != nil {
		return storiface.SectorPaths{}, nil, err
	}

	releaseStorage, err := l.w.localStore.Reserve(ctx, sector, allocate, storageIDs, storiface.FSOverheadSeal)
	if err != nil {
		return storiface.SectorPaths{}, nil, xerrors.Errorf("reserving storage space: %w", err)
	}

	log.Debugf("acquired sector %d (e:%d; a:%d): %v", sector, existing, allocate, paths)

	return paths, func() {
		releaseStorage()

		for _, fileType := range pathTypes {
			if fileType&allocate == 0 {
				continue
			}

			sid := storiface.PathByType(storageIDs, fileType)
			if err := l.w.sindex.StorageDeclareSector(ctx, storiface.ID(sid), sector.ID, fileType, l.op == storiface.AcquireMove); err != nil {
				log.Errorf("declare sector error: %+v", err)
			}
		}
	}, nil
}

func (l *LocalWorker) ffiExec() (ffiwrapper.Storage, error) {
	return ffiwrapper.New(&localWorkerPathProvider{w: l})
}

type ReturnType string

const (
	DataCid               ReturnType = "DataCid"
	AddPiece              ReturnType = "AddPiece"
	SealPreCommit1        ReturnType = "SealPreCommit1"
	SealPreCommit2        ReturnType = "SealPreCommit2"
	SealCommit1           ReturnType = "SealCommit1"
	SealCommit2           ReturnType = "SealCommit2"
	FinalizeSector        ReturnType = "FinalizeSector"
	FinalizeReplicaUpdate ReturnType = "FinalizeReplicaUpdate"
	ReplicaUpdate         ReturnType = "ReplicaUpdate"
	ProveReplicaUpdate1   ReturnType = "ProveReplicaUpdate1"
	ProveReplicaUpdate2   ReturnType = "ProveReplicaUpdate2"
	GenerateSectorKey     ReturnType = "GenerateSectorKey"
	ReleaseUnsealed       ReturnType = "ReleaseUnsealed"
	MoveStorage           ReturnType = "MoveStorage"
	UnsealPiece           ReturnType = "UnsealPiece"
	Fetch                 ReturnType = "Fetch"
)

// in: func(WorkerReturn, context.Context, CallID, err string)
// in: func(WorkerReturn, context.Context, CallID, ret T, err string)
func rfunc(in interface{}) func(context.Context, storiface.CallID, storiface.WorkerReturn, interface{}, *storiface.CallError) error {
	rf := reflect.ValueOf(in)
	ft := rf.Type()
	withRet := ft.NumIn() == 5

	return func(ctx context.Context, ci storiface.CallID, wr storiface.WorkerReturn, i interface{}, err *storiface.CallError) error {
		rctx := reflect.ValueOf(ctx)
		rwr := reflect.ValueOf(wr)
		rerr := reflect.ValueOf(err)
		rci := reflect.ValueOf(ci)

		var ro []reflect.Value

		if withRet {
			ret := reflect.ValueOf(i)
			if i == nil {
				ret = reflect.Zero(rf.Type().In(3))
			}

			ro = rf.Call([]reflect.Value{rwr, rctx, rci, ret, rerr})
		} else {
			ro = rf.Call([]reflect.Value{rwr, rctx, rci, rerr})
		}

		if !ro[0].IsNil() {
			return ro[0].Interface().(error)
		}

		return nil
	}
}

var returnFunc = map[ReturnType]func(context.Context, storiface.CallID, storiface.WorkerReturn, interface{}, *storiface.CallError) error{
	DataCid:               rfunc(storiface.WorkerReturn.ReturnDataCid),
	AddPiece:              rfunc(storiface.WorkerReturn.ReturnAddPiece),
	SealPreCommit1:        rfunc(storiface.WorkerReturn.ReturnSealPreCommit1),
	SealPreCommit2:        rfunc(storiface.WorkerReturn.ReturnSealPreCommit2),
	SealCommit1:           rfunc(storiface.WorkerReturn.ReturnSealCommit1),
	SealCommit2:           rfunc(storiface.WorkerReturn.ReturnSealCommit2),
	FinalizeSector:        rfunc(storiface.WorkerReturn.ReturnFinalizeSector),
	ReleaseUnsealed:       rfunc(storiface.WorkerReturn.ReturnReleaseUnsealed),
	ReplicaUpdate:         rfunc(storiface.WorkerReturn.ReturnReplicaUpdate),
	ProveReplicaUpdate1:   rfunc(storiface.WorkerReturn.ReturnProveReplicaUpdate1),
	ProveReplicaUpdate2:   rfunc(storiface.WorkerReturn.ReturnProveReplicaUpdate2),
	GenerateSectorKey:     rfunc(storiface.WorkerReturn.ReturnGenerateSectorKeyFromData),
	FinalizeReplicaUpdate: rfunc(storiface.WorkerReturn.ReturnFinalizeReplicaUpdate),
	MoveStorage:           rfunc(storiface.WorkerReturn.ReturnMoveStorage),
	UnsealPiece:           rfunc(storiface.WorkerReturn.ReturnUnsealPiece),
	Fetch:                 rfunc(storiface.WorkerReturn.ReturnFetch),
}

func (l *LocalWorker) asyncCall(ctx context.Context, sector storage.SectorRef, rt ReturnType, work func(ctx context.Context, ci storiface.CallID) (interface{}, error)) (storiface.CallID, error) {
	ci := storiface.CallID{
		Sector: sector.ID,
		ID:     uuid.New(),
	}

	if err := l.ct.onStart(ci, rt); err != nil {
		log.Errorf("tracking call (start): %+v", err)
	}

	l.running.Add(1)

	go func() {
		defer l.running.Done()

		ctx := &wctx{
			vals:    ctx,
			closing: l.closing,
		}

		res, err := work(ctx, ci)
		if err != nil {
			rb, err := json.Marshal(res)
			if err != nil {
				log.Errorf("tracking call (marshaling results): %+v", err)
			} else {
				if err := l.ct.onDone(ci, rb); err != nil {
					log.Errorf("tracking call (done): %+v", err)
				}
			}
		}

		if err != nil {
			hostname, osErr := os.Hostname()
			if osErr != nil {
				log.Errorf("get hostname err: %+v", err)
			}

			err = xerrors.Errorf("%w [Hostname: %s]", err.Error(), hostname)
		}

		if doReturn(ctx, rt, ci, l.ret, res, toCallError(err)) {
			if err := l.ct.onReturned(ci); err != nil {
				log.Errorf("tracking call (done): %+v", err)
			}
		}
	}()
	return ci, nil
}

func toCallError(err error) *storiface.CallError {
	var serr *storiface.CallError
	if err != nil && !xerrors.As(err, &serr) {
		serr = storiface.Err(storiface.ErrUnknown, err)
	}

	return serr
}

// doReturn tries to send the result to manager, returns true if successful
func doReturn(ctx context.Context, rt ReturnType, ci storiface.CallID, ret storiface.WorkerReturn, res interface{}, rerr *storiface.CallError) bool {
	for {
		err := returnFunc[rt](ctx, ci, ret, res, rerr)
		if err == nil {
			break
		}

		log.Errorf("return error, will retry in 5s: %s: %+v", rt, err)
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			log.Errorf("failed to return results: %s", ctx.Err())

			// fine to just return, worker is most likely shutting down, and
			// we didn't mark the result as returned yet, so we'll try to
			// re-submit it on restart
			return false
		}
	}

	return true
}

func (l *LocalWorker) NewSector(ctx context.Context, sector storage.SectorRef) error {
	sb, err := l.executor()
	if err != nil {
		return err
	}

	return sb.NewSector(ctx, sector)
}

func (l *LocalWorker) DataCid(ctx context.Context, pieceSize abi.UnpaddedPieceSize, pieceData storage.Data) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, storage.NoSectorRef, DataCid, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return sb.DataCid(ctx, pieceSize, pieceData)
	})
}

func ln(src string, dst string) error {
	var errOut bytes.Buffer
	cmd := exec.Command("/usr/bin/env", "ln", src, dst)
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return xerrors.Errorf("exec ln (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
	}
	return nil
}

var RwLock = sync.RWMutex{}

func (l *LocalWorker) AddPiece(ctx context.Context, sector storage.SectorRef, epcs []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, AddPiece, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {

		if os.Getenv("AP_CACHE") == "true" { // use ap cache
			RwLock.Lock()
			defer RwLock.Unlock()

			log.Infof("LocalWork.AddPiece : miner rpc call add piece  sector:%+v, epcs:%+v, sz:%+v, r:%+v", sector, epcs, sz, r)
			storagePaths, pathsErr := l.Paths(ctx)
			if pathsErr != nil {
				return nil, pathsErr
			}

			for _, path := range storagePaths {
				if !path.CanSeal {
					continue
				}

				var (
					linkPieceFile = fmt.Sprintf("%s/linkPieceFile", path.LocalPath)
					cachePieceCid = fmt.Sprintf("%s/cachePieceCid", path.LocalPath)
					fileSrc       = filepath.Join(path.LocalPath, fmt.Sprintf("unsealed/s-t0%d-%d", sector.ID.Miner, sector.ID.Number))
					f1, lpfErr    = os.Stat(linkPieceFile)
					_, cpcErr     = os.Stat(cachePieceCid)
					flag          bool
				)

			redo:
				// " err != nil " is true => " os.IsNotExist(lpfErr) " is true
				if os.IsNotExist(lpfErr) || os.IsNotExist(cpcErr) || flag || f1.Size() < 34359738378 {

					pieceInfo, apErr := sb.AddPiece(ctx, sector, epcs, sz, r)
					if apErr != nil {
						return nil, apErr
					}
					if lnFlErr := ln(fileSrc, linkPieceFile); lnFlErr != nil {
						log.Errorf("ln(fileSrc, linkPieceFile) is err:%+v", lnFlErr)
						return pieceInfo, nil
					}
					_, statErr := os.Stat(cachePieceCid)
					if statErr != nil && os.IsNotExist(statErr) {
						if wfErr := ioutil.WriteFile(cachePieceCid, []byte(pieceInfo.PieceCID.String()), os.ModePerm); wfErr != nil {
							log.Errorf("os.Stat(x) is err:%+v or ioutil.WriteFile(x) is err:%+v", statErr, wfErr)
						}
					}
					return pieceInfo, nil
				}

				if lnLfErr := ln(linkPieceFile, fileSrc); lnLfErr != nil {
					log.Errorf("ln(linkPieceFile, fileSrc) is err:%+v", lnLfErr)
					flag = true
					goto redo
				}

				if data, rfErr := ioutil.ReadFile(cachePieceCid); rfErr != nil {
					log.Errorf("ioutil.ReadFile(x) is err:%+v", rfErr)
					flag = true
					goto redo
				} else {
					cpc, parseErr := cid.Parse(string(data))
					if parseErr != nil {
						log.Errorf("cid.Parse(string(data)) is err:%+v", parseErr)
						flag = true
						goto redo
					}
					if sdsErr := l.sindex.StorageDeclareSector(ctx, path.ID, sector.ID, storiface.FTUnsealed, path.CanStore); sdsErr != nil {
						log.Errorf("l.sindex.StorageDeclareSector(ctx ... ) is err:%+v", sdsErr)
						flag = true
						goto redo
					}
					return abi.PieceInfo{
						Size:     sz.Padded(),
						PieceCID: cpc,
					}, nil
				}
			}
		}

		return sb.AddPiece(ctx, sector, epcs, sz, r)
	})
}

func (l *LocalWorker) Fetch(ctx context.Context, sector storage.SectorRef, fileType storiface.SectorFileType, ptype storiface.PathType, am storiface.AcquireMode) (storiface.CallID, error) {
	return l.asyncCall(ctx, sector, Fetch, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		_, done, err := (&localWorkerPathProvider{w: l, op: am}).AcquireSector(ctx, sector, fileType, storiface.FTNone, ptype)
		if err == nil {
			done()
		}

		return nil, err
	})
}

func (l *LocalWorker) SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (storiface.CallID, error) {
	return l.asyncCall(ctx, sector, SealPreCommit1, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {

		{
			// cleanup previous failed attempts if they exist
			if err := l.storage.Remove(ctx, sector.ID, storiface.FTSealed, true, nil); err != nil {
				return nil, xerrors.Errorf("cleaning up sealed data: %w", err)
			}

			if err := l.storage.Remove(ctx, sector.ID, storiface.FTCache, true, nil); err != nil {
				return nil, xerrors.Errorf("cleaning up cache data: %w", err)
			}
		}

		sb, err := l.executor()
		if err != nil {
			return nil, err
		}

		return sb.SealPreCommit1(ctx, sector, ticket, pieces)
	})
}

func (l *LocalWorker) SealPreCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.PreCommit1Out) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, SealPreCommit2, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {

		if os.Getenv("ASSIGN_P2_CPU_LIST") != "" {
			return SubProcessSealP2(ctx, sector, phase1Out)
		}

		return sb.SealPreCommit2(ctx, sector, phase1Out)
	})
}

func SubProcessSealP2(ctx context.Context, sector storage.SectorRef, pc1o storage.PreCommit1Out) (storage.SectorCids, error) {
	defer func() {
		if e := recover(); e != nil {
			log.Errorf("sub_process seal p2 panic: %v\n%v", e, string(debug.Stack()))
		}
	}()

	task := ffiwrapper.P2TaskIn{
		Sector:    sector,
		Phase1Out: pc1o,
	}
	args, err := json.Marshal(task)
	if err != nil {
		return storage.SectorCids{}, errors.As(err)
	}
	programName := os.Args[0]

	unixAddr := filepath.Join(os.TempDir(), ".p2-"+sector.ID.Number.String()+"-"+uuid.New().String())
	defer os.Remove(unixAddr)

	var cmd *exec.Cmd
	if os.Getenv("ASSIGN_P2_CPU_LIST") != "" {
		cpuList := os.Getenv("ASSIGN_P2_CPU_LIST")
		cmd = exec.CommandContext(context.TODO(), "taskset", "-c", cpuList, programName,
			"precommit2",
			"--worker-repo", os.Getenv("WORKER_PATH"),
			"--name", task.TaskName(),
			"--addr", unixAddr,
		)
	} else {
		cmd = exec.CommandContext(context.TODO(), programName,
			"precommit2",
			"--worker-repo", os.Getenv("WORKER_PATH"),
			"--name", task.TaskName(),
			"--addr", unixAddr,
		)
	}

	// set the env
	cmd.Env = os.Environ()
	// output the stderr log
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	if err := cmd.Start(); err != nil {
		return storage.SectorCids{}, errors.As(err)
	}
	defer func() {
		time.Sleep(3e9)    // wait 3 seconds for exit.
		cmd.Process.Kill() // restfull exit.
		if err := cmd.Wait(); err != nil {
			log.Error(err)
		}
	}()
	// transfer precommit1 parameters
	var d net.Dialer
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*6)
	defer cancel()

	d.LocalAddr = nil // if you have a local addr, add it here
	raddr := net.UnixAddr{Name: unixAddr, Net: "unix"}

	retryTime := 0
loopUnixConn:
	conn, err := d.DialContext(ctx, "unix", raddr.String())
	if err != nil {
		retryTime++
		if retryTime < 30 {
			time.Sleep(1e9)
			goto loopUnixConn
		}
		return storage.SectorCids{}, errors.As(err)
	}
	defer conn.Close()

	if _, err := conn.Write(args); err != nil {
		return storage.SectorCids{}, errors.As(err)
	}

	// waiting to receive p2 sub process seal result's data
	out, err := ffiwrapper.ReadUnixConn(conn)
	if err != nil {
		return storage.SectorCids{}, errors.As(err)
	}

	resp := ffiwrapper.ExecP2TaskOut{}
	if err := json.Unmarshal(out, &resp); err != nil {
		return storage.SectorCids{}, errors.As(err)
	}
	if len(resp.Err) > 0 {
		return storage.SectorCids{}, xerrors.Errorf(resp.Err)
	}

	return resp.Data, nil
}

func (l *LocalWorker) SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, SealCommit1, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return sb.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
	})
}

func (l *LocalWorker) SealCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.Commit1Out) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, SealCommit2, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return sb.SealCommit2(ctx, sector, phase1Out)
	})
}

func (l *LocalWorker) ReplicaUpdate(ctx context.Context, sector storage.SectorRef, pieces []abi.PieceInfo) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, ReplicaUpdate, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		sealerOut, err := sb.ReplicaUpdate(ctx, sector, pieces)
		return sealerOut, err
	})
}

func (l *LocalWorker) ProveReplicaUpdate1(ctx context.Context, sector storage.SectorRef, sectorKey, newSealed, newUnsealed cid.Cid) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, ProveReplicaUpdate1, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return sb.ProveReplicaUpdate1(ctx, sector, sectorKey, newSealed, newUnsealed)
	})
}

func (l *LocalWorker) ProveReplicaUpdate2(ctx context.Context, sector storage.SectorRef, sectorKey, newSealed, newUnsealed cid.Cid, vanillaProofs storage.ReplicaVanillaProofs) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, ProveReplicaUpdate2, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return sb.ProveReplicaUpdate2(ctx, sector, sectorKey, newSealed, newUnsealed, vanillaProofs)
	})
}

func (l *LocalWorker) GenerateSectorKeyFromData(ctx context.Context, sector storage.SectorRef, commD cid.Cid) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, GenerateSectorKey, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		return nil, sb.GenerateSectorKeyFromData(ctx, sector, commD)
	})
}

func (l *LocalWorker) FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, FinalizeSector, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		if err := sb.FinalizeSector(ctx, sector, keepUnsealed); err != nil {
			return nil, xerrors.Errorf("finalizing sector: %w", err)
		}

		if len(keepUnsealed) == 0 {
			if err := l.storage.Remove(ctx, sector.ID, storiface.FTUnsealed, true, nil); err != nil {
				return nil, xerrors.Errorf("removing unsealed data: %w", err)
			}
		}

		return nil, err
	})
}

func (l *LocalWorker) FinalizeReplicaUpdate(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, FinalizeReplicaUpdate, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		if err := sb.FinalizeReplicaUpdate(ctx, sector, keepUnsealed); err != nil {
			return nil, xerrors.Errorf("finalizing sector: %w", err)
		}

		if len(keepUnsealed) == 0 {
			if err := l.storage.Remove(ctx, sector.ID, storiface.FTUnsealed, true, nil); err != nil {
				return nil, xerrors.Errorf("removing unsealed data: %w", err)
			}
		}

		return nil, err
	})
}

func (l *LocalWorker) ReleaseUnsealed(ctx context.Context, sector storage.SectorRef, safeToFree []storage.Range) (storiface.CallID, error) {
	return storiface.UndefCall, xerrors.Errorf("implement me")
}

func (l *LocalWorker) Remove(ctx context.Context, sector abi.SectorID) error {
	var err error

	if rerr := l.storage.Remove(ctx, sector, storiface.FTSealed, true, nil); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (sealed): %w", rerr))
	}
	if rerr := l.storage.Remove(ctx, sector, storiface.FTCache, true, nil); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (cache): %w", rerr))
	}
	if rerr := l.storage.Remove(ctx, sector, storiface.FTUnsealed, true, nil); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (unsealed): %w", rerr))
	}

	return err
}

func (l *LocalWorker) MoveStorage(ctx context.Context, sector storage.SectorRef, types storiface.SectorFileType) (storiface.CallID, error) {
	return l.asyncCall(ctx, sector, MoveStorage, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		if err := l.storage.MoveStorage(ctx, sector, types); err != nil {
			return nil, xerrors.Errorf("move to storage: %w", err)
		}

		for _, fileType := range storiface.PathTypes {
			if fileType&types == 0 {
				continue
			}

			if err := l.storage.RemoveCopies(ctx, sector.ID, fileType); err != nil {
				return nil, xerrors.Errorf("rm copies (t:%s, s:%v): %w", fileType, sector, err)
			}
		}
		return nil, nil
	})
}

func (l *LocalWorker) UnsealPiece(ctx context.Context, sector storage.SectorRef, index storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, randomness abi.SealRandomness, cid cid.Cid) (storiface.CallID, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.UndefCall, err
	}

	return l.asyncCall(ctx, sector, UnsealPiece, func(ctx context.Context, ci storiface.CallID) (interface{}, error) {
		log.Debugf("worker will unseal piece now, sector=%+v", sector.ID)
		if err = sb.UnsealPiece(ctx, sector, index, size, randomness, cid); err != nil {
			return nil, xerrors.Errorf("unsealing sector: %w", err)
		}

		if err = l.storage.RemoveCopies(ctx, sector.ID, storiface.FTSealed); err != nil {
			return nil, xerrors.Errorf("removing source data: %w", err)
		}

		if err = l.storage.RemoveCopies(ctx, sector.ID, storiface.FTCache); err != nil {
			return nil, xerrors.Errorf("removing source data: %w", err)
		}

		log.Debugf("worker has unsealed piece, sector=%+v", sector.ID)

		return nil, nil
	})
}

func (l *LocalWorker) GenerateWinningPoSt(ctx context.Context, ppt abi.RegisteredPoStProof, mid abi.ActorID, sectors []storiface.PostSectorChallenge, randomness abi.PoStRandomness) ([]proof.PoStProof, error) {
	sb, err := l.executor()
	if err != nil {
		return nil, err
	}

	// don't throttle winningPoSt
	// * Always want it done asap
	// * It's usually just one sector
	var wg sync.WaitGroup
	wg.Add(len(sectors))

	vproofs := make([][]byte, len(sectors))
	var rerr error

	for i, s := range sectors {
		go func(i int, s storiface.PostSectorChallenge) {
			defer wg.Done()

			if l.challengeReadTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, l.challengeReadTimeout)
				defer cancel()
			}

			vanilla, err := l.storage.GenerateSingleVanillaProof(ctx, mid, s, ppt)
			if err != nil {
				rerr = multierror.Append(rerr, xerrors.Errorf("get winning sector:%d,vanila failed: %w", s.SectorNumber, err))
				return
			}
			if vanilla == nil {
				rerr = multierror.Append(rerr, xerrors.Errorf("get winning sector:%d,vanila is nil", s.SectorNumber))
			}
			vproofs[i] = vanilla
		}(i, s)
	}
	wg.Wait()

	if rerr != nil {
		return nil, rerr
	}

	return sb.GenerateWinningPoStWithVanilla(ctx, ppt, mid, randomness, vproofs)
}

func (l *LocalWorker) GenerateWindowPoSt(ctx context.Context, ppt abi.RegisteredPoStProof, mid abi.ActorID, sectors []storiface.PostSectorChallenge, partitionIdx int, randomness abi.PoStRandomness) (storiface.WindowPoStResult, error) {
	sb, err := l.executor()
	if err != nil {
		return storiface.WindowPoStResult{}, err
	}

	var slk sync.Mutex
	var skipped []abi.SectorID

	var wg sync.WaitGroup
	wg.Add(len(sectors))

	vproofs := make([][]byte, len(sectors))

	for i, s := range sectors {
		if l.challengeThrottle != nil {
			select {
			case l.challengeThrottle <- struct{}{}:
			case <-ctx.Done():
				return storiface.WindowPoStResult{}, xerrors.Errorf("context error waiting on challengeThrottle %w", err)
			}
		}

		go func(i int, s storiface.PostSectorChallenge) {
			defer wg.Done()
			defer func() {
				if l.challengeThrottle != nil {
					<-l.challengeThrottle
				}
			}()

			if l.challengeReadTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, l.challengeReadTimeout)
				defer cancel()
			}

			vanilla, err := l.storage.GenerateSingleVanillaProof(ctx, mid, s, ppt)
			slk.Lock()
			defer slk.Unlock()

			if err != nil || vanilla == nil {
				skipped = append(skipped, abi.SectorID{
					Miner:  mid,
					Number: s.SectorNumber,
				})
				log.Errorf("reading PoSt challenge for sector %d, vlen:%d, err: %s", s.SectorNumber, len(vanilla), err)
				return
			}

			vproofs[i] = vanilla
		}(i, s)
	}
	wg.Wait()

	if len(skipped) > 0 {
		// This should happen rarely because before entering GenerateWindowPoSt we check all sectors by reading challenges.
		// When it does happen, window post runner logic will just re-check sectors, and retry with newly-discovered-bad sectors skipped
		log.Errorf("couldn't read some challenges (skipped %d)", len(skipped))

		// note: can't return an error as this in an jsonrpc call
		return storiface.WindowPoStResult{Skipped: skipped}, nil
	}

	res, err := sb.GenerateWindowPoStWithVanilla(ctx, ppt, mid, randomness, vproofs, partitionIdx)

	return storiface.WindowPoStResult{
		PoStProofs: res,
		Skipped:    skipped,
	}, err
}

func (l *LocalWorker) TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error) {
	l.taskLk.Lock()
	defer l.taskLk.Unlock()

	return l.acceptTasks, nil
}

func (l *LocalWorker) TaskCfg(context.Context) (Rwc, error) {
	l.taskLk.Lock()
	defer l.taskLk.Unlock()
	return l.taskCfg, nil
}

func (l *LocalWorker) Disk(context.Context) (storiface.DiskStatus, error) {
	if disk := disk(); disk != nil {
		return *disk, nil
	}

	return storiface.DiskStatus{}, errors.New("invoke diskAll fail ")
}

func disk() (disk *storiface.DiskStatus) {
	// parse mount dir
	mounts, err := gofstab.ParseProc()
	if err != nil {
		log.Warnf("gofstab.ParseProc() is fail")
		return
	}

	var (
		unsealedPath = os.Getenv("LOTUS_WORKER_PATH") + "/unsealed/"
		cachePath    = os.Getenv("LOTUS_WORKER_PATH") + "/cache/"
		sealedPath   = os.Getenv("LOTUS_WORKER_PATH") + "/sealed/"
	)
	if os.Getenv("LOTUS_WORKER_PATH") == "" {
		unsealedPath = os.Getenv("WORKER_PATH") + unsealedPath
		cachePath = os.Getenv("WORKER_PATH") + cachePath
		sealedPath = os.Getenv("WORKER_PATH") + sealedPath
	}

	for _, val := range mounts {
		if strings.Contains(val.File, "/mnt") {
			disk = diskUsage(val.File)
			sum := make(map[string]uint64)
			disk.Unsealed = sectorSize(unsealedPath, val.File, sum)
			disk.Cache = sectorSize(cachePath, val.File, sum)
			disk.Sealed = sectorSize(sealedPath, val.File, sum)
			disk.Sum = sum
			return
		}
	}
	return
}

// disk usage of path/disk
func diskUsage(path string) *storiface.DiskStatus {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return nil
	}

	var ds storiface.DiskStatus
	ds.All = fs.Blocks * uint64(fs.Bsize) / MB
	ds.Free = fs.Bfree * uint64(fs.Bsize) / MB
	ds.Used = ds.All - ds.Free

	ds.Avail = fs.Bavail * uint64(fs.Bsize) / MB
	return &ds
}

func DirSize(path string) (uint64, error) {
	var size uint64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return err
	})
	return size, err
}

func sectorSize(path string, val string, sum map[string]uint64) map[string]uint64 {
	if strings.Contains(path, val) {
		entries, err := ioutil.ReadDir(path)
		if err != nil {
			log.Warnf("ioutil.ReadDir(%+v) is fail", path)
			return nil
		}

		m := make(map[string]uint64)
		for _, entry := range entries {
			if strings.Contains(entry.Name(), "s-t") {
				// dir under cache directory
				if entry.IsDir() && strings.Contains(path, "/cache/") {
					// make sure the dir is exist
					if _, err = ioutil.ReadDir(path + entry.Name()); err != nil {
						log.Warnf("ioutil.ReadDir(%+v + %+v) is fail", path, entry.Name())
						continue
					}
					size, err := DirSize(path + entry.Name())
					if err != nil {
						log.Warnf("DirSize(%+v%+v) is fail", path, entry.Name())
						continue
					}
					m[entry.Name()] = size / MB
					sum[entry.Name()] += size / MB
				}
				// file under unsealed/sealed directory
				if !entry.IsDir() && (strings.Contains(path, "/unsealed/") ||
					strings.Contains(path, "/sealed/")) {
					size, err := DirSize(path + entry.Name())
					if err != nil {
						log.Warnf("DirSize(%+v%+v) is fail", path, entry.Name())
						continue
					}
					m[entry.Name()] = size / MB
					sum[entry.Name()] += size / MB
				}
			}
		}
		return m
	}

	return nil
}

func (l *LocalWorker) TaskDisable(ctx context.Context, tt sealtasks.TaskType) error {
	l.taskLk.Lock()
	defer l.taskLk.Unlock()

	delete(l.acceptTasks, tt)
	return nil
}

func (l *LocalWorker) TaskEnable(ctx context.Context, tt sealtasks.TaskType) error {
	l.taskLk.Lock()
	defer l.taskLk.Unlock()

	l.acceptTasks[tt] = struct{}{}
	return nil
}

func (l *LocalWorker) Paths(ctx context.Context) ([]storiface.StoragePath, error) {
	return l.localStore.Local(ctx)
}

func (l *LocalWorker) memInfo() (memPhysical, memUsed, memSwap, memSwapUsed uint64, err error) {
	h, err := sysinfo.Host()
	if err != nil {
		return 0, 0, 0, 0, err
	}

	mem, err := h.Memory()
	if err != nil {
		return 0, 0, 0, 0, err
	}
	memPhysical = mem.Total
	// mem.Available is memory available without swapping, it is more relevant for this calculation
	memUsed = mem.Total - mem.Available
	memSwap = mem.VirtualTotal
	memSwapUsed = mem.VirtualUsed

	if cgMemMax, cgMemUsed, cgSwapMax, cgSwapUsed, err := cgroupV1Mem(); err == nil {
		if cgMemMax > 0 && cgMemMax < memPhysical {
			memPhysical = cgMemMax
			memUsed = cgMemUsed
		}
		if cgSwapMax > 0 && cgSwapMax < memSwap {
			memSwap = cgSwapMax
			memSwapUsed = cgSwapUsed
		}
	}

	if cgMemMax, cgMemUsed, cgSwapMax, cgSwapUsed, err := cgroupV2Mem(); err == nil {
		if cgMemMax > 0 && cgMemMax < memPhysical {
			memPhysical = cgMemMax
			memUsed = cgMemUsed
		}
		if cgSwapMax > 0 && cgSwapMax < memSwap {
			memSwap = cgSwapMax
			memSwapUsed = cgSwapUsed
		}
	}

	if l.noSwap {
		memSwap = 0
		memSwapUsed = 0
	}

	return memPhysical, memUsed, memSwap, memSwapUsed, nil
}

func (l *LocalWorker) Info(context.Context) (storiface.WorkerInfo, error) {
	hostname, err := os.Hostname() // TODO: allow overriding from config
	if err != nil {
		panic(err)
	}

	gpus, err := ffi.GetGPUDevices()
	if err != nil {
		log.Errorf("getting gpu devices failed: %+v", err)
	}

	memPhysical, memUsed, memSwap, memSwapUsed, err := l.memInfo()
	if err != nil {
		return storiface.WorkerInfo{}, xerrors.Errorf("getting memory info: %w", err)
	}

	resEnv, err := storiface.ParseResourceEnv(func(key, def string) (string, bool) {
		return l.envLookup(key)
	})
	if err != nil {
		return storiface.WorkerInfo{}, xerrors.Errorf("interpreting resource env vars: %w", err)
	}

	return storiface.WorkerInfo{
		Hostname:        hostname,
		IgnoreResources: l.ignoreResources,
		Resources: storiface.WorkerResources{
			MemPhysical: memPhysical,
			MemUsed:     memUsed,
			MemSwap:     memSwap,
			MemSwapUsed: memSwapUsed,
			CPUs:        uint64(runtime.NumCPU()),
			GPUs:        gpus,
			Resources:   resEnv,
		},
	}, nil
}

func (l *LocalWorker) Session(ctx context.Context) (uuid.UUID, error) {
	if atomic.LoadInt64(&l.testDisable) == 1 {
		return uuid.UUID{}, xerrors.Errorf("disabled")
	}

	select {
	case <-l.closing:
		return ClosedWorkerID, nil
	default:
		return l.session, nil
	}
}

func (l *LocalWorker) Close() error {
	close(l.closing)
	return nil
}

// WaitQuiet blocks as long as there are tasks running
func (l *LocalWorker) WaitQuiet() {
	l.running.Wait()
}

type wctx struct {
	vals    context.Context
	closing chan struct{}
}

func (w *wctx) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (w *wctx) Done() <-chan struct{} {
	return w.closing
}

func (w *wctx) Err() error {
	select {
	case <-w.closing:
		return context.Canceled
	default:
		return nil
	}
}

func (w *wctx) Value(key interface{}) interface{} {
	return w.vals.Value(key)
}

var _ context.Context = &wctx{}

var _ Worker = &LocalWorker{}
