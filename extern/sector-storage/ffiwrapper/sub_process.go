package ffiwrapper

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper/basicfs"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/gwaylib/errors"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"io"
	"net"
	"strings"
	"time"
)

type P2TaskIn struct {
	Sector    storage.SectorRef
	Phase1Out storage.PreCommit1Out
}

func (w *P2TaskIn) TaskName() string {
	return fmt.Sprintf("s-t0%d-%d", w.Sector.ID.Miner, w.Sector.ID.Number)
}

type ExecP2TaskOut struct {
	Data storage.SectorCids
	Err  string
}

func ReadUnixConn(conn net.Conn) ([]byte, error) {
	result := []byte{}
	bufLen := 32 * 1024
	buf := make([]byte, bufLen)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Error(err)
			if err != io.EOF {
				return nil, errors.As(err)
			}
		}
		result = append(result, buf[:n]...)
		if n < bufLen {
			break
		}
	}
	return result, nil
}

var P2Cmd = &cli.Command{
	Name:  "precommit2",
	Usage: "run precommit2 in process",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "worker-repo",
			EnvVars: []string{"LOTUS_WORKER_PATH", "WORKER_PATH"},
			Value:   "~/.lotusworker",
		},
		&cli.StringFlag{
			Name: "name", // just for process debug
		},
		&cli.StringFlag{
			Name: "addr", // listen address
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		unixAddr := net.UnixAddr{Name: cctx.String("addr"), Net: "unix"}
		// unix listen
		ln, err := net.ListenUnix("unix", &unixAddr)
		if err != nil {
			panic(err)
		}

		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		argIn, err := ReadUnixConn(conn)
		if err != nil {
			panic(err)
		}

		resp := ExecP2TaskOut{}
		workerRepo, err := homedir.Expand(cctx.String("worker-repo"))
		if err != nil {
			resp.Err = errors.As(err).Error()
			return nil
		}

		task := P2TaskIn{}
		if err := json.Unmarshal(argIn, &task); err != nil {
			resp.Err = errors.As(err).Error()
			return nil
		}

		workerSealer, err := New(&basicfs.Provider{
			Root: workerRepo,
		})
		if err != nil {
			resp.Err = errors.As(err).Error()
			return nil
		}

		out, err := workerSealer.SealPreCommit2(ctx, task.Sector, task.Phase1Out)
		if err != nil {
			resp.Err = strings.Split(strings.TrimSpace(strings.Split(errors.As(err).Error(), "Stack backtrace")[0] ), "\\n")[0] + " }"
		}
		resp.Data = out
		result, err := json.Marshal(&resp)
		if err != nil {
			log.Error(err)
		}

		if _, err := conn.Write(result); err != nil {
			log.Error(err)
		}
		ch := make(chan int)
		select {
		case <-ch:
		case <-time.After(time.Second * 30):
			log.Info("sub process seal p2 timeout 30s")
		}
		return nil
	},
}
