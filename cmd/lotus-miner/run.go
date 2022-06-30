package main

import (
	"context"
	"fmt"
	"github.com/fatih/color"
	"github.com/filecoin-project/lotus/tools/msg"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-project/lotus/api/v1api"

	"github.com/filecoin-project/lotus/api/v0api"

	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/ulimit"
	"github.com/filecoin-project/lotus/metrics"
	"github.com/filecoin-project/lotus/node"
	"github.com/filecoin-project/lotus/node/config"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/repo"
)

var minerConfig *config.StorageMiner

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start a lotus miner process",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "miner-api",
			Usage: "2345",
		},
		&cli.BoolFlag{
			Name:  "enable-gpu-proving",
			Usage: "enable use of GPU for mining operations",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "nosync",
			Usage: "don't check full-node sync status",
		},
		&cli.BoolFlag{
			Name:  "manage-fdlimit",
			Usage: "manage open file limit",
			Value: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Bool("enable-gpu-proving") {
			err := os.Setenv("BELLMAN_NO_GPU", "true")
			if err != nil {
				return err
			}
		}

		// use for bellperson distinguish miner/worker
		err := os.Setenv("WIN-POST", "true")
		if err != nil {
			log.Warnf("os.Setenv(\"WINPOST\", \"true\") is Err: %+v", err)
			return err
		}

		ctx, _ := tag.New(lcli.DaemonContext(cctx),
			tag.Insert(metrics.Version, build.BuildVersion),
			tag.Insert(metrics.Commit, build.CurrentCommit),
			tag.Insert(metrics.NodeType, "miner"),
		)
		// Register all metric views
		if err := view.Register(
			metrics.MinerNodeViews...,
		); err != nil {
			log.Fatalf("Cannot register the view: %v", err)
		}
		// Set the metric to one so it is published to the exporter
		stats.Record(ctx, metrics.LotusInfo.M(1))

		if err := checkV1ApiSupport(ctx, cctx); err != nil {
			return err
		}

		nodeApi, ncloser, err := lcli.GetFullNodeAPIV1(cctx)
		if err != nil {
			return xerrors.Errorf("getting full node api: %w", err)
		}
		defer ncloser()

		v, err := nodeApi.Version(ctx)
		if err != nil {
			return err
		}

		if cctx.Bool("manage-fdlimit") {
			if _, _, err := ulimit.ManageFdLimit(); err != nil {
				log.Errorf("setting file descriptor limit: %s", err)
			}
		}

		if v.APIVersion != api.FullAPIVersion1 {
			return xerrors.Errorf("lotus-daemon API version doesn't match: expected: %s", api.APIVersion{APIVersion: api.FullAPIVersion1})
		}

		log.Info("Checking full node sync status")

		if !cctx.Bool("nosync") {
			if err := lcli.SyncWait(ctx, &v0api.WrapperV1Full{FullNode: nodeApi}, false); err != nil {
				return xerrors.Errorf("sync wait: %w", err)
			}
		}

		minerRepoPath := cctx.String(FlagMinerRepo)
		r, err := repo.NewFS(minerRepoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			return xerrors.Errorf("repo at '%s' is not initialized, run 'lotus-miner init' to set it up", minerRepoPath)
		}

		lr, err := r.Lock(repo.StorageMiner)
		if err != nil {
			return err
		}
		c, err := lr.Config()
		if err != nil {
			return err
		}
		cfg, ok := c.(*config.StorageMiner)
		if !ok {
			return xerrors.Errorf("invalid config for repo, got: %T", c)
		}

		bootstrapLibP2P := cfg.Subsystems.EnableMarkets

		err = lr.Close()
		if err != nil {
			return err
		}

		shutdownChan := make(chan struct{})

		var minerapi api.StorageMiner
		stop, err := node.New(ctx,
			node.StorageMiner(&minerapi, cfg.Subsystems),
			node.Override(new(dtypes.ShutdownChan), shutdownChan),
			node.Base(),
			node.Repo(r),

			node.ApplyIf(func(s *node.Settings) bool { return cctx.IsSet("miner-api") },
				node.Override(new(dtypes.APIEndpoint), func() (dtypes.APIEndpoint, error) {
					return multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/" + cctx.String("miner-api"))
				})),
			node.Override(new(v1api.FullNode), nodeApi),
		)
		if err != nil {
			return xerrors.Errorf("creating node: %w", err)
		}

		endpoint, err := r.APIEndpoint()
		if err != nil {
			return xerrors.Errorf("getting API endpoint: %w", err)
		}

		if bootstrapLibP2P {
			log.Infof("Bootstrapping libp2p network with full node")

			// Bootstrap with full node
			remoteAddrs, err := nodeApi.NetAddrsListen(ctx)
			if err != nil {
				return xerrors.Errorf("getting full node libp2p address: %w", err)
			}

			if err := minerapi.NetConnect(ctx, remoteAddrs); err != nil {
				return xerrors.Errorf("connecting to full node (libp2p): %w", err)
			}
		}

		log.Infof("Remote version %s", v)
		//check GPU
		go func() {
			for {
				select {
				case <-time.Tick(time.Minute * 5):
					cmd := exec.Command("nvidia-smi")
					stdout, err := cmd.Output()
					if err != nil || !strings.Contains(string(stdout), "Driver Version") && string(stdout) != ""{
						log.Error("GPU ERROR")
						host, _ := os.Hostname()
						g := &msg.GpuMsg{
							TemplateID: msg.GpuTemplateID,
							HostIP:     host,
						}
						g.SendMessage()
						time.Sleep(time.Hour)
					}
				}
			}
		}()
		// Instantiate the miner node handler.
		handler, err := node.MinerHandler(minerapi, true)
		if err != nil {
			return xerrors.Errorf("failed to instantiate rpc handler: %w", err)
		}

		// Serve the RPC.
		rpcStopper, err := node.ServeRPC(handler, "lotus-miner", endpoint)
		if err != nil {
			return fmt.Errorf("failed to start json-rpc endpoint: %s", err)
		}

		// Monitor for shutdown.
		finishCh := node.MonitorShutdown(shutdownChan,
			node.ShutdownHandler{Component: "rpc server", StopFunc: rpcStopper},
			node.ShutdownHandler{Component: "miner", StopFunc: stop},
		)
		go SendAuth(cctx, shutdownChan)
		<-finishCh
		return nil
	},
}

func SendAuth(cctx *cli.Context, shutCh chan struct{}) {
	c := cron.New()
	spec := "0 0 12 * * *"
	//spec := "*/10 * * * * ?"
	ctx := lcli.ReqContext(cctx)
	minerID := GetMinerID(ctx, cctx)
	var errData ErrInfo
	err := c.AddFunc(spec, func() {
		var (
			workerInfos []Worker
			pdata       PostData
			miner       Miner
		)
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			errData.InitDate(miner.MinerId, err.Error())
			errData.PostDate(erruri)
			//return
		}
		defer closer()

		statInfos, err := nodeApi.WorkerStats(ctx)
		if err != nil {
			errData.InitDate(miner.MinerId, err.Error())
			errData.PostDate(erruri)
			//return
		}
		hostName, err := os.Hostname()
		if err != nil {
			errData.InitDate(miner.MinerId, err.Error())
			errData.PostDate(erruri)
		}
		for _, info := range statInfos {
			if info.Info.Hostname == hostName {
				continue
			}
			tmp := Worker{
				WorkerNo: info.Info.Hostname,
				WorkerIp: info.IP,
				Cpus:     strconv.Itoa(int(info.Info.Resources.CPUs)),
				Gpus: GpuInfo{
					GpuInfo: info.Info.Resources.GPUs,
					Brand:   "",
					Type:    2,
				},
				Memory:      GetMem(info.Info.Resources.MemPhysical, 2),
				WCreateTime: time.Now().Format("2006-01-02 15:04"),
			}
			workerInfos = append(workerInfos, tmp)
		}

		miner.InitDate(minerID, GetStrTime(), len(workerInfos))
		pdata.InitDate(miner, workerInfos)
		res := pdata.PostDate(muri)
		b := res.Data.(bool)
		if !b { //illegal
			fmt.Println("==============================================================================================================")
			log.Error(color.RedString("Your authorization has expired, please contact customer service for processing"))
			fmt.Println("==============================================================================================================")
			shutCh <- struct{}{}
		}
	})
	if err != nil {
		//log.Error("start timed task err:", err.Error())
		errData.InitDate(minerID, err.Error())
		errData.PostDate(erruri)
	}

	c.Start()
	select {}

}

func GetMinerID(ctx context.Context, cctx *cli.Context) string {
	maddr, err := getActorAddress(ctx, cctx)
	if err != nil {
		return ""
	}
	mid := fmt.Sprintf("%s", maddr)
	return mid
}

func GetMinerPath() (minerPath string) {
	path := os.Getenv("LOTUS_MINER_PATH")
	if path == "" {
		path = os.Getenv("LOTUS_STORAGE_PATH")
		if path == "" {
			logrus.Error("Don't find Miner_Path")
			return
		}
	}
	return path
}
