package main

import (
	"fmt"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
)

var autoPledgeCmd = &cli.Command{
	Name:  "auto-pledge",
	Usage: "auto pledge sector",
	Subcommands: []*cli.Command{
		pledgeStartCmd,
		pledgeStatusCmd,
		pledgeStopCmd,
	},
}

var pledgeStartCmd = &cli.Command{
	Name:  "start",
	Usage: "start auto-pledge sector",
	Action: func(cctx *cli.Context) error {
		mApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		return mApi.PledgeStart(ctx)
	},
}

var pledgeStatusCmd = &cli.Command{
	Name:  "status",
	Usage: "view auto-pledge status",
	Action: func(cctx *cli.Context) error {
		mApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		if status, err := mApi.PledgeStatus(ctx); err != nil {
			log.Error(err)
			return err
		} else if status != 0 {
			fmt.Println("Running")
		} else {
			fmt.Println("Not Running")
		}
		return nil
	},
}

var pledgeStopCmd = &cli.Command{
	Name:  "stop",
	Usage: "stop auto-pledge and exit",
	Action: func(cctx *cli.Context) error {
		mApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		return mApi.PledgeStop(ctx)
	},
}
