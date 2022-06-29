package sealing

import (
	"context"
	"errors"
	"github.com/filecoin-project/lotus/chain/types"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	//"github.com/filecoin-project/lotus/tools/msg"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	pledgeExit    = make(chan bool, 1)
	pledgeRunning = false
	pledgeSync    = sync.Mutex{}

	balanceLimit = acquireBalanceFromEnv()
)

func acquireBalanceFromEnv() uint64 {
	strFromEnv := os.Getenv("BALANCE_LIMIT")
	convLimitBalance, err := strconv.Atoi(strFromEnv)
	if err == nil {
		return uint64(convLimitBalance)
	}
	return uint64(0)
}

func packingNum() int32 {
	pn := os.Getenv("PACKING_NUM")
	convPn, err := strconv.Atoi(pn)
	if err == nil && convPn > 1 {
		return int32(convPn - 1)
	}
	return int32(0)
}

var count int

func (m *Sealing) checkBalance(limitBalance uint64) bool {

	// Sector size
	tok, _, err := m.Api.ChainHead(context.TODO())
	if err != nil {
		log.Error(err)
		return false
	}
	mi, err := m.Api.StateMinerInfo(context.TODO(), m.maddr, tok)
	if err != nil {
		log.Error(err)
		return false
	}
	workerBalance, err := m.Api.WalletBalance(context.TODO(), mi.Worker)
	if err != nil {
		log.Error(err)
		return false
	}
	available := types.FIL(workerBalance).Unitless()
	availableBalance, _ := strconv.ParseFloat(available, 64)
	if uint64(availableBalance) < limitBalance {
		//if (count%15) == 0 || count == 0 {
		//	host, _ := os.Hostname()
		//	b := &msg.BalanceMsg{
		//		TemplateID: msg.BalTemplateID,
		//		HostIP:     host,
		//		Balance:    availableBalance,
		//		Limit:      float64(limitBalance),
		//	}
		//	b.SendMessage()
		//}
		count++
		return false
	}
	return true
}

func (m *Sealing) PledgeStart() error {
	pledgeSync.Lock()
	defer pledgeSync.Unlock()

	if pledgeRunning {
		return errors.New("auto pledge-sector is already running")
	}
	pledgeRunning = true

	go func() {
		defer func() {
			pledgeRunning = false
			// auto recover for panic
			if err := recover(); err != nil {
				log.Error(err)
				m.PledgeStart()
			}
		}()

		for {
			pledgeRunning = true
			select {
			case <-pledgeExit:
				return
			default:
				//if sectorstorage.PledgeWait != 0 {
				if sectorstorage.PledgeWait > packingNum() {
					time.Sleep(time.Second * 3)
					continue
				}
				_, err := m.PledgeSector(context.TODO())
				time.Sleep(time.Second)
				if err != nil {
					log.Error(err)
					continue
				}
			}
		}

	}()

	return nil
}

func (m *Sealing) PledgeStatus() (int, error) {
	pledgeSync.Lock()
	defer pledgeSync.Unlock()
	if !pledgeRunning {
		return 0, nil
	}
	return 1, nil
}

func (m *Sealing) PledgeStop() error {
	pledgeSync.Lock()
	defer pledgeSync.Unlock()
	if !pledgeRunning {
		return errors.New("auto pledge-sector not running")
	}
	if len(pledgeExit) > 0 {
		return errors.New("exiting")
	}
	pledgeExit <- true
	log.Info("Pledge garbage exit")
	return nil
}
