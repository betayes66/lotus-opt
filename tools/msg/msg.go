package msg

import (
	"fmt"
	"github.com/filecoin-project/lotus/node/config"
	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
	"os"
	"strconv"

	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	sms "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/sms/v20210111"
)

const (
	MsgSecretID   = "AKIDiHjxwhEXdWDguufBTWMyLTSGwRmtlOmw"
	MsgSecretKey  = "zjEWaQ2QaVBcomW1HJ6ex1rTqysY0DE2"
	BalTemplateID = "1458238"
	GpuTemplateID = "1458236"
)

var (
	DefaultPhoneNum = []string{"+8618585817510", "+8615086004142"}
)

type Msg interface {
	SendMessage()
}

type BalanceMsg struct {
	TemplateID     string
	HostIP         string
	Balance, Limit float64
}

type GpuMsg struct {
	TemplateID string
	HostIP     string
}

func (b *BalanceMsg) SendMessage() {
	Send(b)
}
func (g *GpuMsg) SendMessage() {
	Send(g)
}
func Send(obj interface{}) {

	credential := common.NewCredential(
		MsgSecretID,
		MsgSecretKey,
	)
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = "sms.tencentcloudapi.com"
	client, _ := sms.NewClient(credential, "ap-guangzhou", cpf)
	request := sms.NewSendSmsRequest()
	request.SmsSdkAppId = common.StringPtr("1400531012")
	request.SignName = common.StringPtr("贵州贝塔科技")
	//request.TemplateId = common.StringPtr(templateID)

	switch value := obj.(type) {
	case *BalanceMsg:
		phoneNum, err := GetPhoneConfig()
		if err != nil || len(phoneNum) == 0 {
			logrus.Error("Get Phone Num err: ", err)
			request.PhoneNumberSet = common.StringPtrs(DefaultPhoneNum)
		} else {
			request.PhoneNumberSet = common.StringPtrs(phoneNum)
		}
		request.TemplateId = common.StringPtr(value.TemplateID)
		tmpBalance, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", value.Balance), 64)
		request.TemplateParamSet = common.StringPtrs([]string{value.HostIP, strconv.FormatFloat(tmpBalance, 'f', -1, 64), strconv.FormatFloat(value.Limit, 'f', -1, 64)})
	case *GpuMsg:
		request.TemplateId = common.StringPtr(value.TemplateID)
		request.PhoneNumberSet = common.StringPtrs(DefaultPhoneNum)
		request.TemplateParamSet = common.StringPtrs([]string{value.HostIP})
	default:
		fmt.Println("Current Template ID not found ")
		return
	}

	_, err := client.SendSms(request)
	//response, err := client.SendSms(request)
	if _, ok := err.(*errors.TencentCloudSDKError); ok {
		fmt.Printf("An API error has returned: %s\n", err)
		return
	}
	if err != nil {
		fmt.Printf("send message err: %s\n", err)
	}
	//fmt.Printf("msg:%s", response.ToJsonString())
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

func GetPhoneConfig() (phones []string, err error) {
	minerRepoPath := GetMinerPath()
	c, err := config.FromFile(minerRepoPath+"/config.toml", config.DefaultStorageMiner())
	if err != nil {
		return nil, err
	}
	cfg, ok := c.(*config.StorageMiner)
	if !ok {
		return nil, xerrors.Errorf("invalid config for repo, got: %T", c)
	}
	return cfg.ListenPhone.PhoneNum, nil

}
