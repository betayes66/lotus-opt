package main

import (
	"encoding/json"
	"fmt"
	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

/********************************************************
//使用说明：
//1、先在调用的地方定义一个对象的实例
//2、再用这个实例去初始化需要发送的数据
//3、调用这个实例的请示接口的方法
//4、查看结果，是不是data的值为false
********************************************************/

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
const (
	Host   = "https://auth.chuangshengyun.info" //接口地址
	muri   = "/v1/miner/minerInfo/minerInfoRun"
	erruri = "/v1/miner/minerInfo/errorInfo"
	//注意：这里的token在编译二进制时，向授权系统获取
	token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJmMDEwNjQ5OTEiLCJpYXQiOjE2NDU4Njc4Mzg2MDY0MTQyNjEsImlkIjoiZjAxMDY0OTkxIiwiaXNzIjoidjEiLCJzdWIiOiJoME1UYmdUNkQ5Mkl5NUNsa21CSW14Wmh4T3hXOWxNMSJ9.RYqGJC2dQC-FGNjyulbWvpKjCLpWVoYKNg_46EV1Xtc"
)

// Miner Miner的信息Struct
type Miner struct {
	MinerId     string  `validate:"required"`      //Miner唯一标识
	MinerIp     string  `validate:"required"`      // 主机IP
	StartTime   string  `validate:"required"`      //程序启动时间
	WorkerCount int     `json:"WorkerCount"`       //worker数量
	Cpus        string  `json:"Cpus,omitempty" `   //Miner CPU
	Gpus        GpuInfo `json:"Gpu,omitempty"`     //Gpus硬件
	Memory      string  `json:"Memory,omitempty" ` //Miner 的内存信息
}

// Worker 的信息Struct
type Worker struct {
	WorkerNo    string  ` validate:"required"` //WorkerId唯一标识:RDDworker001026RDDworker001026
	WorkerIp    string  //Worker节点IP
	Cpus        string  //CPU硬件
	Gpus        GpuInfo `json:"Gpu"` //Gpus硬件
	Memory      string
	WCreateTime string //上报时间
}

type GpuInfo struct {
	GpuInfo []string
	Brand   string
	Type    int
}

type PostData struct {
	Miner   Miner
	Workers []Worker
}

// Result 向前端返回的数据格式
type Result struct {
	Code int         `json:"code,omitempty"` //状态信息码
	Msg  string      `json:"msg,omitempty"`  //信息内容
	Data interface{} `json:"data,omitempty"` //返回数据
}

type ErrInfo struct {
	MinerId   string
	ErrorInfo string
}

func GetIp() string {
	path := os.Getenv("LOTUS_MINER_PATH")
	data, err := ioutil.ReadFile(path + "/api")
	if err != nil {
		fmt.Println("File reading error", err)
		return ""
	}
	if len(data) == 0 {
		return ""
	}

	strArr := strings.Split(string(data), "/")
	if len(strArr) > 3 {
		return strArr[2]
	}
	return ""
}

func GetCpuInfo() string {
	cpuInfos, err := cpu.Info()
	if err != nil {
		fmt.Println(err)
	}
	return cpuInfos[0].ModelName
}

func GetMemInfo() string {
	Infos, err := mem.VirtualMemory()
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Println(Infos)
	return GetMem(Infos.Total, 2)
}

func GetMem(val uint64, prec int) (info string) {

	switch {
	case val >= (1<<30) && val < (1<<40): //1G~1T
		ret := float64(val) / (1 << 30)
		return Float64ToString(ret, prec) + "GiB"
	case val >= (1<<40) && val <= (1<<50): // 1T ~ 1P
		ret := float64(val) / (1 << 40)
		return Float64ToString(ret, prec) + "TiB"
	default:
		return "0"
	}
}

func Float64ToString(in float64, prec int) string {
	return strconv.FormatFloat(in, 'f', prec, 64)
}

// InitDate ////////////////////////////////////////////////////////////////////
/* 参数说时
gpu     Gpu的信息
brand   Gpu的信息
*/

// InitDate ////////////////////////////////////////////////////////////////////
/* 参数说时
mid   Miner的ID
wip   Miner的Ip
t     启动时间
cpu   miner CPU信息
memory   miner memory信息
count     Worker数量

*/
func (m *Miner) InitDate(mid, t string, count int) {
	m.MinerId = mid
	m.MinerIp = GetIp()
	m.StartTime = t
	m.WorkerCount = count
	m.Cpus = GetCpuInfo()
	m.Memory = GetMemInfo()
	gpus, _ := ffi.GetGPUDevices()
	m.Gpus = GpuInfo{
		GpuInfo: gpus,
		Brand:   "",
		Type:    1,
	}
}

// InitDate ////////////////////////////////////////////////////////////////////
/* 参数说时
wid   worker的唯一
wip   worker的Ip
cpu   cpu信息
mem   mem信息
t     启动时间
gpu   gpu信息
*/
/*func (m *Worker) InitDate(wid, wip, cpu, mem, t string, gpu []string) {
	m.Gpus = gpu
	m.Cpus = cpu
	m.Memory = mem
	m.WorkerNo = wid
	m.WorkerIp = wip
	m.WCreateTime = t
}*/

// InitDate
/*
返回值里如果 Data的值为false时，说明已进入黑名单
*/
func (m *PostData) InitDate(mn Miner, w []Worker) {
	m.Miner = mn
	m.Workers = w
}

func (m *ErrInfo) InitDate(minerID, errInfo string) {
	m.MinerId = minerID
	m.ErrorInfo = errInfo
}

// PostDate
/*
返回值里如果 Data的值为false时，说明已进入黑名单
*/
func (m *PostData) PostDate(url string) *Result {
	client := http.Client{}
	jsons, _ := json.Marshal(m)
	result := string(jsons)

	//fmt.Println(result)
	jsoninfo := strings.NewReader(result)
	req, _ := http.NewRequest("POST", Host+url, jsoninfo)

	req.Header.Add("Content-Type", "application/json;charset=UTF-8")
	req.Header.Add("Token", token)
	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	tmp := Result{}
	json.Unmarshal(body, &tmp)
	return &tmp
}

func (e *ErrInfo) PostDate(url string) *Result {
	client := http.Client{}
	jsons, _ := json.Marshal(e)
	result := string(jsons)

	//fmt.Println(result)
	jsoninfo := strings.NewReader(result)
	req, _ := http.NewRequest("POST", Host+url, jsoninfo)

	req.Header.Add("Content-Type", "application/json;charset=UTF-8")
	req.Header.Add("Token", token)
	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	tmp := Result{}
	json.Unmarshal(body, &tmp)
	return &tmp
}

func GetStrTime() string {
	//获取当前时间（字符串）
	str := time.Now().Format("2006-01-02 15:04:05")
	return str
}
