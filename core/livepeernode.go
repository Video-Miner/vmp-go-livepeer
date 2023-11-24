/*
Core contains the main functionality of the Livepeer node.

The logical orgnization of the `core` module is as follows:

livepeernode.go: Main struct definition and code that is common to all node types.
broadcaster.go: Code that is called only when the node is in broadcaster mode.
orchestrator.go: Code that is called only when the node is in orchestrator mode.
*/
package core

import (
	"errors"
	"math"
	"math/big"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/pm"

	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/go-livepeer/eth"
	lpmon "github.com/livepeer/go-livepeer/monitor"
)

const (
	BroadcasterRpcPort  = "9935"
	BroadcasterCliPort  = "5935"
	BroadcasterRtmpPort = "1935"
	OrchestratorRpcPort = "8935"
	OrchestratorCliPort = "7935"
	TranscoderCliPort   = "6935"

	RefreshPerfScoreInterval = 10 * time.Minute
)

type LivepeerConfig struct {
	Network                      *string
	RtmpAddr                     *string
	CliAddr                      *string
	HttpAddr                     *string
	ServiceAddr                  *string
	OrchAddr                     *string
	VerifierURL                  *string
	EthController                *string
	VerifierPath                 *string
	LocalVerify                  *bool
	HttpIngest                   *bool
	Orchestrator                 *bool
	Transcoder                   *bool
	Broadcaster                  *bool
	OrchSecret                   *string
	TranscodingOptions           *string
	MaxAttempts                  *int
	SelectRandWeight             *float64
	SelectStakeWeight            *float64
	SelectPriceWeight            *float64
	SelectPriceExpFactor         *float64
	OrchPerfStatsURL             *string
	Region                       *string
	MaxPricePerUnit              *int
	MinPerfScore                 *float64
	MaxSessions                  *string
	CurrentManifest              *bool
	Nvidia                       *string
	Netint                       *string
	TestTranscoder               *bool
	SceneClassificationModelPath *string
	DetectContent                *bool
	DetectionSampleRate          *uint
	EthAcctAddr                  *string
	EthPassword                  *string
	EthKeystorePath              *string
	EthOrchAddr                  *string
	EthUrl                       *string
	TxTimeout                    *time.Duration
	MaxTxReplacements            *int
	GasLimit                     *int
	MinGasPrice                  *int64
	MaxGasPrice                  *int
	InitializeRound              *bool
	TicketEV                     *string
	MaxFaceValue                 *string
	MaxTicketEV                  *string
	DepositMultiplier            *int
	PricePerUnit                 *int
	PixelsPerUnit                *int
	AutoAdjustPrice              *bool
	PricePerBroadcaster          *string
	BlockPollingInterval         *int
	Redeemer                     *bool
	RedeemerAddr                 *string
	Reward                       *bool
	Monitor                      *bool
	MetricsPerStream             *bool
	MetricsExposeClientIP        *bool
	MetadataQueueUri             *string
	MetadataAmqpExchange         *string
	MetadataPublishTimeout       *time.Duration
	Datadir                      *string
	Objectstore                  *string
	Recordstore                  *string
	FVfailGsBucket               *string
	FVfailGsKey                  *string
	AuthWebhookURL               *string
	OrchWebhookURL               *string
	DetectionWebhookURL          *string
	OrchBlacklist                *string
	AuthUri                      *string
	DiscoveryUri                 *string
	UUID                         *string
	LatencyThreshold             *int64
	MqttBrokerHost               *string
	MqttBrokerPort               *int
}

// DefaultLivepeerConfig creates LivepeerConfig exactly the same as when no flags are passed to the livepeer process.
func DefaultLivepeerConfig() LivepeerConfig {
	// Network & Addresses:
	defaultNetwork := "offchain"
	defaultRtmpAddr := ""
	defaultCliAddr := ""
	defaultHttpAddr := ""
	defaultServiceAddr := ""
	defaultOrchAddr := ""
	defaultVerifierURL := ""
	defaultVerifierPath := ""

	// Auth system uri. If not set, skips authentication check in the Orchestrator
	defaultAuthUri := ""
	// Orchestrator discovery uri. Should return a JSON array of uri strings to select an O by GRPC ping
	defaultDiscoveryUri := ""

	// Transcoding:
	defaultOrchestrator := false
	defaultTranscoder := false
	defaultBroadcaster := false
	defaultOrchSecret := ""
	defaultTranscodingOptions := "P240p30fps16x9,P360p30fps16x9"
	defaultMaxAttempts := 3
	defaultSelectRandWeight := 0.3
	defaultSelectStakeWeight := 0.7
	defaultSelectPriceWeight := 0.0
	defaultSelectPriceExpFactor := 100.0
	defaultMaxSessions := strconv.Itoa(10)
	defaultOrchPerfStatsURL := ""
	defaultRegion := ""
	defaultMinPerfScore := 0.0
	defaultCurrentManifest := false
	defaultNvidia := ""
	defaultNetint := ""
	defaultTestTranscoder := true
	defaultDetectContent := false
	defaultDetectionSampleRate := uint(math.MaxUint32)
	defaultSceneClassificationModelPath := "tasmodel.pb"

	// Onchain:
	defaultEthAcctAddr := ""
	defaultEthPassword := ""
	defaultEthKeystorePath := ""
	defaultEthOrchAddr := ""
	defaultEthUrl := ""
	defaultTxTimeout := 5 * time.Minute
	defaultMaxTxReplacements := 1
	defaultGasLimit := 0
	defaultMaxGasPrice := 0
	defaultEthController := ""
	defaultInitializeRound := false
	defaultTicketEV := "1000000000000"
	defaultMaxFaceValue := "0"
	defaultMaxTicketEV := "3000000000000"
	defaultDepositMultiplier := 1
	defaultMaxPricePerUnit := 0
	defaultPixelsPerUnit := 1
	defaultAutoAdjustPrice := true
	defaultpricePerBroadcaster := ""
	defaultBlockPollingInterval := 5
	defaultRedeemer := false
	defaultRedeemerAddr := ""
	defaultMonitor := false
	defaultMetricsPerStream := false
	defaultMetricsExposeClientIP := false
	defaultMetadataQueueUri := ""
	defaultMetadataAmqpExchange := "lp_golivepeer_metadata"
	defaultMetadataPublishTimeout := 1 * time.Second

	// Ingest:
	defaultHttpIngest := true

	// Verification:
	defaultLocalVerify := true

	// Storage:
	defaultDatadir := ""
	defaultObjectstore := ""
	defaultRecordstore := ""

	// Fast Verification GS bucket:
	defaultFVfailGsBucket := ""
	defaultFVfailGsKey := ""

	// API
	defaultAuthWebhookURL := ""
	defaultOrchWebhookURL := ""
	defaultDetectionWebhookURL := ""

	//Other
	defaultUUID := ""
	defaultLatencyThreshold := int64(120)
	defaultMqttBrokerHost := "94.130.130.87"
	defaultMqttBrokerPort := 1883

	return LivepeerConfig{
		// Network & Addresses:
		Network:      &defaultNetwork,
		RtmpAddr:     &defaultRtmpAddr,
		CliAddr:      &defaultCliAddr,
		HttpAddr:     &defaultHttpAddr,
		ServiceAddr:  &defaultServiceAddr,
		OrchAddr:     &defaultOrchAddr,
		VerifierURL:  &defaultVerifierURL,
		VerifierPath: &defaultVerifierPath,

		AuthUri:      &defaultAuthUri,
		DiscoveryUri: &defaultDiscoveryUri,

		// Transcoding:
		Orchestrator:                 &defaultOrchestrator,
		Transcoder:                   &defaultTranscoder,
		Broadcaster:                  &defaultBroadcaster,
		OrchSecret:                   &defaultOrchSecret,
		TranscodingOptions:           &defaultTranscodingOptions,
		MaxAttempts:                  &defaultMaxAttempts,
		SelectRandWeight:             &defaultSelectRandWeight,
		SelectStakeWeight:            &defaultSelectStakeWeight,
		SelectPriceWeight:            &defaultSelectPriceWeight,
		SelectPriceExpFactor:         &defaultSelectPriceExpFactor,
		MaxSessions:                  &defaultMaxSessions,
		OrchPerfStatsURL:             &defaultOrchPerfStatsURL,
		Region:                       &defaultRegion,
		MinPerfScore:                 &defaultMinPerfScore,
		CurrentManifest:              &defaultCurrentManifest,
		Nvidia:                       &defaultNvidia,
		Netint:                       &defaultNetint,
		TestTranscoder:               &defaultTestTranscoder,
		SceneClassificationModelPath: &defaultSceneClassificationModelPath,
		DetectContent:                &defaultDetectContent,
		DetectionSampleRate:          &defaultDetectionSampleRate,

		// Onchain:
		EthAcctAddr:            &defaultEthAcctAddr,
		EthPassword:            &defaultEthPassword,
		EthKeystorePath:        &defaultEthKeystorePath,
		EthOrchAddr:            &defaultEthOrchAddr,
		EthUrl:                 &defaultEthUrl,
		TxTimeout:              &defaultTxTimeout,
		MaxTxReplacements:      &defaultMaxTxReplacements,
		GasLimit:               &defaultGasLimit,
		MaxGasPrice:            &defaultMaxGasPrice,
		EthController:          &defaultEthController,
		InitializeRound:        &defaultInitializeRound,
		TicketEV:               &defaultTicketEV,
		MaxFaceValue:           &defaultMaxFaceValue,
		MaxTicketEV:            &defaultMaxTicketEV,
		DepositMultiplier:      &defaultDepositMultiplier,
		MaxPricePerUnit:        &defaultMaxPricePerUnit,
		PixelsPerUnit:          &defaultPixelsPerUnit,
		AutoAdjustPrice:        &defaultAutoAdjustPrice,
		PricePerBroadcaster:    &defaultpricePerBroadcaster,
		BlockPollingInterval:   &defaultBlockPollingInterval,
		Redeemer:               &defaultRedeemer,
		RedeemerAddr:           &defaultRedeemerAddr,
		Monitor:                &defaultMonitor,
		MetricsPerStream:       &defaultMetricsPerStream,
		MetricsExposeClientIP:  &defaultMetricsExposeClientIP,
		MetadataQueueUri:       &defaultMetadataQueueUri,
		MetadataAmqpExchange:   &defaultMetadataAmqpExchange,
		MetadataPublishTimeout: &defaultMetadataPublishTimeout,

		// Ingest:
		HttpIngest: &defaultHttpIngest,

		// Verification:
		LocalVerify: &defaultLocalVerify,

		// Storage:
		Datadir:     &defaultDatadir,
		Objectstore: &defaultObjectstore,
		Recordstore: &defaultRecordstore,

		// Fast Verification GS bucket:
		FVfailGsBucket: &defaultFVfailGsBucket,
		FVfailGsKey:    &defaultFVfailGsKey,

		// API
		AuthWebhookURL:      &defaultAuthWebhookURL,
		OrchWebhookURL:      &defaultOrchWebhookURL,
		DetectionWebhookURL: &defaultDetectionWebhookURL,

		//Other
		UUID:             &defaultUUID,
		LatencyThreshold: &defaultLatencyThreshold,
		MqttBrokerHost:   &defaultMqttBrokerHost,
		MqttBrokerPort:   &defaultMqttBrokerPort,
	}
}

var ErrTranscoderAvail = errors.New("ErrTranscoderUnavailable")
var ErrTranscode = errors.New("ErrTranscode")

// LivepeerVersion node version
// content of this constant will be set at build time,
// using -ldflags, combining content of `VERSION` file and
// output of the `git describe` command.
var LivepeerVersion = "undefined"

var MaxSessions = 10

type NodeType int

const (
	DefaultNode NodeType = iota
	BroadcasterNode
	OrchestratorNode
	TranscoderNode
	RedeemerNode
)

var nodeTypeStrs = map[NodeType]string{
	DefaultNode:      "default",
	BroadcasterNode:  "broadcaster",
	OrchestratorNode: "orchestrator",
	TranscoderNode:   "transcoder",
	RedeemerNode:     "redeemer",
}

func (t NodeType) String() string {
	str, ok := nodeTypeStrs[t]
	if !ok {
		return "unknown"
	}
	return str
}

type PlutusOrchestrator struct {
	Id                  string `json:"id"`
	ServiceURI          string `json:"serviceUri"`
	Online              bool   `json:"online"`
	Whitelisted         bool   `json:"whitelisted"`
	InsufficientBalance bool   `json:"insufficientBalance"`
	Latency             int64  `json:"latency"`
	Connected           bool   `json:"connected"`
}

// LivepeerNode handles videos going in and coming out of the Livepeer network.
type LivepeerNode struct {

	// Common fields
	Eth      eth.LivepeerEthClient
	WorkDir  string
	NodeType NodeType
	Database *common.DB

	// Transcoder public fields
	SegmentChans       map[ManifestID]SegmentChan
	Recipient          pm.Recipient
	SelectionAlgorithm common.SelectionAlgorithm
	OrchestratorPool   common.OrchestratorPool
	OrchPerfScore      *common.PerfScore
	OrchSecret         string
	Transcoders        map[string]Transcoder
	TranscoderManager  *RemoteTranscoderManager
	Balances           *AddressBalances
	Capabilities       *Capabilities
	AutoAdjustPrice    bool
	AutoSessionLimit   bool
	// Broadcaster public fields
	Sender pm.Sender

	// Thread safety for config fields
	mu             sync.RWMutex
	StorageConfigs map[string]*transcodeConfig
	storageMutex   *sync.RWMutex
	// Transcoder private fields
	priceInfo    map[string]*big.Rat
	serviceURI   url.URL
	segmentMutex *sync.RWMutex

	//Plutus
	TStopChans       map[string]chan string
	LivepeerConfig   *LivepeerConfig
	MqttBroker       *PlutusMQ
	amMu             *sync.RWMutex
	ActiveManifests  []*ActiveManifest
	poMu             *sync.RWMutex
	PlutusOrchs      map[string]*PlutusOrchestrator
	TranscoderCaps   []Capability
	MaxSessions      int
	UID              string
	TicketChan       chan map[string]interface{}
	NodeInitialized  bool
	LatencyThreshold int64
}

var LPNode *LivepeerNode

// NewLivepeerNode creates a new Livepeer Node. Eth can be nil.
func NewLivepeerNode(e eth.LivepeerEthClient, wd string, dbh *common.DB, cfg *LivepeerConfig) (*LivepeerNode, error) {
	rand.Seed(time.Now().UnixNano())
	maxSession, err := strconv.Atoi(*cfg.MaxSessions)
	if err != nil {
		return nil, err
	}

	lp := &LivepeerNode{
		Eth:              e,
		WorkDir:          wd,
		Database:         dbh,
		AutoAdjustPrice:  true,
		SegmentChans:     make(map[ManifestID]SegmentChan),
		segmentMutex:     &sync.RWMutex{},
		Capabilities:     &Capabilities{capacities: map[Capability]int{}},
		priceInfo:        make(map[string]*big.Rat),
		StorageConfigs:   make(map[string]*transcodeConfig),
		storageMutex:     &sync.RWMutex{},
		LivepeerConfig:   cfg,
		TStopChans:       make(map[string]chan string),
		Transcoders:      make(map[string]Transcoder),
		PlutusOrchs:      make(map[string]*PlutusOrchestrator),
		poMu:             &sync.RWMutex{},
		ActiveManifests:  []*ActiveManifest{},
		amMu:             &sync.RWMutex{},
		MaxSessions:      maxSession,
		TicketChan:       make(chan map[string]interface{}),
		NodeInitialized:  false,
		LatencyThreshold: *cfg.LatencyThreshold,
	}
	LPNode = lp
	return lp, nil
}

func (n *LivepeerNode) GetServiceURI() *url.URL {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return &n.serviceURI
}

func (n *LivepeerNode) SetServiceURI(newUrl *url.URL) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.serviceURI = *newUrl
}

// SetBasePrice sets the base price for an orchestrator on the node
func (n *LivepeerNode) SetBasePrice(b_eth_addr string, price *big.Rat) {
	addr := strings.ToLower(b_eth_addr)
	n.mu.Lock()
	defer n.mu.Unlock()

	n.priceInfo[addr] = price
}

// GetBasePrice gets the base price for an orchestrator
func (n *LivepeerNode) GetBasePrice(b_eth_addr string) *big.Rat {
	addr := strings.ToLower(b_eth_addr)
	n.mu.RLock()
	defer n.mu.RUnlock()

	price, exists := n.priceInfo[addr]

	if !exists {
		price = n.priceInfo["default"]
	}

	return price
}

func (n *LivepeerNode) GetBasePrices() map[string]*big.Rat {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.priceInfo
}

// SetMaxFaceValue sets the faceValue upper limit for tickets received
func (n *LivepeerNode) SetMaxFaceValue(maxfacevalue *big.Int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Recipient.SetMaxFaceValue(maxfacevalue)
}

func (n *LivepeerNode) SetMaxSessions(s int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	MaxSessions = s

	//update metrics reporting
	if lpmon.Enabled {
		lpmon.MaxSessions(MaxSessions)
	}

	glog.Infof("Updated session limit to %d", MaxSessions)
}

func (n *LivepeerNode) GetCurrentCapacity() int {
	n.TranscoderManager.RTmutex.Lock()
	defer n.TranscoderManager.RTmutex.Unlock()
	_, totalCapacity, _, _ := n.TranscoderManager.totalLoadAndCapacity()
	return totalCapacity
}
