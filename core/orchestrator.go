package core

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/big"
	"math/rand"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/golang/glog"

	"github.com/livepeer/go-livepeer/clog"
	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/go-livepeer/eth"
	"github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/go-livepeer/net"
	"github.com/livepeer/go-livepeer/pm"
	"github.com/livepeer/go-tools/drivers"

	lpcrypto "github.com/livepeer/go-livepeer/crypto"
	lpmon "github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/lpms/ffmpeg"
	"github.com/livepeer/lpms/stream"
)

const maxSegmentChannels = 4
const errNoMem = "Cannot allocate memory"

// this is set to be higher than httpPushTimeout in server/mediaserver.go so that B has a chance to end the session
// based on httpPushTimeout before transcodeLoopTimeout is reached
var transcodeLoopTimeout = 70 * time.Second

// Gives us more control of "timeout" / cancellation behavior during testing
var transcodeLoopContext = func() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), transcodeLoopTimeout)
}

// Transcoder / orchestrator RPC interface implementation
type orchestrator struct {
	address ethcommon.Address
	node    *LivepeerNode
	rm      common.RoundsManager
	secret  []byte
}

func (orch *orchestrator) ServiceURI() *url.URL {
	return orch.node.GetServiceURI()
}

func (orch *orchestrator) GetLatencyThreshold() int64 {
	return orch.node.LatencyThreshold
}

func (orch *orchestrator) Sign(msg []byte) ([]byte, error) {
	if orch.node == nil || orch.node.Eth == nil {
		return []byte{}, nil
	}
	return orch.node.Eth.Sign(crypto.Keccak256(msg))
}

func (orch *orchestrator) VerifySig(addr ethcommon.Address, msg string, sig []byte) bool {
	if orch.node == nil || orch.node.Eth == nil {
		return true
	}
	return lpcrypto.VerifySig(addr, crypto.Keccak256([]byte(msg)), sig)
}

func (orch *orchestrator) Address() ethcommon.Address {
	return orch.address
}

func (orch *orchestrator) TranscoderSecret() string {
	return orch.node.OrchSecret
}

func (orch *orchestrator) CheckCapacity(mid ManifestID, source string) error {
	orch.node.segmentMutex.RLock()
	defer orch.node.segmentMutex.RUnlock()
	if _, ok := orch.node.SegmentChans[mid]; ok {
		return nil
	}
	orch.node.TranscoderManager.RTmutex.Lock()
	load, capacity, remainingCap, numTranscoders := orch.node.TranscoderManager.totalLoadAndCapacity()
	glog.Infof("Get total load and capacity. Called by: ", source)
	glog.Infof("Total load: %v, capacity: %v, remaining capacity: %v, numTranscoders: %v", load, capacity, remainingCap, numTranscoders)
	orch.node.TranscoderManager.RTmutex.Unlock()
	if remainingCap < 1 {
		return ErrOrchCap
	}
	return nil
}

func (orch *orchestrator) TranscodeSeg(ctx context.Context, md *SegTranscodingMetadata, seg *stream.HLSSegment) (*TranscodeResult, error) {
	return orch.node.sendToTranscodeLoop(ctx, md, seg)
}

func (orch *orchestrator) ServeTranscoder(stream net.Transcoder_RegisterTranscoderServer, capacity int, remainingCap int, capabilities *net.Capabilities, ethereumAddr ethcommon.Address, uid string, poolVersion string, latency int64) {
	orch.node.serveTranscoder(stream, capacity, remainingCap, capabilities, ethereumAddr, uid, poolVersion, latency)
}

func (orch *orchestrator) TranscoderResults(tcID int64, res *RemoteTranscoderResult) {
	orch.node.TranscoderManager.transcoderResults(tcID, res)
}

func (orch *orchestrator) ProcessPayment(ctx context.Context, payment net.Payment, manifestID ManifestID) error {
	if orch.node == nil || orch.node.Recipient == nil {
		return nil
	}

	if payment.TicketParams == nil {
		return nil
	}

	if payment.Sender == nil || len(payment.Sender) == 0 {
		return fmt.Errorf("Could not find Sender for payment: %v", payment)
	}

	sender := ethcommon.BytesToAddress(payment.Sender)

	recipientAddr := ethcommon.BytesToAddress(payment.TicketParams.Recipient)
	ok, err := orch.isActive(recipientAddr)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("orchestrator %v is inactive in round %v, cannot process payments", recipientAddr.Hex(), orch.rm.LastInitializedRound())
	}

	priceInfo := payment.GetExpectedPrice()

	seed := new(big.Int).SetBytes(payment.TicketParams.Seed)

	priceInfoRat, err := common.RatPriceInfo(priceInfo)
	if err != nil {
		return fmt.Errorf("invalid expected price sent with payment err=%q", err)
	}
	if priceInfoRat == nil {
		return fmt.Errorf("invalid expected price sent with payment err=%q", "expected price is nil")
	}

	ticketParams := &pm.TicketParams{
		Recipient:         ethcommon.BytesToAddress(payment.TicketParams.Recipient),
		FaceValue:         new(big.Int).SetBytes(payment.TicketParams.FaceValue),
		WinProb:           new(big.Int).SetBytes(payment.TicketParams.WinProb),
		RecipientRandHash: ethcommon.BytesToHash(payment.TicketParams.RecipientRandHash),
		Seed:              seed,
		ExpirationBlock:   new(big.Int).SetBytes(payment.TicketParams.ExpirationBlock),
		PricePerPixel:     priceInfoRat,
	}

	ticketExpirationParams := &pm.TicketExpirationParams{
		CreationRound:          payment.ExpirationParams.CreationRound,
		CreationRoundBlockHash: ethcommon.BytesToHash(payment.ExpirationParams.CreationRoundBlockHash),
	}

	totalEV := big.NewRat(0, 1)
	totalTickets := 0
	totalWinningTickets := 0

	var receiveErr error

	for _, tsp := range payment.TicketSenderParams {

		ticket := pm.NewTicket(
			ticketParams,
			ticketExpirationParams,
			sender,
			tsp.SenderNonce,
		)

		clog.V(common.DEBUG).Infof(ctx, "Receiving ticket sessionID=%v faceValue=%v winProb=%v ev=%v", manifestID, eth.FormatUnits(ticket.FaceValue, "ETH"), ticket.WinProbRat().FloatString(10), ticket.EV().FloatString(2))

		_, won, err := orch.node.Recipient.ReceiveTicket(
			ticket,
			tsp.Sig,
			seed,
		)
		if err != nil {
			clog.Errorf(ctx, "Error receiving ticket sessionID=%v recipientRandHash=%x senderNonce=%v: %v", manifestID, ticket.RecipientRandHash, ticket.SenderNonce, err)

			if monitor.Enabled {
				monitor.PaymentRecvError(ctx, sender.Hex(), err.Error())
			}
			if _, ok := err.(*pm.FatalReceiveErr); ok {
				return err
			}
			receiveErr = err
		}

		if receiveErr == nil {
			// Add ticket EV to credit
			ev := ticket.EV()
			orch.node.Balances.Credit(sender, manifestID, ev)
			totalEV.Add(totalEV, ev)
			totalTickets++
		}

		if won {
			clog.V(common.DEBUG).Infof(ctx, "Received winning ticket sessionID=%v recipientRandHash=%x senderNonce=%v", manifestID, ticket.RecipientRandHash, ticket.SenderNonce)

			totalWinningTickets++

			go func(ticket *pm.Ticket, sig []byte, seed *big.Int) {
				if err := orch.node.Recipient.RedeemWinningTicket(ticket, sig, seed); err != nil {
					clog.Errorf(ctx, "error redeeming ticket sessionID=%v recipientRandHash=%x senderNonce=%v err=%q", manifestID, ticket.RecipientRandHash, ticket.SenderNonce, err)
				}
			}(ticket, tsp.Sig, seed)
		}
	}

	if monitor.Enabled {
		monitor.TicketValueRecv(ctx, sender.Hex(), totalEV)
		monitor.TicketsRecv(ctx, sender.Hex(), totalTickets)
		monitor.WinningTicketsRecv(ctx, sender.Hex(), totalWinningTickets)
	}

	if receiveErr != nil {
		return receiveErr
	}

	return nil
}

func (orch *orchestrator) TicketParams(sender ethcommon.Address, priceInfo *net.PriceInfo) (*net.TicketParams, error) {
	if orch.node == nil || orch.node.Recipient == nil {
		return nil, nil
	}

	ratPriceInfo, err := common.RatPriceInfo(priceInfo)
	if err != nil {
		return nil, err
	}

	params, err := orch.node.Recipient.TicketParams(sender, ratPriceInfo)
	if err != nil {
		return nil, err
	}

	return &net.TicketParams{
		Recipient:         params.Recipient.Bytes(),
		FaceValue:         params.FaceValue.Bytes(),
		WinProb:           params.WinProb.Bytes(),
		RecipientRandHash: params.RecipientRandHash.Bytes(),
		Seed:              params.Seed.Bytes(),
		ExpirationBlock:   params.ExpirationBlock.Bytes(),
		ExpirationParams: &net.TicketExpirationParams{
			CreationRound:          params.ExpirationParams.CreationRound,
			CreationRoundBlockHash: params.ExpirationParams.CreationRoundBlockHash.Bytes(),
		},
	}, nil
}

func (orch *orchestrator) PriceInfo(sender ethcommon.Address) (*net.PriceInfo, error) {
	if orch.node == nil || orch.node.Recipient == nil {
		return nil, nil
	}

	price, err := orch.priceInfo(sender)
	if err != nil {
		return nil, err
	}

	if monitor.Enabled {
		monitor.TranscodingPrice(sender.String(), price)
	}

	return &net.PriceInfo{
		PricePerUnit:  price.Num().Int64(),
		PixelsPerUnit: price.Denom().Int64(),
	}, nil
}

// priceInfo returns price per pixel as a fixed point number wrapped in a big.Rat
func (orch *orchestrator) priceInfo(sender ethcommon.Address) (*big.Rat, error) {
	basePrice := orch.node.GetBasePrice(sender.String())

	if basePrice == nil {
		basePrice = orch.node.GetBasePrice("default")
	}

	if !orch.node.AutoAdjustPrice {
		return basePrice, nil
	}

	// If price = 0, overhead is 1
	// If price > 0, overhead = 1 + (1 / txCostMultiplier)
	overhead := big.NewRat(1, 1)
	if basePrice.Num().Cmp(big.NewInt(0)) > 0 {
		txCostMultiplier, err := orch.node.Recipient.TxCostMultiplier(sender)
		if err != nil {
			return nil, err
		}

		if txCostMultiplier.Cmp(big.NewRat(0, 1)) > 0 {
			overhead = overhead.Add(overhead, new(big.Rat).Inv(txCostMultiplier))
		}

	}
	// pricePerPixel = basePrice * overhead
	fixedPrice, err := common.PriceToFixed(new(big.Rat).Mul(basePrice, overhead))
	if err != nil {
		return nil, err
	}
	return common.FixedToPrice(fixedPrice), nil
}

// SufficientBalance checks whether the credit balance for a stream is sufficient
// to proceed with downloading and transcoding
func (orch *orchestrator) SufficientBalance(addr ethcommon.Address, manifestID ManifestID) bool {
	if orch.node == nil || orch.node.Recipient == nil || orch.node.Balances == nil {
		return true
	}

	balance := orch.node.Balances.Balance(addr, manifestID)
	if balance == nil || balance.Cmp(orch.node.Recipient.EV()) < 0 {
		return false
	}
	return true
}

// DebitFees debits the balance for a ManifestID based on the amount of output pixels * price
func (orch *orchestrator) DebitFees(addr ethcommon.Address, manifestID ManifestID, price *net.PriceInfo, pixels int64) {
	// Don't debit in offchain mode
	if orch.node == nil || orch.node.Balances == nil {
		return
	}
	priceRat := big.NewRat(price.GetPricePerUnit(), price.GetPixelsPerUnit())
	orch.node.Balances.Debit(addr, manifestID, priceRat.Mul(priceRat, big.NewRat(pixels, 1)))
}

func (orch *orchestrator) Capabilities() *net.Capabilities {
	if orch.node == nil {
		return nil
	}
	return orch.node.Capabilities.ToNetCapabilities()
}

func (orch *orchestrator) AuthToken(sessionID string, expiration int64) *net.AuthToken {
	h := hmac.New(sha256.New, orch.secret)
	msg := append([]byte(sessionID), new(big.Int).SetInt64(expiration).Bytes()...)
	h.Write(msg)

	return &net.AuthToken{
		Token:      h.Sum(nil),
		SessionId:  sessionID,
		Expiration: expiration,
	}
}

func (orch *orchestrator) isActive(addr ethcommon.Address) (bool, error) {
	// Accept payments when already activated or will be activated in the next round
	nextRound := new(big.Int).Add(orch.rm.LastInitializedRound(), big.NewInt(1))
	filter := &common.DBOrchFilter{
		CurrentRound: nextRound,
		Addresses:    []ethcommon.Address{addr},
	}
	orchs, err := orch.node.Database.SelectOrchs(filter)
	if err != nil {
		return false, err
	}

	return len(orchs) > 0, nil
}

func NewOrchestrator(n *LivepeerNode, rm common.RoundsManager) *orchestrator {
	var addr ethcommon.Address
	if n.Eth != nil {
		addr = n.Eth.Account().Address
	}
	return &orchestrator{
		node:    n,
		address: addr,
		rm:      rm,
		secret:  common.RandomBytesGenerator(32),
	}
}

// LivepeerNode transcode methods

var ErrOrchBusy = errors.New("OrchestratorBusy")
var ErrOrchCap = errors.New("OrchestratorCapped")

type TranscodeResult struct {
	Err           error
	Sig           []byte
	TranscodeData *TranscodeData
	OS            drivers.OSSession
}

// TranscodeData contains the transcoding output for an input segment
type TranscodeData struct {
	Segments   []*TranscodedSegmentData
	Pixels     int64 // Decoded pixels
	Detections []ffmpeg.DetectData
}

// TranscodedSegmentData contains encoded data for a profile
type TranscodedSegmentData struct {
	Data   []byte
	PHash  []byte // Perceptual hash data (maybe nil)
	Pixels int64  // Encoded pixels
}

type SegChanData struct {
	ctx context.Context
	seg *stream.HLSSegment
	md  *SegTranscodingMetadata
	res chan *TranscodeResult
}

type RemoteTranscoderResult struct {
	TranscodeData *TranscodeData
	Err           error
}

type SegmentChan chan *SegChanData
type TranscoderChan chan *RemoteTranscoderResult

type transcodeConfig struct {
	OS      drivers.OSSession
	LocalOS drivers.OSSession
}

func (rtm *RemoteTranscoderManager) getTaskChan(taskID int64) (TranscoderChan, error) {
	rtm.taskMutex.RLock()
	defer rtm.taskMutex.RUnlock()
	if tc, ok := rtm.taskChans[taskID]; ok {
		return tc, nil
	}
	return nil, fmt.Errorf("No transcoder channel")
}

func (rtm *RemoteTranscoderManager) addTaskChan() (int64, TranscoderChan) {
	rtm.taskMutex.Lock()
	defer rtm.taskMutex.Unlock()
	taskID := rtm.taskCount
	rtm.taskCount++
	if tc, ok := rtm.taskChans[taskID]; ok {
		// should really never happen
		glog.V(common.DEBUG).Info("Transcoder channel already exists for ", taskID)
		return taskID, tc
	}
	rtm.taskChans[taskID] = make(TranscoderChan, 1)
	return taskID, rtm.taskChans[taskID]
}

func (rtm *RemoteTranscoderManager) removeTaskChan(taskID int64) {
	rtm.taskMutex.Lock()
	defer rtm.taskMutex.Unlock()
	if _, ok := rtm.taskChans[taskID]; !ok {
		glog.V(common.DEBUG).Info("Transcoder channel nonexistent for job ", taskID)
		return
	}
	delete(rtm.taskChans, taskID)
}

func (n *LivepeerNode) getSegmentChan(ctx context.Context, md *SegTranscodingMetadata) (SegmentChan, error) {
	// concurrency concerns here? what if a chan is added mid-call?
	n.segmentMutex.Lock()
	defer n.segmentMutex.Unlock()
	if sc, ok := n.SegmentChans[ManifestID(md.AuthToken.SessionId)]; ok {
		return sc, nil
	}
	n.TranscoderManager.RTmutex.Lock()
	_, _, remainingCap, _ := n.TranscoderManager.totalLoadAndCapacity()
	n.TranscoderManager.RTmutex.Unlock()
	if remainingCap < 1 {
		return nil, ErrOrchCap
	}
	sc := make(SegmentChan, maxSegmentChannels)
	clog.V(common.DEBUG).Infof(ctx, "Creating new segment chan")
	if err := n.transcodeSegmentLoop(clog.Clone(context.Background(), ctx), md, sc); err != nil {
		return nil, err
	}
	n.SegmentChans[ManifestID(md.AuthToken.SessionId)] = sc
	if lpmon.Enabled {
		lpmon.CurrentSessions(len(n.SegmentChans))
	}
	return sc, nil
}

func (n *LivepeerNode) sendToTranscodeLoop(ctx context.Context, md *SegTranscodingMetadata, seg *stream.HLSSegment) (*TranscodeResult, error) {
	clog.V(common.DEBUG).Infof(ctx, "Starting to transcode segment")
	ch, err := n.getSegmentChan(ctx, md)
	if err != nil {
		clog.Errorf(ctx, "Could not find segment chan err=%q", err)
		return nil, err
	}
	segChanData := &SegChanData{ctx: ctx, seg: seg, md: md, res: make(chan *TranscodeResult, 1)}
	select {
	case ch <- segChanData:
		clog.V(common.DEBUG).Infof(ctx, "Submitted segment to transcode loop ")
	default:
		// sending segChan should not block; if it does, the channel is busy
		clog.Errorf(ctx, "Transcoder was busy with a previous segment")
		return nil, ErrOrchBusy
	}
	res := <-segChanData.res
	return res, res.Err
}

func (n *LivepeerNode) transcodeSeg(ctx context.Context, config transcodeConfig, seg *stream.HLSSegment, md *SegTranscodingMetadata) *TranscodeResult {
	var fnamep *string
	terr := func(err error) *TranscodeResult {
		if fnamep != nil {
			os.Remove(*fnamep)
		}
		return &TranscodeResult{Err: err}
	}

	// Prevent unnecessary work, check for replayed sequence numbers.
	// NOTE: If we ever process segments from the same job concurrently,
	// we may still end up doing work multiple times. But this is OK for now.

	//Assume d is in the right format, write it to disk
	inName := common.RandName() + ".tempfile"
	if _, err := os.Stat(n.WorkDir); os.IsNotExist(err) {
		err := os.Mkdir(n.WorkDir, 0700)
		if err != nil {
			clog.Errorf(ctx, "Transcoder cannot create workdir err=%q", err)
			return terr(err)
		}
	}
	// Create input file from segment. Removed after claiming complete or error
	fname := path.Join(n.WorkDir, inName)
	fnamep = &fname
	if err := ioutil.WriteFile(fname, seg.Data, 0644); err != nil {
		clog.Errorf(ctx, "Transcoder cannot write file err=%q", err)
		return terr(err)
	}

	// Check if there's a transcoder available
	if len(n.Transcoders) == 0 {
		return terr(ErrTranscoderAvail)
	}

	var url string

	if config.OS.IsExternal() && config.OS.IsOwn(seg.Name) {
		// We're using a remote TC and segment is already in our own OS
		// Incurs an additional download for topologies with T on local network!
		url = seg.Name
	} else {
		// Need to store segment in our local OS
		var err error
		name := fmt.Sprintf("%d.tempfile", seg.SeqNo)
		url, err = config.LocalOS.SaveData(ctx, name, bytes.NewReader(seg.Data), nil, 0)
		if err != nil {
			return terr(err)
		}
		seg.Name = url
	}
	md.Fname = url

	//Do the transcoding
	start := time.Now()
	tData, err := n.TranscoderManager.Transcode(ctx, md)
	if err != nil {
		if _, ok := err.(UnrecoverableError); ok {
			panic(err)
		}
		clog.Errorf(ctx, "Error transcoding segName=%s err=%q", seg.Name, err)
		return terr(err)
	}

	tSegments := tData.Segments
	if len(tSegments) != len(md.Profiles) {
		clog.Errorf(ctx, "Did not receive the correct number of transcoded segments; got %v expected %v", len(tSegments),
			len(md.Profiles))
		return terr(fmt.Errorf("MismatchedSegments"))
	}

	took := time.Since(start)
	clog.V(common.DEBUG).Infof(ctx, "Transcoding of segment took=%v", took)
	if monitor.Enabled {
		monitor.SegmentTranscoded(ctx, 0, seg.SeqNo, md.Duration, took, common.ProfilesNames(md.Profiles), true, true)
	}

	// Prepare the result object
	var tr TranscodeResult
	segHashes := make([][]byte, len(tSegments))

	for i := range md.Profiles {
		if tSegments[i].Data == nil || len(tSegments[i].Data) < 25 {
			clog.Errorf(ctx, "Cannot find transcoded segment for bytes=%d", len(tSegments[i].Data))
			return terr(fmt.Errorf("ZeroSegments"))
		}
		if md.CalcPerceptualHash && tSegments[i].PHash == nil {
			clog.Errorf(ctx, "Could not find perceptual hash for profile=%v", md.Profiles[i].Name)
			// FIXME: Return the error once everyone has upgraded their nodes
			// return terr(fmt.Errorf("MissingPerceptualHash"))
		}
		clog.V(common.DEBUG).Infof(ctx, "Transcoded segment profile=%s bytes=%d",
			md.Profiles[i].Name, len(tSegments[i].Data))
		hash := crypto.Keccak256(tSegments[i].Data)
		segHashes[i] = hash
	}
	os.Remove(fname)
	tr.OS = config.OS
	tr.TranscodeData = tData

	if n == nil || n.Eth == nil {
		return &tr
	}

	segHash := crypto.Keccak256(segHashes...)
	tr.Sig, tr.Err = n.Eth.Sign(segHash)
	if tr.Err != nil {
		clog.Errorf(ctx, "Unable to sign hash of transcoded segment hashes err=%q", tr.Err)
	}
	return &tr
}

func (n *LivepeerNode) transcodeSegmentLoop(logCtx context.Context, md *SegTranscodingMetadata, segChan SegmentChan) error {
	manifestID := string(md.ManifestID)
	clog.V(common.DEBUG).Infof(logCtx, "Starting transcode segment loop for manifestID=%s sessionID=%s", md.ManifestID, md.AuthToken.SessionId)

	// Set up local OS for any remote transcoders to use if necessary
	if drivers.NodeStorage == nil {
		return fmt.Errorf("Missing local storage")
	}

	los := drivers.NodeStorage.NewSession(md.AuthToken.SessionId)

	// determine appropriate OS to use
	os := drivers.NewSession(FromNetOsInfo(md.OS))
	if os == nil {
		// no preference (or unknown pref), so use our own
		os = los
	}
	storageConfig := transcodeConfig{
		OS:      os,
		LocalOS: los,
	}
	n.storageMutex.Lock()
	n.StorageConfigs[md.AuthToken.SessionId] = &storageConfig
	n.storageMutex.Unlock()
	go func() {
		for {
			// XXX make context timeout configurable
			ctx, cancel := context.WithTimeout(context.Background(), transcodeLoopTimeout)
			select {
			case <-ctx.Done():
				clog.V(common.DEBUG).Infof(logCtx, "Segment loop timed out; closing ")
				n.endTranscodingSession(md.AuthToken.SessionId, manifestID, logCtx)

				return
			case chanData, ok := <-segChan:
				// Check if channel was closed due to endTranscodingSession being called by B
				if !ok {
					cancel()
					return
				}
				chanData.res <- n.transcodeSeg(chanData.ctx, storageConfig, chanData.seg, chanData.md)
			}
			cancel()
		}
	}()
	return nil
}

func (n *LivepeerNode) endTranscodingSession(sessionId string, manifestID string, logCtx context.Context) {
	// timeout; clean up goroutine here
	var (
		exists  bool
		storage *transcodeConfig
		sess    *RemoteTranscoder
	)
	n.storageMutex.Lock()
	if storage, exists = n.StorageConfigs[sessionId]; exists {
		storage.OS.EndSession()
		storage.LocalOS.EndSession()
		delete(n.StorageConfigs, sessionId)
	}
	n.storageMutex.Unlock()
	// check to avoid nil pointer caused by garbage collection while this go routine is still running
	if n.TranscoderManager != nil {
		n.TranscoderManager.RTmutex.Lock()
		// send empty segment to signal transcoder internal session teardown if session exist
		if sess, exists = n.TranscoderManager.streamSessions[sessionId]; exists {
			segData := &net.SegData{
				AuthToken: &net.AuthToken{SessionId: sessionId},
			}
			msg := &net.NotifySegment{
				SegData: segData,
			}
			_ = sess.stream.Send(msg)
		}
		n.TranscoderManager.completeStreamSession(sessionId, manifestID)
		n.TranscoderManager.RTmutex.Unlock()
	}
	n.segmentMutex.Lock()
	mid := ManifestID(sessionId)
	if _, exists = n.SegmentChans[mid]; exists {
		close(n.SegmentChans[mid])
		delete(n.SegmentChans, mid)
		if lpmon.Enabled {
			lpmon.CurrentSessions(len(n.SegmentChans))
		}
	}
	n.segmentMutex.Unlock()
	if exists {
		clog.V(common.DEBUG).Infof(logCtx, "Transcoding session ended by the Broadcaster for sessionID=%v", sessionId)
	}
}

func (n *LivepeerNode) serveTranscoder(stream net.Transcoder_RegisterTranscoderServer, capacity int, remainingCap int, capabilities *net.Capabilities, ethereumAddr ethcommon.Address, uid string, poolVersion string, latency int64) {
	from := common.GetConnectionAddr(stream.Context())
	coreCaps := CapabilitiesFromNetCapabilities(capabilities)
	n.Capabilities.AddCapacity(coreCaps)
	defer n.Capabilities.RemoveCapacity(coreCaps)

	if n.AutoSessionLimit {
		n.SetMaxSessions(n.GetCurrentCapacity() + capacity)
	}

	// Manage blocks while transcoder is connected
	n.TranscoderManager.Manage(n, stream, capacity, remainingCap, capabilities, ethereumAddr, uid, poolVersion, latency)
	glog.V(common.DEBUG).Infof("Closing transcoder=%s channel", from)

	if n.AutoSessionLimit {
		defer n.SetMaxSessions(n.GetCurrentCapacity())
	}
}

func (rtm *RemoteTranscoderManager) transcoderResults(tcID int64, res *RemoteTranscoderResult) {
	remoteChan, err := rtm.getTaskChan(tcID)
	if err != nil {
		return // do we need to return anything?
	}
	remoteChan <- res
}

type RemoteTranscoder struct {
	manager       *RemoteTranscoderManager
	stream        net.Transcoder_RegisterTranscoderServer
	capabilities  *Capabilities
	eof           chan struct{}
	Addr          string            `json:"addr"`
	Capacity      int               `json:"capacity"`
	RemainingCap  int               `json:"remainingCap"`
	LocalLoad     int               `json:"localLoad"`
	TotalLoad     int               `json:"totalLoad"`
	EthereumAddr  ethcommon.Address `json:"ethereumAddr"`
	Uid           string            `json:"uid"`
	PoolVersion   string            `json:"poolVersion"`
	node          *LivepeerNode
	stats         *RemoteTranscoderStats
	ConnTimestamp int64 `json:"connTimestamp"`
	ConnLatency   int64 `json:"connLatency"`
}

// RemoteTranscoderFatalError wraps error to indicate that error is fatal
type RemoteTranscoderFatalError struct {
	error
}

// NewRemoteTranscoderFatalError creates new RemoteTranscoderFatalError
// Exported here to be used in other packages
func NewRemoteTranscoderFatalError(err error) error {
	return RemoteTranscoderFatalError{err}
}

var ErrRemoteTranscoderTimeout = errors.New("Remote transcoder took too long")
var ErrNoTranscodersAvailable = errors.New("no transcoders available")
var ErrNoCompatibleTranscodersAvailable = errors.New("no transcoders can provide requested capabilities")

func (rt *RemoteTranscoder) done() {
	// select so we don't block indefinitely if there's no listener
	select {
	case rt.eof <- struct{}{}:
	default:
	}
}

// Transcode do actual transcoding by sending work to remote transcoder and waiting for the result
func (rt *RemoteTranscoder) Transcode(logCtx context.Context, md *SegTranscodingMetadata) (*TranscodeData, error) {
	taskID, taskChan := rt.manager.addTaskChan()
	defer rt.manager.removeTaskChan(taskID)
	fname := md.Fname
	signalEOF := func(err error) (*TranscodeData, error) {
		rt.done()
		clog.Errorf(logCtx, "Fatal error with remote transcoder=%s taskId=%d fname=%s err=%q", rt.Addr, taskID, fname, err)
		return nil, RemoteTranscoderFatalError{err}
	}

	// Copy and remove some fields to minimize unneeded transfer
	mdCopy := *md
	mdCopy.OS = nil // remote transcoders currently upload directly back to O
	mdCopy.Hash = ethcommon.Hash{}
	segData, err := NetSegData(&mdCopy)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	msg := &net.NotifySegment{
		Url:     fname,
		TaskId:  taskID,
		SegData: segData,
		// Triggers failure on Os that don't know how to use SegData
		Profiles: []byte("invalid"),
	}
	err = rt.stream.Send(msg)

	if err != nil {
		return signalEOF(err)
	}

	// set a minimum timeout to accommodate transport / processing overhead
	dur := common.HTTPTimeout
	paddedDur := 4.0 * md.Duration // use a multiplier of 4 for now
	if paddedDur > dur {
		dur = paddedDur
	}

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	select {
	case <-ctx.Done():
		return signalEOF(ErrRemoteTranscoderTimeout)
	case chanData := <-taskChan:
		if chanData.Err != nil {
			if chanData.Err.Error() == errNoMem {
				clog.InfofErr(logCtx, "Jailing Remote transcoder=%s host=%s taskId=%d fname=%s dur=%v for failing to transcode with error=",
					rt.EthereumAddr.String(), rt.Addr, taskID, fname, time.Since(start), chanData.Err)
				// Lock reading jail status and processing transcoder data
				rt.stats.readMutex.Lock()
				rt.stats.processMutex.Lock()
				rt.stats.JailTranscoder() //< Sets jail time
				rt.stats.jailed = true    //< Set jail flag separately. Switching sessions happens in selectTranscoder
				rt.stats.processMutex.Unlock()
				rt.stats.readMutex.Unlock()
			} else {
				clog.InfofErr(logCtx, "Unhandled exception: Remote transcoder=%s host=%s taskId=%d fname=%s dur=%v failed to transcode with error=",
					rt.EthereumAddr.String(), rt.Addr, taskID, fname, time.Since(start), chanData.Err)
			}
			return chanData.TranscodeData, chanData.Err
		}
		segmentLen := 0
		pixelsEncoded := float64(0)
		var pixelsDecoded int64 = 0
		if chanData.TranscodeData != nil {
			pixelsDecoded = chanData.TranscodeData.Pixels
			segmentLen = len(chanData.TranscodeData.Segments)
			responseTime := float64(time.Since(start).Milliseconds())
			for i := 0; i < len(chanData.TranscodeData.Segments); i++ {
				pixelsEncoded += float64(chanData.TranscodeData.Segments[i].Pixels)
			}
			go func() {
				rt.RecordStats(responseTime, float64(md.Duration.Milliseconds()), pixelsEncoded, float64(chanData.TranscodeData.Pixels), clog.GetVal(logCtx, "orchSessionID"))
			}()
		}
		clog.InfofErr(logCtx, "Successfully received results from remote transcoder=%s host=%s segments=%d taskId=%d fname=%s dur=%v",
			rt.EthereumAddr.String(), rt.Addr, segmentLen, taskID, fname, time.Since(start), chanData.Err)

		// Log segment transcode to DB
		pixelsEncodedInt := int64(pixelsEncoded)

		var sessionIdlist string = ""
		for mid := range rt.node.SegmentChans {
			sessionIdlist += (string(mid) + ",")
		}
		js, _ := json.Marshal(md.Profiles)
		glog.Infof(string(js))

		sender := clog.GetVal(ctx, "sender")
		basePrice := rt.node.GetBasePrice(sender)
		glog.Infof("Price used for broadcaster %v is %v", sender, basePrice.String())
		price := basePrice.Num().Int64()

		segStats := TranscodedSegmentStats{
			UID:           rt.Uid,
			LocalLoad:     rt.LocalLoad,
			TotalLoad:     rt.TotalLoad,
			SessionId:     md.AuthToken.SessionId,
			SessionList:   sessionIdlist,
			Duration:      md.Duration.Milliseconds(),
			PixelsDecoded: pixelsDecoded,
			PixelsEncoded: pixelsEncodedInt,
			Price:         price,
			Took:          time.Since(start).Milliseconds(),
			Profile:       string(js),
			Timestamp:     time.Now().Unix(),
		}

		rt.node.MqttBroker.PublishSegmentTranscoded(segStats)

		if err != nil {
			glog.Error("Error writing transcoder transcode stat increment log=", err)
		}

		return chanData.TranscodeData, chanData.Err
	}
}
func NewRemoteTranscoder(m *RemoteTranscoderManager, stream net.Transcoder_RegisterTranscoderServer, capacity int, remainingCap int, caps *Capabilities, ethereumAddr ethcommon.Address, uid string, poolVersion string, node *LivepeerNode, latency int64) *RemoteTranscoder {
	totalLoad := capacity - remainingCap
	return &RemoteTranscoder{
		manager:       m,
		stream:        stream,
		eof:           make(chan struct{}, 1),
		Capacity:      capacity,
		RemainingCap:  remainingCap,
		TotalLoad:     totalLoad,
		LocalLoad:     0,
		Addr:          common.GetConnectionAddr(stream.Context()),
		capabilities:  caps,
		EthereumAddr:  ethereumAddr,
		PoolVersion:   poolVersion,
		node:          node,
		stats:         nil,
		Uid:           uid,
		ConnTimestamp: time.Now().Unix(),
		ConnLatency:   latency,
	}
}

func NewRemoteTranscoderManager() *RemoteTranscoderManager {
	return &RemoteTranscoderManager{
		remoteTranscoders: []*RemoteTranscoder{},
		liveTranscoders:   map[net.Transcoder_RegisterTranscoderServer]*RemoteTranscoder{},
		RTmutex:           sync.Mutex{},

		taskMutex: &sync.RWMutex{},
		taskChans: make(map[int64]TranscoderChan),

		streamSessions: make(map[string]*RemoteTranscoder),

		PoolManager: NewPublicTranscoderPool(),
	}
}

// type byLoadFactor []*RemoteTranscoder

// func loadFactor(r *RemoteTranscoder) float64 {
// 	return float64(r.load) / float64(r.capacity)
// }

// func (r byLoadFactor) Len() int      { return len(r) }
// func (r byLoadFactor) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
// func (r byLoadFactor) Less(i, j int) bool {
// 	return loadFactor(r[j]) > loadFactor(r[i]) // sort ascending
// }

type RemoteTranscoderManager struct {
	remoteTranscoders []*RemoteTranscoder
	liveTranscoders   map[net.Transcoder_RegisterTranscoderServer]*RemoteTranscoder
	RTmutex           sync.Mutex

	// For tracking tasks assigned to remote transcoders
	taskMutex *sync.RWMutex
	taskChans map[int64]TranscoderChan
	taskCount int64

	// Map for keeping track of sessions and their respective transcoders
	streamSessions map[string]*RemoteTranscoder

	PoolManager *PublicTranscoderPool

	//pass mqtt broker to publish messages
	MqttBroker *PlutusMQ
}

func (rtm *RemoteTranscoderManager) Stop() {

}

// RegisteredTranscodersCount returns number of registered transcoders
func (rtm *RemoteTranscoderManager) RegisteredTranscodersCount() int {
	rtm.RTmutex.Lock()
	defer rtm.RTmutex.Unlock()
	return len(rtm.liveTranscoders)
}

// RegisteredTranscodersInfo returns list of restered transcoder's information
func (rtm *RemoteTranscoderManager) RegisteredTranscodersInfo() []common.RemoteTranscoderInfo {
	rtm.RTmutex.Lock()
	res := make([]common.RemoteTranscoderInfo, 0, len(rtm.liveTranscoders))
	for _, transcoder := range rtm.liveTranscoders {
		res = append(res, common.RemoteTranscoderInfo{Address: transcoder.Addr, Capacity: transcoder.Capacity, EthereumAddress: transcoder.EthereumAddr})
	}
	rtm.RTmutex.Unlock()
	return res
}

// Manage adds transcoder to list of live transcoders. Doesn't return until transcoder disconnects
func (rtm *RemoteTranscoderManager) Manage(node *LivepeerNode, stream net.Transcoder_RegisterTranscoderServer, capacity int, remainingCap int, capabilities *net.Capabilities, ethereumAddr ethcommon.Address, uid string, poolVersion string, latency int64) {
	//check latency test against threshold
	if latency == 0 {
		glog.Infof("Latency test failed for transcoder=%s, latency=%d, threshold=%d. Latency cannot be zero", ethereumAddr, latency, node.LatencyThreshold)
		return
	}

	if latency > node.LatencyThreshold {
		glog.Infof("Latency test failed for transcoder=%s, latency=%d, threshold=%d", ethereumAddr, latency, node.LatencyThreshold)
		return
	}

	glog.Infof("Latency test passed for transcoder=%s, latency=%d, threshold=%d", ethereumAddr, latency, node.LatencyThreshold)

	from := common.GetConnectionAddr(stream.Context())
	transcoder := NewRemoteTranscoder(rtm, stream, capacity, remainingCap, CapabilitiesFromNetCapabilities(capabilities), ethereumAddr, uid, poolVersion, node, latency)
	thisAddr := ethereumAddr.String()

	go func() {
		ctx := stream.Context()
		<-ctx.Done()
		err := ctx.Err()
		glog.Errorf("Stream closed for transcoder=%s, err=%q", from, err)
		transcoder.done()
	}()

	rtm.RTmutex.Lock()
	// Point to the stats object of an existing remote T connection
	for i := 0; i < len(rtm.PoolManager.transcoderStats); i++ {
		if rtm.PoolManager.transcoderStats[i].owner == thisAddr {
			glog.Infof("Reusing stats object for Transcoder %s", from)
			transcoder.stats = rtm.PoolManager.transcoderStats[i]
			break
		}
	}
	// Count sum of capacity
	thisCapacity := capacity // init to capacity as the current T is not in the list yet
	for i := 0; i < len(rtm.remoteTranscoders); i++ {
		if rtm.remoteTranscoders[i].EthereumAddr.String() == thisAddr {
			thisCapacity += rtm.remoteTranscoders[i].Capacity
		}
	}
	if transcoder.stats == nil {
		glog.Infof("Creating new stats object for Transcoder %s", from)
		transcoder.stats = newStats(thisAddr)
		transcoder.stats.sessionAccxTime = make(map[string]int64)
		rtm.PoolManager.transcoderStats = append(rtm.PoolManager.transcoderStats, transcoder.stats)
	}
	transcoder.InitPromStats()
	rtm.liveTranscoders[transcoder.stream] = transcoder
	rtm.remoteTranscoders = append(rtm.remoteTranscoders, transcoder)
	var totalLoad, totalCapacity, liveTranscodersNum int
	if monitor.Enabled {
		totalLoad, totalCapacity, _, liveTranscodersNum = rtm.totalLoadAndCapacity()
	}
	node.MqttBroker.PublishTranscoderConnection(transcoder, false, "orchestrator.go:1066")
	node.MqttBroker.SubTranscoderTelemetry(transcoder.Uid)
	rtm.RTmutex.Unlock()

	if monitor.Enabled {
		monitor.SetTranscodersNumberAndLoad(totalLoad, totalCapacity, liveTranscodersNum)
		monitor.SetTranscoderCapacity(thisAddr, thisCapacity)
		monitor.SetTranscoderLocalLoad(thisAddr, 0)
	}

	<-transcoder.eof
	glog.Infof("Got transcoder=%s eof, removing from live transcoders map", from)

	rtm.RTmutex.Lock()
	delete(rtm.liveTranscoders, transcoder.stream)
	if monitor.Enabled {
		totalLoad, totalCapacity, _, liveTranscodersNum = rtm.totalLoadAndCapacity()
	}
	glog.Infof("Removing Transcoder %s from the list of remote transcoders as they've disconnected", thisAddr)
	rtm.remoteTranscoders = removeFromRemoteTranscoders(transcoder, rtm.remoteTranscoders)
	// Count sum of capacity
	thisCapacity = 0
	for i := 0; i < len(rtm.remoteTranscoders); i++ {
		if rtm.remoteTranscoders[i].EthereumAddr.String() == thisAddr {
			thisCapacity += rtm.remoteTranscoders[i].Capacity
		}
	}

	// *******legacy from database logging*********
	// err = transcoder.node.Database.CreateTranscoderConnectionLogRecord(thisAddr, "disconnected", thisCapacity, time.Now().Unix())
	// if err != nil {
	// 	glog.Error("Error writing transcoder connection log=", err)
	// }

	node.MqttBroker.PublishTranscoderConnection(transcoder, true, "orchestrator.go:1093")

	rtm.RTmutex.Unlock()
	if monitor.Enabled {
		monitor.SetTranscodersNumberAndLoad(totalLoad, totalCapacity, liveTranscodersNum)
		monitor.SetTranscoderCapacity(thisAddr, thisCapacity)
		monitor.SetTranscoderLocalLoad(thisAddr, 0)
		monitor.SetTranscoderStats(thisAddr, 0, 0, 0, 0, 0, 0)
		monitor.SetTranscoderLastVals(thisAddr, 0, 0, 0)
	}
}

func removeFromRemoteTranscoders(rt *RemoteTranscoder, remoteTranscoders []*RemoteTranscoder) []*RemoteTranscoder {
	if len(remoteTranscoders) == 0 {
		// No transcoders to remove, return
		return remoteTranscoders
	}

	newRemoteTs := make([]*RemoteTranscoder, 0)
	for _, t := range remoteTranscoders {
		if t != rt {
			newRemoteTs = append(newRemoteTs, t)
		}
	}
	return newRemoteTs
}

// Returns the index of the transcoder in the RemoteTranscoder list. Returns -1 if it cannot find them
func (rtm *RemoteTranscoderManager) getTranscoderIndex(remoteTranscoder *RemoteTranscoder) int {
	for j := 0; j < len(rtm.remoteTranscoders); j++ {
		if rtm.remoteTranscoders[j] == remoteTranscoder {
			return j
		}
	}
	return -1
}

// Selects randomly between transcoders who are considered top performers in any of the measured performance indicators
// The more indicators being met, the higher the chance of being selected
func selectionTopPerformers(remoteTranscoders []*RemoteTranscoder, realtimeThreshold float64, encodedThreshold float64, decodedThreshold float64) *RemoteTranscoder {
	rand.Seed(time.Now().UnixNano())
	glog.Infof("Selection: Selecting a Transcoder by random selection over Top Performers")
	KPIThreshold := 1 //< By default consider T's who reach at least one KPI
	eligible := make([]*RemoteTranscoder, 0)
	for i := 0; i < len(remoteTranscoders); i++ {
		remoteTranscoders[i].stats.readMutex.Lock()
		count := 0
		if remoteTranscoders[i].stats.realtimeValue > realtimeThreshold {
			count += 1
		}
		if remoteTranscoders[i].stats.encodedValue > encodedThreshold {
			count += 1
		}
		if remoteTranscoders[i].stats.decodedValue > decodedThreshold {
			count += 1
		}
		remoteTranscoders[i].stats.readMutex.Unlock()
		glog.Infof("Selection: Transcoder '%s' meets %v thresholds", remoteTranscoders[i].EthereumAddr, count)
		if count == KPIThreshold {
			eligible = append(eligible, remoteTranscoders[i])
		} else if count > KPIThreshold {
			// We have a transcoder who meets more KPI's, so make the selection stricter
			KPIThreshold = count
			eligible = nil
			eligible = append(eligible, remoteTranscoders[i])
		}
	}
	glog.Infof("Selection: Filtered to transcoders who meet %v thresholds", KPIThreshold)
	if len(eligible) == 0 {
		return remoteTranscoders[0]
	}
	// Finally select our transcoder
	thisRoll := rand.Intn(len(eligible))
	glog.Infof("Selection: Rolled a %v out of %v", thisRoll+1, len(eligible))
	for i := 0; i < len(eligible); i++ {
		thisRoll -= 1
		if thisRoll < 0 {
			return eligible[i]
		}
	}
	return remoteTranscoders[0]
}

// Takes fraction% of remoteTranscoders, based on performance leaderboards
func (rtm *RemoteTranscoderManager) selectionBuckets(remoteTranscoders []*RemoteTranscoder, fraction float64) []*RemoteTranscoder {
	rtm.PoolManager.rankingMutex.Lock()
	defer rtm.PoolManager.rankingMutex.Unlock()
	rand.Seed(time.Now().UnixNano())
	offset := rand.Intn(64)
	required := int(math.Ceil(float64(len(remoteTranscoders)) * fraction))
	newRemoteTs := make([]*RemoteTranscoder, 0)
	glog.Infof("Selection: Filtering set of Transcoders to the top %d percentage (%d out of %d)", int(fraction*100), required, len(remoteTranscoders))
	encIdx := 0
	maxEncIdx := len(rtm.PoolManager.encodingLeaderboard)
	decIdx := 0
	maxDecIdx := len(rtm.PoolManager.decodingLeaderboard)
	rtrIdx := 0
	maxRTRIdx := len(rtm.PoolManager.realtimeLeaderboard)
	if maxEncIdx == 0 || maxDecIdx == 0 || maxRTRIdx == 0 {
		glog.Infof("Selection: No leaderboards available yet, so returning default set")
		return remoteTranscoders
	}
	for required > 0 {
		thisAddr := ""
		// Select ETH address from correct bucket
		if (required+offset)%3 == 1 {
			// Check ENC
			if encIdx > maxEncIdx {
				break
			}
			thisAddr = rtm.PoolManager.encodingLeaderboard[encIdx].owner
			encIdx++
			glog.Infof("Selection: `%s` is eligible based on being ranked %d in encoding", thisAddr, encIdx)
		}
		if (required+offset)%3 == 2 {
			// Check DEC
			if decIdx > maxDecIdx {
				break
			}
			thisAddr = rtm.PoolManager.encodingLeaderboard[decIdx].owner
			decIdx++
			glog.Infof("Selection: `%s` is eligible based on being ranked %d in decoding", thisAddr, decIdx)
		}
		if (required+offset)%3 == 0 {
			// Check RTR
			if rtrIdx > maxRTRIdx {
				break
			}
			thisAddr = rtm.PoolManager.encodingLeaderboard[rtrIdx].owner
			rtrIdx++
			glog.Infof("Selection: `%s` is eligible based on being ranked %d in realtime ratio", thisAddr, rtrIdx)
		}
		if thisAddr == "" {
			glog.Errorf("Selection: Unable to find a transcoder, so returning default set")
			return remoteTranscoders
		}
		// Add their remote transcoders to the eligible pool
		for i := 0; i < len(remoteTranscoders); i++ {
			if remoteTranscoders[i].stats.owner == thisAddr && required > 0 {
				newRemoteTs = append(newRemoteTs, remoteTranscoders[i])
				if i == 0 {
					remoteTranscoders = remoteTranscoders[i+1:]
				} else if i == (len(remoteTranscoders) - 1) {
					remoteTranscoders = remoteTranscoders[:i]
				} else {
					remoteTranscoders = append(remoteTranscoders[:i], remoteTranscoders[i+1:]...)
				}
				required--
				i--
			}
		}
	}

	if len(newRemoteTs) < required {
		glog.Errorf("Selection: Only found %d out of %d required transcoders, so returning default set", len(newRemoteTs), required)
		return remoteTranscoders
	}
	return newRemoteTs
}

// Sorts the gives list and selects randomly by load
func selectionByLoad(remoteTranscoders []*RemoteTranscoder) *RemoteTranscoder {
	glog.Infof("Selection: Selecting Transcoder by load")
	rand.Seed(time.Now().UnixNano())
	minLoad := remoteTranscoders[0].TotalLoad
	if minLoad > 0 {
		for _, t := range remoteTranscoders {
			if t.TotalLoad < minLoad {
				minLoad = t.TotalLoad
				if minLoad == 0 {
					break
				}
			}
		}
	}
	eligible := 0
	// Count eligible transcoders
	for _, t := range remoteTranscoders {
		if t.TotalLoad == minLoad {
			eligible++
			glog.Infof("Selection: Transcoder '%s' is eligible for selection with total load %v", t.EthereumAddr, t.TotalLoad)
		}
	}
	if eligible == 0 {
		return remoteTranscoders[0]
	}
	// Finally select our transcoder
	thisRoll := rand.Intn(eligible)
	glog.Infof("Selection: Rolled a %v out of %v", thisRoll+1, eligible)
	for i := 0; i < len(remoteTranscoders); i++ {
		if remoteTranscoders[i].TotalLoad > minLoad {
			continue
		}
		thisRoll -= 1
		if thisRoll < 0 {
			return remoteTranscoders[i]
		}
	}
	return remoteTranscoders[0]
}

// Filters
func (rtm *RemoteTranscoderManager) findCompatibleTranscoder(md *SegTranscodingMetadata) int {
	glog.Infof("Selection: Finding new Transcoder for manifest `%s`", md.ManifestID)
	// If a specific T has been requested, try them. Else do not continue with another T
	mIdLower := strings.ToLower(string(md.ManifestID))
	if strings.Contains(mIdLower, "vmtest") {
		glog.Infof("Selection: Selecting a Transcoder specified in the manifest (internal test stream)")
		for i := 0; i < len(rtm.remoteTranscoders); i++ {
			// Skip remote Transcoders with malformed data
			tAddr := strings.ToLower(rtm.remoteTranscoders[i].EthereumAddr.String())
			if strings.Contains(mIdLower, tAddr) {
				return i
			}
		}
		// Return a -1 so we don't send these streams to another T
		return -1
	}
	// If a transcoder hint is supplied in the manifest, see try to use it
	if strings.Contains(mIdLower, "hint") {
		glog.Infof("Selection: Selecting a Transcoder specified in the manifest")
		for i := 0; i < len(rtm.remoteTranscoders); i++ {
			// Skip remote Transcoders with malformed data
			tAddr := strings.ToLower(rtm.remoteTranscoders[i].EthereumAddr.String())
			if strings.Contains(mIdLower, tAddr) {
				return i
			}
		}
		glog.Infof("Selection: Selected Transcoder not connected to this region. Using default selection algo")
	}
	// Split T's into 'bad performer', untrusted and trusted transcoders
	compatibleTrusted := []*RemoteTranscoder{}
	compatibleUntrusted := []*RemoteTranscoder{}
	compatibleJailed := []*RemoteTranscoder{}
	realtimeThreshold := 0.0
	encodedThreshold := 0.0
	decodedThreshold := 0.0
	// Build temp list of eligible (un)trusted T's and init thresholds
	for i := 0; i < len(rtm.remoteTranscoders); i++ {
		rtm.remoteTranscoders[i].stats.readMutex.Lock()
		// Skip remote Transcoders with malformed data
		if rtm.remoteTranscoders[i].stats == nil {
			glog.Errorf("Selection: Skipping incompatible Transcoder '%s' as their Statistics are set to nil", rtm.remoteTranscoders[i].EthereumAddr)
			rtm.remoteTranscoders[i].stats.readMutex.Unlock()
			continue
		}
		// Skip T's who have reached their capacity
		// Plutus - updated this to check against remaining capacity. It used to check max capacity - load to see if capacity still exists
		glog.Info(fmt.Sprintf("Remaining Capacity for %s: %s", rtm.remoteTranscoders[i].EthereumAddr, rtm.remoteTranscoders[i].RemainingCap))
		if rtm.remoteTranscoders[i].RemainingCap < 1 {
			glog.Infof("Selection: Skipping Transcoder at max capacity '%s'...", rtm.remoteTranscoders[i].EthereumAddr)
			rtm.remoteTranscoders[i].stats.readMutex.Unlock()
			continue
		}
		// Skip T's who cannot transcode the segment
		if md.Caps != nil && !md.Caps.bitstring.CompatibleWith(rtm.remoteTranscoders[i].capabilities.bitstring) {
			glog.Infof("Selection: Skipping incompatible Transcoder '%s'...", rtm.remoteTranscoders[i].EthereumAddr)
			rtm.remoteTranscoders[i].stats.readMutex.Unlock()
			continue
		}
		// Split bad performing T's to a separate list
		if rtm.remoteTranscoders[i].stats.jailed == true {
			glog.Infof("Selection: Found jailed Transcoder '%s' with RTR=%v", rtm.remoteTranscoders[i].EthereumAddr, rtm.remoteTranscoders[i].stats.realtimeValue)
			compatibleJailed = append(compatibleJailed, rtm.remoteTranscoders[i])
			rtm.remoteTranscoders[i].stats.readMutex.Unlock()
			continue
		}
		// Build set of untrusted T's
		if rtm.remoteTranscoders[i].stats.decodedValue == 0 || rtm.remoteTranscoders[i].stats.encodedValue == 0 ||
			rtm.remoteTranscoders[i].stats.realtimeValue == 0 {
			compatibleUntrusted = append(compatibleUntrusted, rtm.remoteTranscoders[i])
			glog.Infof("Selection: Found untrusted Transcoder '%s'...", rtm.remoteTranscoders[i].EthereumAddr)
		} else {
			// Build set of trusted T's
			compatibleTrusted = append(compatibleTrusted, rtm.remoteTranscoders[i])
			glog.Infof("Selection: Found trusted Transcoder '%s' totalLoad=%v RTR=%.1f encoded=%.1f decoded=%.1f", rtm.remoteTranscoders[i].EthereumAddr, rtm.remoteTranscoders[i].TotalLoad,
				rtm.remoteTranscoders[i].stats.realtimeThreshold, rtm.remoteTranscoders[i].stats.encodedThreshold, rtm.remoteTranscoders[i].stats.decodedThreshold)
			if rtm.remoteTranscoders[i].stats.realtimeThreshold > realtimeThreshold {
				realtimeThreshold = rtm.remoteTranscoders[i].stats.realtimeThreshold
			}
			if rtm.remoteTranscoders[i].stats.encodedThreshold > encodedThreshold {
				encodedThreshold = rtm.remoteTranscoders[i].stats.encodedThreshold
			}
			if rtm.remoteTranscoders[i].stats.decodedThreshold > decodedThreshold {
				decodedThreshold = rtm.remoteTranscoders[i].stats.decodedThreshold
			}
		}
		rtm.remoteTranscoders[i].stats.readMutex.Unlock()
	}
	glog.Infof("Selection: We have %v trusted, %v untrusted and %v jailed transcoders", len(compatibleTrusted), len(compatibleUntrusted), len(compatibleJailed))
	if len(compatibleTrusted) == 0 && len(compatibleUntrusted) == 0 && len(compatibleJailed) == 0 {
		return -1
	}
	if len(compatibleTrusted) == 0 && len(compatibleUntrusted) == 0 {
		// If all transcoders are jailed, ignore jail status
		compatibleTrusted = compatibleJailed
	} else if len(compatibleTrusted) == 0 {
		// If we have no trusted T's, select untrusted by load
		selectedTranscoder := selectionByLoad(compatibleUntrusted)
		return rtm.getTranscoderIndex(selectedTranscoder)
	}
	// Determine selection method based on SegTranscodingMetadata
	// calculate pixels encoded per second of the target rendition
	profileString := ""
	profilePixelsPerSecond := 0
	for _, profile := range md.Profiles {
		profileString += profile.Name + ","
		res := strings.Split(profile.Resolution, "x")
		if len(res) < 2 {
			continue
		}
		width, err := strconv.Atoi(res[0])
		if err != nil {
			continue
		}
		height, err := strconv.Atoi(res[1])
		if err != nil {
			continue
		}
		profilePixelsPerSecond += width * height * int(profile.Framerate)
	}
	// Select using STRICT method, within one STDEV of the top performer in any performance indicator
	if len(md.Profiles) >= 4 || profilePixelsPerSecond >= 110592000 {
		// Profiles >= 1440px30FPS pixels per second or with 4 or more renditions
		glog.Infof("Selection: Tier 3 stream `%s`", profileString)
		selectedTranscoder := selectionTopPerformers(compatibleTrusted, realtimeThreshold, encodedThreshold, decodedThreshold)
		return rtm.getTranscoderIndex(selectedTranscoder)
	}
	if profilePixelsPerSecond >= 62208000 {
		// Profiles >= 1080px30FPS pixels per second
		glog.Infof("Selection: Tier 2 stream `%s`", profileString)
		selectedTranscoder := selectionByLoad(rtm.selectionBuckets(compatibleTrusted, 0.25))
		return rtm.getTranscoderIndex(selectedTranscoder)
	} else if profilePixelsPerSecond >= 27648000 || len(md.Profiles) > 1 {
		glog.Infof("Selection: Tier 1 stream `%s`", profileString)
		// Profiles >= 720px30FPS pixels per second
		eligibleTranscoders := append(rtm.selectionBuckets(compatibleTrusted, 0.50), compatibleUntrusted...)
		selectedTranscoder := selectionByLoad(eligibleTranscoders)
		return rtm.getTranscoderIndex(selectedTranscoder)
	} else {
		// Profiles < 720px30FPS pixels per second and 1 rendition only
		glog.Infof("Selection: Tier 0 stream `%s`", profileString)
		if len(compatibleUntrusted) > 0 {
			glog.Infof("Using the untrusted set only")
			selectedTranscoder := selectionByLoad(compatibleUntrusted)
			return rtm.getTranscoderIndex(selectedTranscoder)
		} else {
			selectedTranscoder := selectionByLoad(compatibleTrusted)
			return rtm.getTranscoderIndex(selectedTranscoder)
		}
	}
}

func (rtm *RemoteTranscoderManager) selectTranscoder(md *SegTranscodingMetadata) (*RemoteTranscoder, error) {
	sessionId := md.AuthToken.SessionId
	manifestID := string(md.ManifestID)
	rtm.RTmutex.Lock()
	defer rtm.RTmutex.Unlock()

	for i := range rtm.remoteTranscoders {
		glog.Info(i)
	}

	// While we have remote transcoders connected
	for len(rtm.remoteTranscoders) > 0 {
		// Reuse the Transcoder if we already have a session
		currentTranscoder, sessionExists := rtm.streamSessions[sessionId]
		// Switch away from jailed transcoders
		if sessionExists && currentTranscoder.stats != nil {
			currentTranscoder.stats.readMutex.Lock()
			if currentTranscoder.stats.jailed == true {
				glog.Infof("Selection: Switching away from Transcoder %s as they are currently jailed", currentTranscoder.EthereumAddr)
				rtm.completeStreamSession(sessionId, manifestID)
				currentTranscoder.stats.readMutex.Unlock()
				continue
			}
			currentTranscoder.stats.readMutex.Unlock()
		}
		// If not, select a new Transcoder
		if !sessionExists {
			// Transcoders are at capacity
			if rtm.transcodersAtCapacity() {
				return nil, ErrNoTranscodersAvailable
			}
			lastCompatibleTranscoder := rtm.findCompatibleTranscoder(md)
			// Transcoders are not capable of transcoding the segment
			if lastCompatibleTranscoder == -1 {
				return nil, ErrNoCompatibleTranscodersAvailable
			}
			currentTranscoder = rtm.remoteTranscoders[lastCompatibleTranscoder]
			glog.Infof("Selection: Selected compatible Transcoder %s", currentTranscoder.EthereumAddr)
		} else {
			glog.Infof("Selection: Reusing Transcoder %s", currentTranscoder.EthereumAddr)
		}
		// Check if the selected Transcoder is still live
		if _, ok := rtm.liveTranscoders[currentTranscoder.stream]; !ok {
			// Remove the stream session because the transcoder is no longer live
			if sessionExists {
				rtm.completeStreamSession(sessionId, manifestID)
			}
			// transcoder does not exist in table; remove and retry
			rtm.remoteTranscoders = removeFromRemoteTranscoders(currentTranscoder, rtm.remoteTranscoders)
			glog.Infof("Selection: Skipping Transcoder %s as they are no longer live (they reconnected or disconnected)", currentTranscoder.EthereumAddr)
			continue
		}
		// Open a new transcoding session to the selected Transcoder
		if !sessionExists {
			rtm.streamSessions[sessionId] = currentTranscoder
			currentTranscoder.LocalLoad++
			glog.Infof("Created new session for Transcoder %s. localLoad %d totalLoad %d/%d", currentTranscoder.EthereumAddr, currentTranscoder.LocalLoad, currentTranscoder.TotalLoad, currentTranscoder.Capacity)
			if monitor.Enabled {
				monitor.SetTranscoderLocalLoad(currentTranscoder.EthereumAddr.String(), currentTranscoder.LocalLoad)
			}

			var sessionIdlist string = ""
			for mid := range currentTranscoder.node.SegmentChans {
				sessionIdlist += (string(mid) + ",")
			}

			rtm.MqttBroker.PublishTranscoderConnection(currentTranscoder, false, "orchestrator.go:1512")
		}
		return currentTranscoder, nil
	}

	return nil, ErrNoTranscodersAvailable
}

// ends transcoding session and releases resources
func (node *LivepeerNode) EndTranscodingSession(sessionId string) {
	logCtx := context.TODO()
	clog.V(common.DEBUG).Infof(logCtx, "Transcoding session ended by the Broadcaster for sessionID=%v", sessionId)
	node.endTranscodingSession(sessionId, "", logCtx)
}

func (node *RemoteTranscoderManager) EndTranscodingSession(sessionId string) {
	panic("shouldn't be called on RemoteTranscoderManager")
}

// completeStreamSessions end a stream session for a remote transcoder and decrements its load
// caller should hold the mutex lock
func (rtm *RemoteTranscoderManager) completeStreamSession(sessionId string, manifestID string) {
	t, ok := rtm.streamSessions[sessionId]
	if !ok {
		return
	}
	t.LocalLoad--
	glog.Infof("Ended session for Transcoder %s. localLoad %d totalLoad %d/%d", t.EthereumAddr, t.LocalLoad, t.TotalLoad, t.Capacity)
	if monitor.Enabled {
		monitor.SetTranscoderLocalLoad(t.EthereumAddr.String(), t.LocalLoad)
	}

	var sessionIdlist string = ""
	for mid := range t.node.SegmentChans {
		if string(mid) == sessionId {
			continue
		}
		sessionIdlist += (string(mid) + ",")
	}

	rtm.MqttBroker.PublishTranscoderConnection(t, false, "orchestrator.go:1552")
	delete(rtm.streamSessions, sessionId)
}

// Caller of this function should hold RTmutex lock
func (rtm *RemoteTranscoderManager) totalLoadAndCapacity() (load int, capacity int, remainingCap int, numLiveTranscoders int) {
	for _, t := range rtm.liveTranscoders {
		load += t.LocalLoad
		capacity += t.Capacity
		remainingCap += t.RemainingCap
	}

	numLiveTranscoders = len(rtm.liveTranscoders)
	return
}

// Caller of this function should hold RTmutex lock
func (rtm *RemoteTranscoderManager) transcodersAtCapacity() bool {
	for _, t := range rtm.liveTranscoders {
		if t.TotalLoad < t.Capacity {
			return false
		}
	}
	return true
}

// Transcode does actual transcoding using remote transcoder from the pool
func (rtm *RemoteTranscoderManager) Transcode(ctx context.Context, md *SegTranscodingMetadata) (*TranscodeData, error) {
	currentTranscoder, err := rtm.selectTranscoder(md)
	if err != nil {
		return nil, err
	}
	res, err := currentTranscoder.Transcode(ctx, md)
	if err != nil {
		rtm.RTmutex.Lock()
		rtm.completeStreamSession(md.AuthToken.SessionId, string(md.ManifestID))
		rtm.RTmutex.Unlock()
	}
	_, fatal := err.(RemoteTranscoderFatalError)
	if fatal {
		// Don't retry if we've timed out; broadcaster likely to have moved on
		// XXX problematic for VOD when we *should* retry
		if err.(RemoteTranscoderFatalError).error == ErrRemoteTranscoderTimeout {
			return res, err
		}
		return rtm.Transcode(ctx, md)
	}

	return res, err
}
