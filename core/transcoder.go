package core

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"

	ethcommon "github.com/ethereum/go-ethereum/common"

	gonet "net"

	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/livepeer/go-livepeer/clog"
	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/go-livepeer/net"
	"github.com/livepeer/lpms/ffmpeg"
)

type Transcoder interface {
	Transcode(ctx context.Context, md *SegTranscodingMetadata) (*TranscodeData, error)
	EndTranscodingSession(sessionId string)
}

type LocalTranscoder struct {
	workDir string
}

type UnrecoverableError struct {
	error
}

func NewUnrecoverableError(err error) UnrecoverableError {
	return UnrecoverableError{err}
}

var WorkDir string

func setEffectiveDetectorConfig(md *SegTranscodingMetadata) {
	aiEnabled := DetectorProfile != nil
	actualProfile := DetectorProfile
	if aiEnabled {
		presetSampleRate := DetectorProfile.(*ffmpeg.SceneClassificationProfile).SampleRate
		if md.DetectorEnabled && len(md.DetectorProfiles) == 1 {
			actualProfile = md.DetectorProfiles[0]
			requestedSampleRate := actualProfile.(*ffmpeg.SceneClassificationProfile).SampleRate
			// 0 is not a valid value
			if requestedSampleRate == 0 {
				requestedSampleRate = math.MaxUint32
			}
			actualProfile.(*ffmpeg.SceneClassificationProfile).SampleRate = uint(math.Min(float64(presetSampleRate),
				float64(requestedSampleRate)))
			// copy other fields from default AI capability, as we don't yet support custom ones
			actualProfile.(*ffmpeg.SceneClassificationProfile).ModelPath = DetectorProfile.(*ffmpeg.SceneClassificationProfile).ModelPath
			actualProfile.(*ffmpeg.SceneClassificationProfile).Input = DetectorProfile.(*ffmpeg.SceneClassificationProfile).Input
			actualProfile.(*ffmpeg.SceneClassificationProfile).Output = DetectorProfile.(*ffmpeg.SceneClassificationProfile).Output
			actualProfile.(*ffmpeg.SceneClassificationProfile).Classes = DetectorProfile.(*ffmpeg.SceneClassificationProfile).Classes
		}
	}
	if actualProfile != nil && actualProfile.(*ffmpeg.SceneClassificationProfile).SampleRate < math.MaxUint32 {
		md.DetectorProfiles = []ffmpeg.DetectorProfile{actualProfile}
		md.DetectorEnabled = true
	} else {
		md.DetectorProfiles = []ffmpeg.DetectorProfile{}
		md.DetectorEnabled = false
	}
}

func (lt *LocalTranscoder) Transcode(ctx context.Context, md *SegTranscodingMetadata) (td *TranscodeData, retErr error) {
	// Returns UnrecoverableError instead of panicking to gracefully notify orchestrator about transcoder's failure
	defer recoverFromPanic(&retErr)

	// Set up in / out config
	in := &ffmpeg.TranscodeOptionsIn{
		Fname: md.Fname,
		Accel: ffmpeg.Software,
	}
	setEffectiveDetectorConfig(md)
	profiles := md.Profiles
	opts := profilesToTranscodeOptions(lt.workDir, ffmpeg.Software, profiles, md.CalcPerceptualHash, md.SegmentParameters)
	if md.DetectorEnabled {
		opts = append(opts, detectorsToTranscodeOptions(lt.workDir, ffmpeg.Software, md.DetectorProfiles)...)
	}

	_, seqNo, parseErr := parseURI(md.Fname)
	start := time.Now()

	res, err := ffmpeg.Transcode3(in, opts)
	if err != nil {
		return nil, err
	}

	if monitor.Enabled && parseErr == nil {
		// This will run only when fname is actual URL and contains seqNo in it.
		// When orchestrator works as transcoder, `fname` will be relative path to file in local
		// filesystem and will not contain seqNo in it. For that case `SegmentTranscoded` will
		// be called in orchestrator.go
		monitor.SegmentTranscoded(ctx, 0, seqNo, md.Duration, time.Since(start), common.ProfilesNames(profiles), true, true)
	}

	return resToTranscodeData(ctx, res, opts)
}

func (lt *LocalTranscoder) EndTranscodingSession(sessionId string) {
	// no-op for software transcoder
}

func NewLocalTranscoder(workDir string) Transcoder {
	return &LocalTranscoder{workDir: workDir}
}

type NvidiaTranscoder struct {
	device  string
	session *ffmpeg.Transcoder
}

type NetintTranscoder struct {
	device  string
	session *ffmpeg.Transcoder
}

func (nv *NetintTranscoder) Transcode(ctx context.Context, md *SegTranscodingMetadata) (td *TranscodeData, retErr error) {
	// Returns UnrecoverableError instead of panicking to gracefully notify orchestrator about transcoder's failure
	defer recoverFromPanic(&retErr)

	in := &ffmpeg.TranscodeOptionsIn{
		Fname:  md.Fname,
		Accel:  ffmpeg.Netint,
		Device: nv.device,
	}
	profiles := md.Profiles
	out := profilesToTranscodeOptions(WorkDir, ffmpeg.Netint, profiles, md.CalcPerceptualHash, md.SegmentParameters)
	if md.DetectorEnabled {
		out = append(out, detectorsToTranscodeOptions(WorkDir, ffmpeg.Netint, md.DetectorProfiles)...)
	}

	_, seqNo, parseErr := parseURI(md.Fname)
	start := time.Now()

	res, err := nv.session.Transcode(in, out)
	if err != nil {
		return nil, err
	}

	if monitor.Enabled && parseErr == nil {
		// This will run only when fname is actual URL and contains seqNo in it.
		// When orchestrator works as transcoder, `fname` will be relative path to file in local
		// filesystem and will not contain seqNo in it. For that case `SegmentTranscoded` will
		// be called in orchestrator.go
		monitor.SegmentTranscoded(ctx, 0, seqNo, md.Duration, time.Since(start), common.ProfilesNames(profiles), true, true)
	}

	return resToTranscodeData(ctx, res, out)
}

func (lt *LocalTranscoder) Stop() {
	//no-op for software transcoder
}

func (nv *NvidiaTranscoder) Transcode(ctx context.Context, md *SegTranscodingMetadata) (td *TranscodeData, retErr error) {
	// Returns UnrecoverableError instead of panicking to gracefully notify orchestrator about transcoder's failure
	defer recoverFromPanic(&retErr)

	in := &ffmpeg.TranscodeOptionsIn{
		Fname:  md.Fname,
		Accel:  ffmpeg.Nvidia,
		Device: nv.device,
	}
	profiles := md.Profiles
	setEffectiveDetectorConfig(md)
	out := profilesToTranscodeOptions(WorkDir, ffmpeg.Nvidia, profiles, md.CalcPerceptualHash, md.SegmentParameters)
	if md.DetectorEnabled {
		out = append(out, detectorsToTranscodeOptions(WorkDir, ffmpeg.Nvidia, md.DetectorProfiles)...)
	}

	_, seqNo, parseErr := parseURI(md.Fname)
	start := time.Now()

	res, err := nv.session.Transcode(in, out)
	if err != nil {
		return nil, err
	}

	if monitor.Enabled && parseErr == nil {
		// This will run only when fname is actual URL and contains seqNo in it.
		// When orchestrator works as transcoder, `fname` will be relative path to file in local
		// filesystem and will not contain seqNo in it. For that case `SegmentTranscoded` will
		// be called in orchestrator.go
		monitor.SegmentTranscoded(ctx, 0, seqNo, md.Duration, time.Since(start), common.ProfilesNames(profiles), true, true)
	}

	return resToTranscodeData(ctx, res, out)
}

func (nv *NvidiaTranscoder) EndTranscodingSession(sessionId string) {
	nv.Stop()
}

func (nt *NetintTranscoder) EndTranscodingSession(sessionId string) {
	nt.Stop()
}

type transcodeTestParams struct {
	TestAvailable bool
	Cap           Capability
	OutProfile    ffmpeg.VideoProfile
	SegmentPath   string
}

func (params transcodeTestParams) IsRequired() bool {
	return InArray(params.Cap, DefaultCapabilities())
}

func (params transcodeTestParams) Kind() string {
	if params.IsRequired() {
		return "required capability"
	}
	return "optional capability"
}

func (params transcodeTestParams) Name() string {
	name, err := CapabilityToName(params.Cap)
	if err == nil {
		return name
	}
	return "unknown"
}

type continueLoop bool

func transcodeWithSample(handler func(*transcodeTestParams) continueLoop) {
	// default capabilities
	allCaps := append(DefaultCapabilities(), OptionalCapabilities()...)
	handlerParams := transcodeTestParams{SegmentPath: filepath.Join(WorkDir, "testseg.tempfile")}
	defer os.Remove(handlerParams.SegmentPath)
	for _, handlerParams.Cap = range allCaps {
		var capTest CapabilityTest
		capTest, handlerParams.TestAvailable = CapabilityTestLookup[handlerParams.Cap]
		if handlerParams.TestAvailable {
			handlerParams.OutProfile = capTest.outProfile
			b := bytes.NewReader(capTest.inVideoData)
			z, err := gzip.NewReader(b)
			if err != nil {
				continue
			}
			mp4testSeg, err := ioutil.ReadAll(z)
			z.Close()
			if err != nil {
				glog.Errorf("error reading test segment for capability %d: %s", handlerParams.Cap, err)
				continue
			}
			err = ioutil.WriteFile(handlerParams.SegmentPath, mp4testSeg, 0644)
			if err != nil {
				glog.Errorf("error writing test segment for capability %d: %s", handlerParams.Cap, err)
				continue
			}
		}
		if !handler(&handlerParams) {
			return
		}
	}
}

func testAccelTranscode(device string, tf func(device string) TranscoderSession, fname string, profile ffmpeg.VideoProfile, renditionCount int) (outputProduced, outputValid bool, err error) {
	transcoder := tf(device)
	outputProfiles := make([]ffmpeg.VideoProfile, 0, renditionCount)
	for i := 0; i < renditionCount; i++ {
		outputProfiles = append(outputProfiles, profile)
	}
	metadata := &SegTranscodingMetadata{Fname: fname, Profiles: outputProfiles}
	td, err := transcoder.Transcode(context.Background(), metadata)
	transcoder.Stop()
	if err != nil {
		return false, false, err
	}
	outputProduced = len(td.Segments) > 0
	outputValid = td.Pixels > 0
	return outputProduced, outputValid, err
}

// Test which capabilities transcoder supports
func TestTranscoderCapabilities(devices []string, tf func(device string) TranscoderSession) (caps []Capability, fatalError error) {
	// disable logging, unless verbosity is set
	vFlag := flag.Lookup("v").Value.String()
	detailsMsg := ""
	if vFlag == "" {
		detailsMsg = ", set verbosity level to see more details"
		logLevel := ffmpeg.FfmpegGetLogLevel()
		defer ffmpeg.FfmpegSetLogLevel(logLevel)
		ffmpeg.FfmpegSetLogLevel(0)
		ffmpeg.LogTranscodeErrors = false
		defer func() { ffmpeg.LogTranscodeErrors = true }()
	}
	fatalError = nil
	transcodeWithSample(func(params *transcodeTestParams) continueLoop {
		if !params.TestAvailable {
			// Assume capability is supported if we do not have test for it
			caps = append(caps, params.Cap)
			return true
		}
		runRestrictedSessionTest := true
		transcodingFailed := func() {
			// check GeForce limit
			if runRestrictedSessionTest {
				// do it only once
				runRestrictedSessionTest = false
				// if 4 renditions didn't succeed, try 3 renditions on first device to check if it could be session limit
				outputProduced, outputValid, err := testAccelTranscode(devices[0], tf, params.SegmentPath, params.OutProfile, 3)
				if err != nil && outputProduced && outputValid {
					glog.Error("Maximum number of simultaneous NVENC video encoding sessions is restricted by driver")
					fatalError = fmt.Errorf("maximum number of simultaneous NVENC video encoding sessions is restricted by driver")
				}
			}
			if params.IsRequired() {
				// All devices need to support this capability, stop further testing
				fatalError = fmt.Errorf("%s %q is not supported on hardware", params.Kind(), params.Name())
			}
		}
		// check that capability is supported on all devices
		for _, device := range devices {
			outputProduced, outputValid, err := testAccelTranscode(device, tf, params.SegmentPath, params.OutProfile, 4)
			if err != nil {
				glog.Infof("%s %q is not supported on device %s%s", params.Kind(), params.Name(), device, detailsMsg)
				// likely means capability is not supported, don't check on other devices
				transcodingFailed()
				return fatalError == nil
			}
			if !outputProduced || !outputValid {
				// abnormal behavior
				glog.Errorf("Empty result segment when testing for %s %q", params.Kind(), params.Name())
				transcodingFailed()
				return fatalError == nil
			}
			// no error creating 4 renditions - disable 3 renditions test, as restriction is on driver level, not device
			runRestrictedSessionTest = false
		}
		caps = append(caps, params.Cap)
		return true
	})
	return caps, fatalError
}

func testSoftwareTranscode(tmpdir string, fname string, profile ffmpeg.VideoProfile, renditionCount int) (outputProduced, outputValid bool, err error) {
	transcoder := NewLocalTranscoder(tmpdir)
	outputProfiles := make([]ffmpeg.VideoProfile, 0, renditionCount)
	for i := 0; i < renditionCount; i++ {
		outputProfiles = append(outputProfiles, profile)
	}
	metadata := &SegTranscodingMetadata{Fname: fname, Profiles: outputProfiles}
	td, err := transcoder.Transcode(context.Background(), metadata)
	if err != nil {
		return false, false, err
	}
	outputProduced = len(td.Segments) > 0
	outputValid = td.Pixels > 0
	return outputProduced, outputValid, err
}

func TestSoftwareTranscoderCapabilities(tmpdir string) (caps []Capability, fatalError error) {
	// iterate all capabilities and test ones which has test data
	fatalError = nil
	transcodeWithSample(func(params *transcodeTestParams) continueLoop {
		if !params.TestAvailable {
			caps = append(caps, params.Cap)
			return true
		}
		// check that capability is supported on all devices
		outputProduced, outputValid, err := testSoftwareTranscode(tmpdir, params.SegmentPath, params.OutProfile, 4)
		if err != nil {
			// likely means capability is not supported
			return true
		}
		if !outputProduced || !outputValid {
			// abnormal behavior
			fatalError = fmt.Errorf("empty result segment when testing for capability %d", params.Cap)
			return false
		}
		caps = append(caps, params.Cap)
		return true
	})
	return caps, fatalError
}

func GetTranscoderFactoryByAccel(acceleration ffmpeg.Acceleration) (func(device string) TranscoderSession, func(detector ffmpeg.DetectorProfile, gpu string) (TranscoderSession, error), error) {
	switch acceleration {
	case ffmpeg.Nvidia:
		return NewNvidiaTranscoder, NewNvidiaTranscoderWithDetector, nil
	case ffmpeg.Netint:
		return NewNetintTranscoder, nil, nil
	default:
		return nil, nil, ffmpeg.ErrTranscoderHw
	}
}

func NewNvidiaTranscoder(gpu string) TranscoderSession {
	return &NvidiaTranscoder{
		device:  gpu,
		session: ffmpeg.NewTranscoder(),
	}
}

func NewNetintTranscoder(gpu string) TranscoderSession {
	return &NetintTranscoder{
		device:  gpu,
		session: ffmpeg.NewTranscoder(),
	}
}

func NewNvidiaTranscoderWithDetector(detector ffmpeg.DetectorProfile, gpu string) (TranscoderSession, error) {
	// Hardcode detection to device 0 for now
	// Transcoding can still run on a separate GPU as we copy frames to CPU before detection
	session, err := ffmpeg.NewTranscoderWithDetector(detector, gpu)
	return &NvidiaTranscoder{
		device:  gpu,
		session: session,
	}, err
}

func (nv *NvidiaTranscoder) Stop() {
	nv.session.StopTranscoder()
}

func (nv *NetintTranscoder) Stop() {
	nv.session.StopTranscoder()
}

func parseURI(uri string) (string, uint64, error) {
	var mid string
	var seqNo uint64
	parts := strings.Split(uri, "/")
	if len(parts) < 3 {
		return mid, seqNo, fmt.Errorf("BadURI")
	}
	mid = parts[len(parts)-2]
	parts = strings.Split(parts[len(parts)-1], ".")
	seqNo, err := strconv.ParseUint(parts[0], 10, 64)
	return mid, seqNo, err
}

func resToTranscodeData(ctx context.Context, res *ffmpeg.TranscodeResults, opts []ffmpeg.TranscodeOptions) (*TranscodeData, error) {
	if len(res.Encoded) != len(opts) {
		return nil, errors.New("lengths of results and options different")
	}

	// Convert results into in-memory bytes following the expected API
	segments := []*TranscodedSegmentData{}
	// Extract detection data from detector outputs
	detections := []ffmpeg.DetectData{}
	for i := range opts {
		if opts[i].Detector == nil {
			oname := opts[i].Oname
			o, err := ioutil.ReadFile(oname)
			if err != nil {
				clog.Errorf(ctx, "Cannot read transcoded output for name=%s", oname)
				return nil, err
			}
			// Extract perceptual hash if calculated
			var s []byte = nil
			if opts[i].CalcSign {
				sigfile := oname + ".bin"
				s, err = ioutil.ReadFile(sigfile)
				if err != nil {
					clog.Errorf(ctx, "Cannot read perceptual hash at name=%s", sigfile)
					return nil, err
				}
				err = os.Remove(sigfile)
				if err != nil {
					clog.Errorf(ctx, "Cannot delete perceptual hash after reading name=%s", sigfile)
				}
			}
			segments = append(segments, &TranscodedSegmentData{Data: o, Pixels: res.Encoded[i].Pixels, PHash: s})
			os.Remove(oname)
		} else {
			detections = append(detections, res.Encoded[i].DetectData)
		}
	}

	return &TranscodeData{
		Segments:   segments,
		Pixels:     res.Decoded.Pixels,
		Detections: detections,
	}, nil
}

func profilesToTranscodeOptions(workDir string, accel ffmpeg.Acceleration, profiles []ffmpeg.VideoProfile, calcPHash bool,
	segPar *SegmentParameters) []ffmpeg.TranscodeOptions {

	opts := make([]ffmpeg.TranscodeOptions, len(profiles))
	for i := range profiles {
		o := ffmpeg.TranscodeOptions{
			Oname:        fmt.Sprintf("%s/out_%s.tempfile", workDir, common.RandName()),
			Profile:      profiles[i],
			Accel:        accel,
			AudioEncoder: ffmpeg.ComponentOptions{Name: "copy"},
			CalcSign:     calcPHash,
		}
		if segPar != nil && segPar.Clip != nil {
			o.From = segPar.Clip.From
			o.To = segPar.Clip.To
		}
		opts[i] = o
	}
	return opts
}

func detectorsToTranscodeOptions(workDir string, accel ffmpeg.Acceleration, profiles []ffmpeg.DetectorProfile) []ffmpeg.TranscodeOptions {
	opts := make([]ffmpeg.TranscodeOptions, len(profiles))
	for i := range profiles {
		var o ffmpeg.TranscodeOptions
		switch profiles[i].Type() {
		case ffmpeg.SceneClassification:
			classifier := profiles[i].(*ffmpeg.SceneClassificationProfile)
			classifier.ModelPath = ffmpeg.DSceneAdultSoccer.ModelPath
			classifier.Input = ffmpeg.DSceneAdultSoccer.Input
			classifier.Output = ffmpeg.DSceneAdultSoccer.Output
			o = ffmpeg.TranscodeOptions{
				Detector: classifier,
				Accel:    accel,
			}
		}
		opts[i] = o
	}
	return opts
}

func recoverFromPanic(retErr *error) {
	if r := recover(); r != nil {
		err, ok := r.(error)
		if !ok {
			err = errors.New("unrecoverable transcoding failure")
		}
		*retErr = NewUnrecoverableError(err)
	}
}

// Try adding transcoder stuff from server package here
//---------------------------------------------------------------------------------------------------------------------

const protoVerLPT = "videominer-1.2-525e816"
const depProtoVerLPT = "videominer-1.2-31a7016"
const transcodingErrorMimeType = "livepeer/transcoding-error"

var errSecret = errors.New("invalid secret")
var errReadFromServer = errors.New("error reading from server: EOF")
var errOrchoffline = errors.New("orchestrator offline")
var errZeroCapacity = errors.New("zero capacity")
var errInterrupted = errors.New("execution interrupted")
var errCapabilities = errors.New("incompatible segment capabilities")
var errNoEthAddress = errors.New("no ethereum address")
var errPoolVersion = errors.New("invalid pool version")
var errNoAuth = errors.New("not authenticated")
var errNoOrchestrator = errors.New("no Orchestrator near your area")
var errAlreadyConnected = errors.New("already has an active connection to this Orchestrator")
var errSegEncoding = errors.New("ErrorSegEncoding")
var errSegSig = errors.New("ErrSegSig")
var errFormat = errors.New("unrecognized profile output format")
var errProfile = errors.New("unrecognized encoder profile")
var errEncoder = errors.New("unrecognized video codec")
var errDuration = errors.New("invalid duration")
var errCapCompat = errors.New("incompatible capabilities")
var errLatencyTestFailed = errors.New("latency test failed")

type TranscodedSegmentStats struct {
	UID           string `json:"uid"`
	LocalLoad     int    `json:"localload"`
	TotalLoad     int    `json:"totalload"`
	SessionId     string `json:"sessionid"`
	SessionList   string `json:"sessionlist"`
	SegmentLength string `json:"segmentlength"`
	Duration      int64  `json:"duration"`
	PixelsDecoded int64  `json:"pixelsdecoded"`
	PixelsEncoded int64  `json:"pixelsencoded"`
	Price         int64  `json:"price"`
	Took          int64  `json:"took"`
	Profile       string `json:"profile"`
	Timestamp     int64  `json:"timestamp"`
}

func (n *LivepeerNode) StopTranscoder(id string) {
	go func() {
		defer delete(n.TStopChans, id)
		defer close(n.TStopChans[id])
		n.TStopChans[id] <- "Stop"
	}()
}

const GRPCConnectTimeout = 3 * time.Second
const GRPCTimeout = 8 * time.Second

var tlsConfig = &tls.Config{InsecureSkipVerify: true}

func startOrchestratorClient(ctx context.Context, uri *url.URL) (net.OrchestratorClient, *grpc.ClientConn, error) {
	clog.V(common.DEBUG).Infof(ctx, "Connecting RPC to uri=%v", uri)
	conn, err := grpc.Dial(uri.Host,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithBlock(),
		grpc.WithTimeout(GRPCConnectTimeout))
	if err != nil {
		return nil, nil, fmt.Errorf("did not connect to orch %s", uri)

	}
	c := net.NewOrchestratorClient(conn)

	return c, conn, nil
}

func sendPing(orchAddr *url.URL, value []byte) (*net.PingPong, error) {
	orchClient, conn, err := startOrchestratorClient(context.Background(), orchAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), GRPCTimeout)
	defer cancel()

	return orchClient.Ping(ctx, &net.PingPong{Value: value})
}

func testOrchestrator(orchUri *url.URL) string {
	ctx, cancel := context.WithTimeout(context.Background(), GRPCConnectTimeout)
	defer cancel()
	pong := make(chan string)
	go func(uri *url.URL) {
		_, err := sendPing(uri, nil)
		if err != nil {
			return
		}
		pong <- uri.String()
	}(orchUri)

	select {
	case <-ctx.Done():
		return ""
	case orch := <-pong:
		return orch
	}
}

func ManageTranscoderInstance(n *LivepeerNode, id string) {
	// PlutusOrchs thread safety
	n.poMu.Lock()
	defer n.poMu.Unlock()

	orch, o_exists := n.PlutusOrchs[id]
	if !o_exists {
		_, ok := n.TStopChans[id]
		if ok {
			n.StopTranscoder(id)
		}
		glog.Infof("Orchestrator %s Is no longer a member of the pool. Closing connection", id)
		return
	}

	if !orch.Online {
		//-----------------------------
		// Commented this section to prevent Transcoders from disconnecting when orchestrator offline message results from temporary outage with mqtt server.
		//-----------------------------
		// _, ok := n.TStopChans[id]
		// if ok {
		// 	n.StopTranscoder(id)
		// 	glog.Infof("Orchestrator %s is offline. Closing connection", id)
		// }
		return
	}

	if !orch.Whitelisted {
		_, ok := n.TStopChans[id]
		if ok {
			n.StopTranscoder(id)
			glog.Infof("Orchestrator %s has been blacklisted. Closing connection", id)
		}
		return
	}

	if orch.InsufficientBalance {
		_, ok := n.TStopChans[id]
		if ok {
			n.StopTranscoder(id)
			glog.Infof("Orchestrator %s has insufficient balance of eth deposited. Closing connection", id)
		}
		return
	}

	latencyThreshold := *n.LivepeerConfig.LatencyThreshold
	if orch.Latency <= 0 {
		segments := strings.Split(orch.ServiceURI, ":")
		parsed, _ := url.Parse(fmt.Sprintf("https://%s/", segments[0]))
		host := parsed.Host
		port := segments[1]
		ips, _ := gonet.LookupIP(host)

		var latencyDiminshing int64
		for i, ip := range ips {
			if ipv4 := ip.To4(); ipv4 != nil {
				glog.Infof("DNS %v -> %v ", orch.ServiceURI, "https://"+ipv4.String()+":"+port)
				thisUrlObj, err := url.ParseRequestURI("https://" + ipv4.String() + ":" + port)
				if err != nil {
					glog.Error("Skipping due to a failed GRPC connection test: ", err)
					return
				} else {
					start := time.Now()
					thisUrlPinged := testOrchestrator(thisUrlObj)
					latency := int64(time.Since(start).Milliseconds())
					if thisUrlPinged == "" {
						glog.Warning(fmt.Sprintf("Skipping due to a failed GRPC connection test to %v", thisUrlPinged))
						return
					} else {
						glog.Infof("GRPC test to `%v` took %v ms", thisUrlPinged, latency)

						if i == 0 {
							latencyDiminshing = latency + 1
						}

						if latency < latencyDiminshing {
							latencyDiminshing = latency
							orch.Latency = latency
							orch.ServiceURI = ipv4.String() + ":" + port
						}
					}
				}
			}
		}
	}

	if orch.Latency > latencyThreshold {
		glog.V(common.VERBOSE).Infof("Latency for orch `%s` is %v, which is greater than theshold of %v", orch.ServiceURI, orch.Latency, latencyThreshold)
		return
	}

	_, existingSession := n.TStopChans[id]
	if !existingSession {
		glog.Infof("Start new session with %s", orch.ServiceURI)
		go RunTranscoder(n, orch.ServiceURI, orch.Id, orch.Latency)
	}

}

func checkTranscoderError(err error) error {
	if err != nil {
		s := status.Convert(err)
		if s.Message() == errSecret.Error() { // consider this unrecoverable
			return NewRemoteTranscoderFatalError(errSecret)
		}
		if s.Message() == errReadFromServer.Error() { // consider this unrecoverable
			return NewRemoteTranscoderFatalError(errReadFromServer)
		}
		if s.Message() == errZeroCapacity.Error() { // consider this unrecoverable
			return NewRemoteTranscoderFatalError(errZeroCapacity)
		}
		if s.Message() == errOrchoffline.Error() { // consider this unrecoverable
			return NewRemoteTranscoderFatalError(errOrchoffline)
		}
		if s.Message() == errLatencyTestFailed.Error() { // consider this unrecoverable
			return NewRemoteTranscoderFatalError(errLatencyTestFailed)
		}
		if status.Code(err) == codes.Canceled {
			return NewRemoteTranscoderFatalError(errInterrupted)
		}
	}
	return err
}

func RunTranscoder(n *LivepeerNode, orchAddr string, orchId string, latency int64) {
	defer glog.Info("Transcoder loop closed: ", orchAddr)

	n.TStopChans[orchId] = make(chan string)

loop:
	for {
		select {
		case t := <-n.TStopChans[orchId]:
			glog.Infof("Exiting Livepeer Transcoder: %v", t)
			// Cancelling context will close connection to orchestrator
			//cancel()
			break loop
		default:
			glog.Info("Registering transcoder to ", orchAddr)
			err := runTranscoder(n, orchAddr, orchId, latency)
			glog.Info("%v Unregistering transcoder: ", orchAddr, err)
			if _, fatal := err.(RemoteTranscoderFatalError); fatal {
				glog.Info("Terminating transcoder because of ", err)
				// Returning nil here will make `backoff` to stop trying to reconnect and exit
				n.StopTranscoder(orchId)

				//break loop
			}
			// By returning error we tell `backoff` to try to connect again
			select {
			case <-n.TStopChans[orchId]:
				glog.Info("Cancel transcoder connection retry")
				break loop
			case <-time.After(5 * time.Second):
			}

		}
	}
}

func runTranscoder(n *LivepeerNode, orchAddr string, orchId string, latency int64) error {

	ethAddr := ethcommon.HexToAddress(*n.LivepeerConfig.EthAcctAddr)
	//n.TStopChans[orchId] = make(chan string)
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	conn, err := grpc.Dial(orchAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	if err != nil {
		glog.Error("Did not connect transcoder to orchesrator: ", err)
		return err
	}
	defer conn.Close()

	c := net.NewTranscoderClient(conn)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	// Silence linter
	defer cancel()

	_, remainingCap := n.GetTranscoderLoadAndCapacity()
	r, err := c.RegisterTranscoder(ctx, &net.RegisterRequest{
		Secret: n.OrchSecret,
		//try changing to core.MaxSessions
		Capacity:        int64(MaxSessions),
		RemainingCap:    int64(remainingCap),
		Capabilities:    NewCapabilities(n.TranscoderCaps, []Capability{}).ToNetCapabilities(),
		EthereumAddress: ethAddr.Bytes(),
		TranscoderID:    n.UID,
		PoolVersion:     protoVerLPT,
		LatencyTest:     latency,
		// For testing latency error uncomment below and comment above.
		// LatencyTest: 600,
	})
	if err := checkTranscoderError(err); err != nil {
		glog.Error("Could not register transcoder to orchestrator ", err)
		return err
	}
	glog.Info("***** Transcoder succesfully started! *****")
	glog.Infof("Connected to: %v", orchAddr)
	glog.Info("Waiting for segments...")

	//Catch interrupt signal to shut down transcoder
	exitc := make(chan os.Signal)
	signal.Notify(exitc, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(exitc)

	// this code isn't working for some reason... after a transcoder stops because the orchestrator disconnects, and then reconnected when
	// the orchestrator restarted, the 'connected' flag is still 'false'
	n.PlutusOrchs[orchId].Connected = true
	defer func() {
		n.PlutusOrchs[orchId].Connected = false
	}()

	go func() {
		select {
		case sig := <-exitc:
			glog.Infof("Exiting Livepeer Transcoder: %v", sig)
			// Cancelling context will close connection to orchestrator
			cancel()
			return
		case t := <-n.TStopChans[orchId]:
			glog.Infof("Exiting Livepeer Transcoder: %v", t)
			// Cancelling context will close connection to orchestrator
			cancel()
			return
		}
	}()

	httpc := &http.Client{Transport: &http2.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	var wg sync.WaitGroup
	for {
		notify, err := r.Recv()
		if err := checkTranscoderError(err); err != nil {
			glog.Infof(`%v End of stream receive cycle because of err=%q, waiting for running transcode jobs to complete`, orchAddr, err)
			wg.Wait()
			return err
		}
		if notify.SegData != nil && notify.SegData.AuthToken != nil && len(notify.SegData.AuthToken.SessionId) > 0 && len(notify.Url) == 0 {
			// session teardown signal
			n.Transcoders["base"].EndTranscodingSession(notify.SegData.AuthToken.SessionId)

		} else {
			wg.Add(1)
			go func() {
				runTranscode(n, orchAddr, httpc, notify, orchId)
				wg.Done()
			}()
		}
	}
}

func runTranscode(n *LivepeerNode, orchAddr string, httpc *http.Client, notify *net.NotifySegment, orchId string) {
	md, err := coreSegMetadata(notify.SegData)

	//plutus - add manifest to active list if not exist, or updates last seen time
	n.ActiveManifestAddUpdate(md.ManifestID)

	glog.Infof("Transcoding taskId=%d url=%s tId=%s", notify.TaskId, notify.Url, orchId)
	var contentType, fname string
	var body bytes.Buffer
	var tData *TranscodeData

	if err != nil {
		glog.Errorf("Unable to parse segData taskId=%d url=%s err=%q", notify.TaskId, notify.Url, err)
		sendTranscodeResult(context.Background(), n, orchAddr, httpc, notify, contentType, &body, tData, err)
		return
		// TODO short-circuit error handling
		// See https://github.com/livepeer/go-livepeer/issues/1518
	}
	profiles := md.Profiles
	ctx := clog.AddManifestID(context.Background(), string(md.ManifestID))
	if md.AuthToken != nil {
		ctx = clog.AddOrchSessionID(ctx, md.AuthToken.SessionId)
	}
	ctx = clog.AddSeqNo(ctx, uint64(md.Seq))
	ctx = clog.AddVal(ctx, "taskId", strconv.FormatInt(notify.TaskId, 10))
	if n.Capabilities != nil && !md.Caps.CompatibleWith(n.Capabilities.ToNetCapabilities()) {
		clog.Errorf(ctx, "Requested capabilities for segment are not compatible with this node taskId=%d url=%s err=%q", notify.TaskId, notify.Url, errCapabilities)
		sendTranscodeResult(ctx, n, orchAddr, httpc, notify, contentType, &body, tData, errCapabilities)
		return
	}
	data, err := GetSegmentData(ctx, notify.Url)
	if err != nil {
		clog.Errorf(ctx, "Transcoder cannot get segment from taskId=%d url=%s err=%q", notify.TaskId, notify.Url, err)
		sendTranscodeResult(ctx, n, orchAddr, httpc, notify, contentType, &body, tData, err)
		return
	}
	// Write it to disk
	if _, err := os.Stat(n.WorkDir); os.IsNotExist(err) {
		err = os.Mkdir(n.WorkDir, 0700)
		if err != nil {
			clog.Errorf(ctx, "Transcoder cannot create workdir err=%q", err)
			sendTranscodeResult(ctx, n, orchAddr, httpc, notify, contentType, &body, tData, err)
			return
		}
	}
	// Create input file from segment. Removed after transcoding done
	fname = path.Join(n.WorkDir, common.RandName()+".tempfile")
	if err = ioutil.WriteFile(fname, data, 0600); err != nil {
		clog.Errorf(ctx, "Transcoder cannot write file err=%q", err)
		sendTranscodeResult(ctx, n, orchAddr, httpc, notify, contentType, &body, tData, err)
		return
	}
	defer os.Remove(fname)
	md.Fname = fname
	clog.V(common.DEBUG).Infof(ctx, "Segment from taskId=%d url=%s saved to file=%s", notify.TaskId, notify.Url, fname)

	start := time.Now()
	tData, err = n.Transcoders["base"].Transcode(ctx, md)
	clog.V(common.VERBOSE).InfofErr(ctx, "Transcoding done for taskId=%d url=%s dur=%v", notify.TaskId, notify.Url, time.Since(start), err)
	if err != nil {
		if _, ok := err.(UnrecoverableError); ok {
			defer panic(err)
		}
		sendTranscodeResult(ctx, n, orchAddr, httpc, notify, contentType, &body, tData, err)
		return
	}
	if err == nil && len(tData.Segments) != len(profiles) {
		err = errors.New("segment / profile mismatch")
		sendTranscodeResult(ctx, n, orchAddr, httpc, notify, contentType, &body, tData, err)
		return
	}
	boundary := common.RandName()
	w := multipart.NewWriter(&body)
	for i, v := range tData.Segments {
		ctyp, err := common.ProfileFormatMimeType(profiles[i].Format)
		if err != nil {
			clog.Errorf(ctx, "Could not find mime type err=%q", err)
			continue
		}
		w.SetBoundary(boundary)
		hdrs := textproto.MIMEHeader{
			"Content-Type":   {ctyp},
			"Content-Length": {strconv.Itoa(len(v.Data))},
			"Pixels":         {strconv.FormatInt(v.Pixels, 10)},
		}
		fw, err := w.CreatePart(hdrs)
		if err != nil {
			clog.Errorf(ctx, "Could not create multipart part err=%q", err)
		}
		io.Copy(fw, bytes.NewBuffer(v.Data))
		// Add perceptual hash data as a part if generated
		if md.CalcPerceptualHash {
			w.SetBoundary(boundary)
			hdrs := textproto.MIMEHeader{
				"Content-Type":   {"application/octet-stream"},
				"Content-Length": {strconv.Itoa(len(v.PHash))},
			}
			fw, err := w.CreatePart(hdrs)
			if err != nil {
				clog.Errorf(ctx, "Could not create multipart part err=%q", err)
			}
			io.Copy(fw, bytes.NewBuffer(v.PHash))
		}
	}
	w.Close()
	contentType = "multipart/mixed; boundary=" + boundary
	sendTranscodeResult(ctx, n, orchAddr, httpc, notify, contentType, &body, tData, err)
}

func sendTranscodeResult(ctx context.Context, n *LivepeerNode, orchAddr string, httpc *http.Client, notify *net.NotifySegment,
	contentType string, body *bytes.Buffer, tData *TranscodeData, err error,
) {
	if err != nil {
		clog.Errorf(ctx, "Unable to transcode err=%q", err)
		body.Write([]byte(err.Error()))
		contentType = transcodingErrorMimeType
	}
	req, err := http.NewRequest("POST", "https://"+orchAddr+"/transcodeResults", body)
	if err != nil {
		clog.Errorf(ctx, "Error posting results to orch=%s staskId=%d url=%s err=%q", orchAddr,
			notify.TaskId, notify.Url, err)
		return
	}
	req.Header.Set("Authorization", protoVerLPT)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("TaskId", strconv.FormatInt(notify.TaskId, 10))

	pixels := int64(0)
	// add detections
	if tData != nil {
		if len(tData.Detections) > 0 {
			detectData, err := json.Marshal(tData.Detections)
			if err != nil {
				clog.Errorf(ctx, "Error posting results, couldn't serialize detection data orch=%s staskId=%d url=%s err=%q", orchAddr,
					notify.TaskId, notify.Url, err)
				return
			}
			req.Header.Set("Detections", string(detectData))
		}
		pixels = tData.Pixels
	}
	req.Header.Set("Pixels", strconv.FormatInt(pixels, 10))
	uploadStart := time.Now()
	resp, err := httpc.Do(req)
	if err != nil {
		clog.Errorf(ctx, "Error submitting results err=%q", err)
	} else {
		rbody, rerr := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			if rerr != nil {
				clog.Errorf(ctx, "Orchestrator returned HTTP statusCode=%v with unreadable body err=%q", resp.StatusCode, rerr)
			} else {
				clog.Errorf(ctx, "Orchestrator returned HTTP statusCode=%v err=%q", resp.StatusCode, string(rbody))
			}
		}
	}
	uploadDur := time.Since(uploadStart)
	clog.V(common.VERBOSE).InfofErr(ctx, "Transcoding done results sent for taskId=%d url=%s uploadDur=%v", notify.TaskId, notify.Url, uploadDur, err)

	if monitor.Enabled {
		monitor.SegmentUploaded(ctx, 0, uint64(notify.TaskId), uploadDur, "")
	}
}

func coreSegMetadata(segData *net.SegData) (*SegTranscodingMetadata, error) {
	if segData == nil {
		glog.Error("Empty seg data")
		return nil, errors.New("empty seg data")
	}
	var err error
	profiles := []ffmpeg.VideoProfile{}
	if len(segData.FullProfiles3) > 0 {
		profiles, err = makeFfmpegVideoProfiles(segData.FullProfiles3)
	} else if len(segData.FullProfiles2) > 0 {
		profiles, err = makeFfmpegVideoProfiles(segData.FullProfiles2)
	} else if len(segData.FullProfiles) > 0 {
		profiles, err = makeFfmpegVideoProfiles(segData.FullProfiles)
	} else if len(segData.Profiles) > 0 {
		profiles, err = common.BytesToVideoProfile(segData.Profiles)
	}
	if err != nil {
		glog.Error("Unable to deserialize profiles ", err)
		return nil, err
	}

	var os *net.OSInfo
	if len(segData.Storage) > 0 {
		os = segData.Storage[0]
	}

	dur := time.Duration(segData.Duration) * time.Millisecond
	if dur < 0 || dur > common.MaxDuration {
		glog.Error("Invalid duration")
		return nil, errDuration
	}
	if dur == 0 {
		dur = 2 * time.Second // assume 2sec default duration
	}

	caps := CapabilitiesFromNetCapabilities(segData.Capabilities)
	if caps == nil {
		// For older broadcasters. Note if there are any orchestrator
		// mandatory capabilities, seg creds verification will fail.
		caps = NewCapabilities(nil, nil)
	}

	detectorProfs := []ffmpeg.DetectorProfile{}
	for _, detector := range segData.DetectorProfiles {
		var detectorProfile ffmpeg.DetectorProfile
		// Refer to the following for type magic:
		// https://developers.google.com/protocol-buffers/docs/reference/go-generated#oneof
		switch x := detector.Value.(type) {
		case *net.DetectorProfile_SceneClassification:
			profile := x.SceneClassification
			classes := []ffmpeg.DetectorClass{}
			for _, class := range profile.Classes {
				classes = append(classes, ffmpeg.DetectorClass{
					ID:   int(class.ClassId),
					Name: class.ClassName,
				})
			}
			detectorProfile = &ffmpeg.SceneClassificationProfile{
				SampleRate: uint(profile.SampleRate),
				Classes:    classes,
			}
		}
		detectorProfs = append(detectorProfs, detectorProfile)
	}
	var segPar *SegmentParameters
	if segData.SegmentParameters != nil {
		segPar = &SegmentParameters{
			From: time.Duration(segData.SegmentParameters.From) * time.Millisecond,
			To:   time.Duration(segData.SegmentParameters.To) * time.Millisecond,
		}
	}

	return &SegTranscodingMetadata{
		ManifestID:         ManifestID(segData.ManifestId),
		Seq:                segData.Seq,
		Hash:               ethcommon.BytesToHash(segData.Hash),
		Profiles:           profiles,
		OS:                 os,
		Duration:           dur,
		Caps:               caps,
		AuthToken:          segData.AuthToken,
		DetectorEnabled:    segData.DetectorEnabled,
		DetectorProfiles:   detectorProfs,
		CalcPerceptualHash: segData.CalcPerceptualHash,
		SegmentParameters:  segPar,
	}, nil
}

func makeFfmpegVideoProfiles(protoProfiles []*net.VideoProfile) ([]ffmpeg.VideoProfile, error) {
	profiles := make([]ffmpeg.VideoProfile, 0, len(protoProfiles))
	for _, profile := range protoProfiles {
		name := profile.Name
		if name == "" {
			name = "net_" + ffmpeg.DefaultProfileName(int(profile.Width), int(profile.Height), int(profile.Bitrate))
		}
		format := ffmpeg.FormatMPEGTS
		switch profile.Format {
		case net.VideoProfile_MPEGTS:
		case net.VideoProfile_MP4:
			format = ffmpeg.FormatMP4
		default:
			return nil, errFormat
		}
		encoderProf := ffmpeg.ProfileNone
		switch profile.Profile {
		case net.VideoProfile_ENCODER_DEFAULT:
		case net.VideoProfile_H264_BASELINE:
			encoderProf = ffmpeg.ProfileH264Baseline
		case net.VideoProfile_H264_MAIN:
			encoderProf = ffmpeg.ProfileH264Main
		case net.VideoProfile_H264_HIGH:
			encoderProf = ffmpeg.ProfileH264High
		case net.VideoProfile_H264_CONSTRAINED_HIGH:
			encoderProf = ffmpeg.ProfileH264ConstrainedHigh
		default:
			return nil, errProfile
		}
		encoder := ffmpeg.H264
		switch profile.Encoder {
		case net.VideoProfile_H264:
			encoder = ffmpeg.H264
		case net.VideoProfile_H265:
			encoder = ffmpeg.H265
		case net.VideoProfile_VP8:
			encoder = ffmpeg.VP8
		case net.VideoProfile_VP9:
			encoder = ffmpeg.VP9
		default:
			return nil, errEncoder
		}
		var gop time.Duration
		if profile.Gop < 0 {
			gop = time.Duration(profile.Gop)
		} else {
			gop = time.Duration(profile.Gop) * time.Millisecond
		}
		prof := ffmpeg.VideoProfile{
			Name:         name,
			Bitrate:      fmt.Sprint(profile.Bitrate),
			Framerate:    uint(profile.Fps),
			FramerateDen: uint(profile.FpsDen),
			Resolution:   fmt.Sprintf("%dx%d", profile.Width, profile.Height),
			Format:       format,
			Profile:      encoderProf,
			GOP:          gop,
			Encoder:      encoder,
		}
		profiles = append(profiles, prof)
	}
	return profiles, nil
}
