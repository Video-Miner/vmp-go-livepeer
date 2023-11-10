package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	gonet "net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/livepeer/lpms/ffmpeg"

	"github.com/golang/glog"
	"google.golang.org/grpc/peer"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/go-livepeer/core"
	"github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/go-livepeer/net"
)

const protoVerLPT = "videominer-1.2-525e816"
const depProtoVerLPT = "videominer-1.2-31a7016"
const transcodingErrorMimeType = "livepeer/transcoding-error"

var errSecret = errors.New("invalid secret")
var errZeroCapacity = errors.New("zero capacity")
var errInterrupted = errors.New("execution interrupted")
var errCapabilities = errors.New("incompatible segment capabilities")
var errNoEthAddress = errors.New("no ethereum address")
var errPoolVersion = errors.New("invalid pool version")
var errNoAuth = errors.New("not authenticated")
var errNoOrchestrator = errors.New("no Orchestrator near your area")
var errLatencyTestFailed = errors.New("latency test failed")
var errAlreadyConnected = errors.New("already has an active connection to this Orchestrator")

// Standalone Transcoder

func findFastestOrchestrator(discoveryUri string) string {
	best := ""
	// Retrieve Orch Uri's
	resp, err := http.Get(discoveryUri)
	if err != nil {
		glog.Error(err)
		return best
	}
	if resp.StatusCode != 200 {
		glog.Infof("Unable to retrieve local Orchestrators: pool management system returned status code `%v`", resp.StatusCode)
		return best
	}
	var target []string
	err = json.NewDecoder(resp.Body).Decode(&target)
	if err != nil {
		panic(err)
	}

	orchUrls := []string{}
	for _, thisUri := range target {
		url, err := url.Parse(thisUri)
		if err != nil {
			glog.Warning("Unable to parse uri '%v': %v", thisUri, err)
			continue
		}
		glog.Infof("Discovered Orchestrator at `%v`", url.Host)
		orchUrls = append(orchUrls, url.Host)
	}

	port := "8443"
	// Minimum threshold in ms (keep in mind grpc connection is more than just a ping)
	latencyThreshold := 500.0

	for _, thisUrl := range orchUrls {
		ips, _ := gonet.LookupIP(thisUrl)
		for _, ip := range ips {
			if ipv4 := ip.To4(); ipv4 != nil {
				glog.Infof("DNS %v -> %v ", thisUrl, "https://"+ipv4.String()+":"+port)
				thisUrlObj, err := url.ParseRequestURI("https://" + ipv4.String() + ":" + port)
				if err != nil {
					glog.Error("Skipping due to a failed GRPC connection test: ", err)
				} else {
					start := time.Now()
					thisUrlPinged := testOrchestrator(thisUrlObj)
					latency := float64(time.Since(start).Milliseconds())
					if thisUrlPinged == "" {
						glog.Warning("Skipping due to a failed GRPC connection test to %v", thisUrlPinged)
					} else {
						glog.Infof("GRPC test to `%v` took %f ms", thisUrlPinged, latency)
						if latency < latencyThreshold {
							latencyThreshold = latency
							best = ipv4.String() + ":" + port
						}
					}
				}
			}
		}
	}
	return best
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

// Orchestrator gRPC

func (h *lphttp) RegisterTranscoder(req *net.RegisterRequest, stream net.Transcoder_RegisterTranscoderServer) error {
	p, _ := peer.FromContext(stream.Context())
	h.orchestrator.ServiceURI()
	ethAddress := ethcommon.BytesToAddress(req.EthereumAddress)
	host := p.Addr.String()

	if protoVerLPT != req.PoolVersion && depProtoVerLPT != req.PoolVersion {
		glog.Infof("Received Register request with an invalid software version %s from Transcoder %s at %s", req.PoolVersion, ethAddress.String(), host)
		return errPoolVersion
	} else if depProtoVerLPT == req.PoolVersion {
		glog.Warningf("Authenticating transcoder=%s address=%v poolVersion=%s (DEPRECATED)", host, ethAddress.String(), req.PoolVersion)
	} else if req.LatencyTest > h.node.LatencyThreshold {
		glog.Infof("Latency test failed for transcoder=%s address=%v poolVersion=%s", host, ethAddress.String(), req.PoolVersion)
		return errLatencyTestFailed
	} else {
		glog.Infof("Authenticating transcoder=%s address=%v poolVersion=%s", host, ethAddress.String(), req.PoolVersion)
	}

	type Data struct {
		EthAddr ethcommon.Address `json:"ethAddr"`
		IpAddrT string            `json:"ipAddrT"`
	}

	data := Data{
		EthAddr: ethAddress,
		IpAddrT: host,
	}

	// Check for duplicate connections
	for _, tConn := range h.node.TranscoderManager.RegisteredTranscodersInfo() {
		if tConn.Address == data.IpAddrT {
			glog.Errorf("Rejecting Transcoder, as it is already connected at %s with address %s", tConn.Address, tConn.EthereumAddress.String())
			return errAlreadyConnected
		}
	}

	from := common.GetConnectionAddr(stream.Context())
	glog.Infof("Successfully authenticated transcoder=%s totalCapacity=%d remainingCapacity=%d", from, req.Capacity, req.RemainingCap)

	// ***Commented out Livepeer's secret check in favor of the pool's secret check above
	// if req.Secret != h.orchestrator.TranscoderSecret() {
	// 	glog.Errorf("err=%q", errSecret.Error())
	// 	return errSecret
	// }
	if req.Capacity <= 0 {
		glog.Errorf("err=%q", errZeroCapacity.Error())
		return errZeroCapacity
	}
	// handle case of legacy Transcoder which do not advertise capabilities
	if req.Capabilities == nil {
		req.Capabilities = core.NewCapabilities(core.DefaultCapabilities(), nil).ToNetCapabilities()
	}

	if req.EthereumAddress == nil {
		glog.Info(errNoEthAddress.Error())
		return errNoEthAddress
	}

	// blocks until stream is finished
	h.orchestrator.ServeTranscoder(stream, int(req.Capacity), int(req.RemainingCap), req.Capabilities, ethcommon.BytesToAddress(req.EthereumAddress), string(req.TranscoderID), req.PoolVersion, int64(req.LatencyTest))
	return nil
}

// Orchestrator HTTP

func (h *lphttp) TranscodeResults(w http.ResponseWriter, r *http.Request) {
	orch := h.orchestrator

	authType := r.Header.Get("Authorization")
	if protoVerLPT != authType && depProtoVerLPT != authType {
		glog.Error("Invalid auth type ", authType)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		glog.Error("Error getting mime type ", err)
		http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
		return
	}

	tid, err := strconv.ParseInt(r.Header.Get("TaskId"), 10, 64)
	if err != nil {
		glog.Error("Could not parse task ID ", err)
		http.Error(w, "Invalid Task ID", http.StatusBadRequest)
		return
	}

	// read detection data - only scene classification is supported
	var detections []ffmpeg.DetectData
	var sceneDetections []ffmpeg.SceneClassificationData
	var detectionsHeader = r.Header.Get("Detections")
	if len(detectionsHeader) > 0 {
		err = json.Unmarshal([]byte(detectionsHeader), &sceneDetections)
		if err != nil {
			glog.Error("Could not parse detection data ", err)
			http.Error(w, "Invalid detection data", http.StatusBadRequest)
			return
		}
		for _, sd := range sceneDetections {
			detections = append(detections, sd)
		}
	}

	var res core.RemoteTranscoderResult
	if transcodingErrorMimeType == mediaType {
		w.Write([]byte("OK"))
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			glog.Errorf("Unable to read transcoding error body taskId=%v err=%q", tid, err)
			res.Err = err
		} else {
			res.Err = fmt.Errorf(string(body))
		}
		glog.Errorf("Transcoding error for taskId=%v err=%q", tid, res.Err)
		orch.TranscoderResults(tid, &res)
		return
	}

	decodedPixels, err := strconv.ParseInt(r.Header.Get("Pixels"), 10, 64)
	if err != nil {
		glog.Error("Could not parse decoded pixels", err)
		http.Error(w, "Invalid Pixels", http.StatusBadRequest)
		return
	}

	var segments []*core.TranscodedSegmentData
	if mediaType == "multipart/mixed" {
		start := time.Now()
		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				glog.Error("Could not process multipart part ", err)
				res.Err = err
				break
			}
			body, err := common.ReadAtMost(p, common.MaxSegSize)
			if err != nil {
				glog.Error("Error reading body ", err)
				res.Err = err
				break
			}

			if len(p.Header.Values("Pixels")) > 0 {
				encodedPixels, err := strconv.ParseInt(p.Header.Get("Pixels"), 10, 64)
				if err != nil {
					glog.Error("Error getting pixels in header:", err)
					res.Err = err
					break
				}
				segments = append(segments, &core.TranscodedSegmentData{Data: body, Pixels: encodedPixels})
			} else if p.Header.Get("Content-Type") == "application/octet-stream" {
				// Perceptual hash data for last segment
				if len(segments) > 0 {
					segments[len(segments)-1].PHash = body
				} else {
					err := errors.New("Unknown perceptual hash")
					glog.Error("No previous segment present to attach perceptual hash data to: ", err)
					res.Err = err
					break
				}
			}
		}
		res.TranscodeData = &core.TranscodeData{
			Segments:   segments,
			Pixels:     decodedPixels,
			Detections: detections,
		}
		dlDur := time.Since(start)
		glog.V(common.VERBOSE).Infof("Downloaded results from remote transcoder=%s taskId=%d dur=%s", r.RemoteAddr, tid, dlDur)

		if monitor.Enabled {
			monitor.SegmentDownloaded(r.Context(), 0, uint64(tid), dlDur)
		}

		orch.TranscoderResults(tid, &res)
	}
	if res.Err != nil {
		http.Error(w, res.Err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte("OK"))
}
