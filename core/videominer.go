package core

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/common"
	lpmon "github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/lpms/ffmpeg"
	"gonum.org/v1/gonum/stat"
)

var errPixelMismatch = errors.New("pixel mismatch")

// / Contains stored statistics per transcoder Eth address
// All pixel counts are per millisecond
type RemoteTranscoderStats struct {
	owner string
	// Recording stats into the stats object
	recordMutex      sync.Mutex
	pixelsEncodedObs []float64
	pixelsDecodedObs []float64
	realTimeRatio    []float64
	sessionAccxTime  map[string]int64
	fresh            bool
	// Parsing stats in the stats object
	processMutex      sync.Mutex
	pixelsEncodedAvg  float64
	pixelsEncodedVar  float64
	pixelsEncodedLast float64
	pixelsDecodedAvg  float64
	pixelsDecodedVar  float64
	pixelsDecodedLast float64
	realTimeAverage   float64
	realTimeVariance  float64
	realTimeLastValue float64
	jailedUntil       int64
	strikes           int
	lastStrikeGiven   int64
	lastStrikeReduced int64
	// Reading final stats during selection
	readMutex         sync.Mutex
	jailed            bool
	encodedThreshold  float64
	decodedThreshold  float64
	realtimeThreshold float64
	encodedValue      float64
	decodedValue      float64
	realtimeValue     float64
}

func newStats(owner string) *RemoteTranscoderStats {
	return &RemoteTranscoderStats{
		pixelsEncodedAvg:  0,
		pixelsEncodedVar:  0,
		pixelsEncodedLast: 0,
		pixelsDecodedAvg:  0,
		pixelsDecodedVar:  0,
		pixelsDecodedLast: 0,
		realTimeAverage:   0,
		realTimeVariance:  0,
		realTimeLastValue: 0,
		recordMutex:       sync.Mutex{},
		processMutex:      sync.Mutex{},
		owner:             owner,
		jailedUntil:       0,
		strikes:           0,
		lastStrikeGiven:   0,
		lastStrikeReduced: 0,
		fresh:             true,
		readMutex:         sync.Mutex{},
		jailed:            false,
		encodedThreshold:  0.0,
		decodedThreshold:  0.0,
		realtimeThreshold: 0.0,
		encodedValue:      0.0,
		decodedValue:      0.0,
		realtimeValue:     0.0,
	}
}

type BabyStats struct {
	owner             string
	encodedThreshold  float64
	decodedThreshold  float64
	realtimeThreshold float64
}

type ActiveManifest struct {
	ManifestID ManifestID
	LastSeen   time.Time
}

func (n *LivepeerNode) ActiveManifestAddUpdate(manifestId ManifestID) {
	//ActiveManifests thread safety
	n.amMu.Lock()

	for _, am := range n.ActiveManifests {
		if am.ManifestID == manifestId {
			am.LastSeen = time.Now()
			n.amMu.Unlock()
			return
		}
	}
	am := ActiveManifest{ManifestID: manifestId, LastSeen: time.Now()}
	n.ActiveManifests = append(n.ActiveManifests, &am)
	glog.Infof(`Active manifest added: %s`, manifestId)
	n.amMu.Unlock()
	n.MqttBroker.PublishTranscoderLoadCapacity("videominer.go:116")

}

// called by transcoder node, not the orchestrator node. caller is not required to hold mutex as it is held by this function.
func (n *LivepeerNode) GetTranscoderLoadAndCapacity() (int, int) {
	n.amMu.RLock()
	defer n.amMu.RUnlock()
	load := len(n.ActiveManifests)
	remainingCap := MaxSessions - load
	return load, remainingCap
}

func (n *LivepeerNode) ActiveManifestCleanup() {
	modified := false
	//ActiveManifests thread safety
	n.amMu.Lock()

	//temp := n.ActiveManifests[:0]

	var temp []*ActiveManifest
	for _, am := range n.ActiveManifests {
		// assuming if a manifest has not been seen in over 10 seconds the stream is over
		// may need to be more thoughtful about this in the future (e.g., 2 x segment duration)
		if time.Since(am.LastSeen) < (10 * time.Second) {
			glog.Infof(`Active manifest kept: %s; Last seen: %v`, am.ManifestID, time.Since(am.LastSeen))
			temp = append(temp, am)
		} else {
			glog.Infof(`Active manifest removed: %s; Last seen: %v`, am.ManifestID, time.Since(am.LastSeen))
			modified = true
		}
	}

	n.ActiveManifests = temp
	n.amMu.Unlock()

	if modified {
		n.MqttBroker.PublishTranscoderLoadCapacity("videominer.go:153")
	}
}

func simpleCopyStats(owner string, encodedThreshold float64, decodedThreshold float64, realtimeThreshold float64) *BabyStats {
	return &BabyStats{
		owner:             owner,
		encodedThreshold:  encodedThreshold,
		decodedThreshold:  decodedThreshold,
		realtimeThreshold: realtimeThreshold,
	}
}

type PublicTranscoderPool struct {
	transcoderStats     []*RemoteTranscoderStats
	quit                chan struct{}
	rankingMutex        sync.Mutex
	encodingLeaderboard []*BabyStats
	decodingLeaderboard []*BabyStats
	realtimeLeaderboard []*BabyStats
}

func NewPublicTranscoderPool() *PublicTranscoderPool {
	return &PublicTranscoderPool{
		transcoderStats:     []*RemoteTranscoderStats{},
		quit:                make(chan struct{}),
		rankingMutex:        sync.Mutex{},
		encodingLeaderboard: []*BabyStats{},
		decodingLeaderboard: []*BabyStats{},
		realtimeLeaderboard: []*BabyStats{},
	}
}

// Reinits stats after a reconnect if they have been set to 0, but we have enough observations to make this T trusted
func (rt *RemoteTranscoder) InitPromStats() {
	if rt.stats == nil {
		glog.Errorf("Cannot init prometheus stats for Transcoder '%s', due to rt.stats == nil", rt.EthereumAddr)
		return
	}
	// Lock in case the T is connected from multiple separate T processes
	rt.stats.recordMutex.Lock()
	defer rt.stats.recordMutex.Unlock()
	rt.stats.processMutex.Lock()
	defer rt.stats.processMutex.Unlock()
	// Immediately mark as trusted if we already have enough
	if len(rt.stats.pixelsEncodedObs) > 10 && len(rt.stats.pixelsDecodedObs) > 10 && len(rt.stats.realTimeRatio) > 10 {
		if lpmon.Enabled {
			lpmon.SetTranscoderStats(rt.stats.owner, rt.stats.pixelsEncodedAvg, rt.stats.pixelsEncodedVar,
				rt.stats.pixelsDecodedAvg, rt.stats.pixelsDecodedVar,
				rt.stats.realTimeAverage, rt.stats.realTimeVariance)
			lpmon.SetTranscoderLastVals(rt.stats.owner, rt.stats.pixelsEncodedLast, rt.stats.pixelsDecodedLast, rt.stats.realTimeLastValue)
		}
	} else if lpmon.Enabled {
		lpmon.SetTranscoderStats(rt.stats.owner, 0, 0, 0, 0, 0, 0)
		lpmon.SetTranscoderLastVals(rt.stats.owner, rt.stats.pixelsEncodedLast, rt.stats.pixelsDecodedLast, rt.stats.realTimeLastValue)
	}
}

// Makes sure that the Transcoder rank is up-to-date
func (pool *PublicTranscoderPool) StatsMonitorLoop() {
	// Re-rank and calculate averages and STDEV every 10 seconds
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-pool.quit:
			return
		case <-ticker.C:
			pool.StatsMonitor()
		}
	}
}

func (pool *PublicTranscoderPool) StopStatsMonitorLoop() {
	close(pool.quit)
}

func (pool *PublicTranscoderPool) StatsMonitor() {
	glog.Infof("Ranking: checking for updated Stats objects...")
	requiresReranking := false
	statsCopy := make([]*BabyStats, 0)
	jailedTs := 0 //< count amount of transcoders jailed
	freeTs := 0   //< count amount of transcoders not jailed
	// Check each stats object if they've got updates stats
	for i := 0; i < len(pool.transcoderStats); i++ {
		pool.transcoderStats[i].recordMutex.Lock()
		pool.transcoderStats[i].processMutex.Lock()
		if pool.transcoderStats[i].fresh == false {
			glog.Infof("Ranking: Stats object for Transcoder %s requires updating", pool.transcoderStats[i].owner)
			CalcKPIs(pool.transcoderStats[i])
			requiresReranking = true
			pool.transcoderStats[i].fresh = true
		}
		statsCopy = append(statsCopy, simpleCopyStats(pool.transcoderStats[i].owner,
			pool.transcoderStats[i].encodedThreshold, pool.transcoderStats[i].decodedThreshold,
			pool.transcoderStats[i].realtimeThreshold))
		jailed := CheckJail(pool.transcoderStats[i])
		if jailed != pool.transcoderStats[i].jailed {
			glog.Infof("Ranking: Updating Jailed status for Transcoder '%s'...", pool.transcoderStats[i].owner)
			pool.transcoderStats[i].readMutex.Lock()
			pool.transcoderStats[i].jailed = jailed
			pool.transcoderStats[i].readMutex.Unlock()
		}
		if pool.transcoderStats[i].jailed {
			jailedTs++
		} else {
			freeTs++
		}
		pool.transcoderStats[i].RemoveExpiredSessionIds()
		pool.transcoderStats[i].recordMutex.Unlock()
		pool.transcoderStats[i].processMutex.Unlock()
	}
	// If all T's are jailed, release half of them
	if freeTs == 0 && jailedTs > 0 {
		glog.Info("Ranking: Releasing all transcoders from jail")
		pool.ReleaseJail()
	}
	if requiresReranking == true {
		pool.RankTranscoders(statsCopy)
	}
	statsCopy = nil
}

// Resets jail status for all T's
func (pool *PublicTranscoderPool) ReleaseJail() {
	for i := 0; i < len(pool.transcoderStats); i++ {
		pool.transcoderStats[i].recordMutex.Lock()
		pool.transcoderStats[i].processMutex.Lock()

		ResetStats(pool.transcoderStats[i])

		pool.transcoderStats[i].recordMutex.Unlock()
		pool.transcoderStats[i].processMutex.Unlock()
	}
}

type byEncodingThreshold []*BabyStats

func (r byEncodingThreshold) Len() int      { return len(r) }
func (r byEncodingThreshold) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r byEncodingThreshold) Less(i, j int) bool {
	return r[i].encodedThreshold > r[j].encodedThreshold // sort descending
}

type byDecodingThreshold []*BabyStats

func (r byDecodingThreshold) Len() int      { return len(r) }
func (r byDecodingThreshold) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r byDecodingThreshold) Less(i, j int) bool {
	return r[i].decodedThreshold > r[j].decodedThreshold // sort descending
}

type byRealtimeThreshold []*BabyStats

func (r byRealtimeThreshold) Len() int      { return len(r) }
func (r byRealtimeThreshold) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r byRealtimeThreshold) Less(i, j int) bool {
	return r[i].realtimeThreshold > r[j].realtimeThreshold // sort descending
}

// Orders Transcoders by KPI
func (pool *PublicTranscoderPool) RankTranscoders(stats []*BabyStats) {
	glog.Infof("Ranking: recalculating leaderboards...")
	newEncoding := make([]*BabyStats, 0)
	newDecoding := make([]*BabyStats, 0)
	newRealtime := make([]*BabyStats, 0)
	// For each KPI, sort and store as new leaderboard
	newEncoding = append(newEncoding, stats...)
	sort.Sort(byEncodingThreshold(newEncoding))
	newDecoding = append(newDecoding, stats...)
	sort.Sort(byDecodingThreshold(newDecoding))
	newRealtime = append(newRealtime, stats...)
	sort.Sort(byRealtimeThreshold(newRealtime))
	glog.Infof("Ranking:## POS #      ENC     #      DEC     #      RTR     ##")
	for i := 0; i < len(newEncoding); i++ {
		encName := newEncoding[i].owner[:6] + ".." + newEncoding[i].owner[len(newEncoding[i].owner)-4:]
		decName := newDecoding[i].owner[:6] + ".." + newDecoding[i].owner[len(newDecoding[i].owner)-4:]
		rtrName := newRealtime[i].owner[:6] + ".." + newRealtime[i].owner[len(newRealtime[i].owner)-4:]
		glog.Infof("Ranking:## %03d # %s # %s # %s ##", i+1, encName, decName, rtrName)
	}
	glog.Infof("Ranking:## POS #      ENC     #      DEC     #      RTR     ##")
	// Lock to overwrite ranking list
	glog.Infof("Ranking: Updating leaderboards...")
	pool.rankingMutex.Lock()
	defer pool.rankingMutex.Unlock()
	pool.encodingLeaderboard = newEncoding
	pool.decodingLeaderboard = newDecoding
	pool.realtimeLeaderboard = newRealtime
}

// Quietly resets a Transcoders stats. Requires recordMutex and processMutex to be locked
func ResetStats(stats *RemoteTranscoderStats) {
	stats.readMutex.Lock()
	defer stats.readMutex.Unlock()
	stats.pixelsEncodedObs = nil
	stats.pixelsDecodedObs = nil
	stats.realTimeRatio = nil
	stats.pixelsEncodedAvg = 0
	stats.pixelsEncodedVar = 0
	stats.pixelsEncodedLast = 0
	stats.pixelsDecodedAvg = 0
	stats.pixelsDecodedVar = 0
	stats.pixelsDecodedLast = 0
	stats.realTimeAverage = 0
	stats.realTimeVariance = 0
	stats.realTimeLastValue = 0
	stats.jailedUntil = 0
	stats.jailed = false
	stats.fresh = false
	stats.encodedThreshold = 0.0
	stats.decodedThreshold = 0.0
	stats.realtimeThreshold = 0.0
	stats.encodedValue = 0.0
	stats.decodedValue = 0.0
	stats.realtimeValue = 0.0
	stats.lastStrikeReduced = time.Now().Unix()
}

func (stats *RemoteTranscoderStats) RemoveExpiredSessionIds() {
	// 10 second timeout
	expiryTime := time.Now().Unix() - 10
	for key, val := range stats.sessionAccxTime {
		if val <= expiryTime {
			glog.Infof("Ranking: Session '%s' for transcoder `%s` is now inactive...", key, stats.owner)
			delete(stats.sessionAccxTime, key)
		}
	}
}

// Assumes stats mutexes are already locked
func (stats *RemoteTranscoderStats) JailTranscoder() {
	stats.strikes++
	stats.lastStrikeGiven = time.Now().Unix()
	// Start with a 10 minute timeout
	timeout := 600
	// For each strike, double it until it exceeds 1 day (86400 seconds)
	for i := 0; i < stats.strikes; i++ {
		timeout *= 2
		if timeout > 86400 {
			break
		}
	}
	if timeout > 86400 {
		timeout = 86400
	}
	stats.jailedUntil = time.Now().Add(time.Second * time.Duration(timeout)).Unix()
}

// Jails Transcoders who pee in the pool. Unjails them once the timeout has expired
func CheckJail(stats *RemoteTranscoderStats) bool {
	if stats.encodedValue == 0.0 && stats.decodedValue == 0.0 && stats.realtimeValue == 0.0 {
		return false
	}

	// Reset stats if their jail has expired
	if stats.jailedUntil > 0 && stats.jailedUntil < time.Now().Unix() {
		glog.Infof("Ranking: Resetting Transcoder '%s' since they are out of jail", stats.owner)
		ResetStats(stats)
		return false
	}
	// Skip remote Transcoders who are shadowbanned
	if stats.jailedUntil > 0 {
		glog.Infof("Ranking: Transcoder '%s' remains jailed ...", stats.owner)
		return true
	}

	// At this point the T is known to be not in jail - maybe they should be?

	// If they've been good for 24 hours without any second chances, remove a strike
	if stats.strikes > 0 && stats.lastStrikeGiven+86400 < time.Now().Unix() && stats.lastStrikeReduced+86400 < time.Now().Unix() {
		glog.Infof("Ranking: Removing a strike from Transcoder '%s'...", stats.owner)
		stats.lastStrikeReduced = time.Now().Unix()
		stats.strikes--
	}

	// Jail for any segment transcoded below realtime
	for i := 0; i < len(stats.realTimeRatio); i++ {
		if stats.realTimeRatio[i] <= 1.0 && stats.jailedUntil == 0 {
			glog.Infof("Ranking: Transcoder '%s' jailed for a realtime ratio of `%.1f` ...", stats.owner, stats.realTimeRatio[i])
			// Remove the offending segment so that it does not re-trigger the next iteration
			stats.realTimeRatio = append(stats.realTimeRatio[:i], stats.realTimeRatio[i+1:]...)
			// Allow one strike every 24h
			if stats.lastStrikeGiven+86400 < time.Now().Unix() {
				glog.Infof("Ranking: Giving a second chance to Transcoder '%s' ...", stats.owner)
				stats.lastStrikeGiven = time.Now().Unix()
				return false
			}
			stats.JailTranscoder()
			return true
		}
	}

	return false
}

// Calculates statistics for tracked KPI's
func CalcKPIs(stats *RemoteTranscoderStats) {
	// Wait a bit before exporting stats, as low observation count produces unreliable results
	if len(stats.pixelsEncodedObs) > 10 && len(stats.pixelsDecodedObs) > 10 && len(stats.realTimeRatio) > 10 {
		// Only use the last 100 observations for stats - drop older entries
		// Handle encoded stats
		if len(stats.pixelsEncodedObs) > 100 {
			stats.pixelsEncodedObs = stats.pixelsEncodedObs[1:]
		}
		mean, std := stat.MeanStdDev(stats.pixelsEncodedObs, nil)
		stats.pixelsEncodedAvg = mean
		stats.pixelsEncodedVar = std
		encodedThreshold := mean - std
		// Handle decoded stats
		if len(stats.pixelsDecodedObs) > 100 {
			stats.pixelsDecodedObs = stats.pixelsDecodedObs[1:]
		}
		mean, std = stat.MeanStdDev(stats.pixelsDecodedObs, nil)
		stats.pixelsDecodedAvg = mean
		stats.pixelsDecodedVar = std
		decodedThreshold := mean - std
		// Handle RTR stats
		if len(stats.realTimeRatio) > 100 {
			stats.realTimeRatio = stats.realTimeRatio[1:]
		}
		mean, std = stat.MeanStdDev(stats.realTimeRatio, nil)
		stats.realTimeAverage = mean
		stats.realTimeVariance = std
		realtimeThreshold := mean - std
		if lpmon.Enabled {
			lpmon.SetTranscoderStats(stats.owner, stats.pixelsEncodedAvg, stats.pixelsEncodedVar,
				stats.pixelsDecodedAvg, stats.pixelsDecodedVar,
				stats.realTimeAverage, stats.realTimeVariance)
		}
		if encodedThreshold != stats.encodedThreshold ||
			decodedThreshold != stats.decodedThreshold ||
			realtimeThreshold != stats.realtimeThreshold {
			stats.readMutex.Lock()
			defer stats.readMutex.Unlock()
			glog.Infof("Ranking: Updating thresholds for Transcoder '%s'...", stats.owner)
			stats.encodedThreshold = encodedThreshold
			stats.decodedThreshold = decodedThreshold
			stats.realtimeThreshold = realtimeThreshold
			stats.encodedValue = stats.pixelsEncodedAvg
			stats.decodedValue = stats.pixelsDecodedAvg
			stats.realtimeValue = stats.realTimeAverage
		}
	}
}

// Updates the T's stored statistics
func (rt *RemoteTranscoder) RecordStats(responseTime float64, duration float64, pixelsEncoded float64, pixelsDecoded float64, sessionId string) {
	if pixelsEncoded == 0 || pixelsDecoded == 0 || duration == 0 || responseTime == 0 || rt.stats == nil {
		glog.Errorf("Cannot update stats for Transcoder '%s', due to malformed data pixelsEncoded=%v pixelsDecoded=%v duration=%v responseTime=%v (rt.stats == nil)=%t",
			rt.EthereumAddr, pixelsEncoded, pixelsDecoded, duration, responseTime, rt.stats == nil)
		return
	}
	// Lock against data races between writing, calculating stats and reading
	rt.stats.recordMutex.Lock()
	defer rt.stats.recordMutex.Unlock()
	// Update sessionId
	if rt.stats.sessionAccxTime[sessionId] == 0 {
		glog.Infof("Ignoring first segment for session '%s' for transcoder `%s`...", sessionId, rt.EthereumAddr.String())
		rt.stats.sessionAccxTime[sessionId] = time.Now().Unix()
		return
	}
	rt.stats.sessionAccxTime[sessionId] = time.Now().Unix()
	rt.stats.fresh = false
	// Store new observation
	rt.stats.pixelsEncodedLast = math.Floor(pixelsEncoded / responseTime)
	rt.stats.pixelsEncodedObs = append(rt.stats.pixelsEncodedObs, rt.stats.pixelsEncodedLast)
	rt.stats.pixelsDecodedLast = math.Floor(pixelsDecoded / responseTime)
	rt.stats.pixelsDecodedObs = append(rt.stats.pixelsDecodedObs, rt.stats.pixelsDecodedLast)
	rt.stats.realTimeLastValue = duration / responseTime
	rt.stats.realTimeRatio = append(rt.stats.realTimeRatio, rt.stats.realTimeLastValue)
	// Clip to latest 100 observations
	if len(rt.stats.pixelsEncodedObs) > 100 {
		rt.stats.pixelsEncodedObs = rt.stats.pixelsEncodedObs[1:]
	}
	if len(rt.stats.pixelsDecodedObs) > 100 {
		rt.stats.pixelsDecodedObs = rt.stats.pixelsDecodedObs[1:]
	}
	if len(rt.stats.realTimeRatio) > 100 {
		rt.stats.realTimeRatio = rt.stats.realTimeRatio[1:]
	}
	// Prometheus export
	if lpmon.Enabled {
		lpmon.SetTranscoderLastVals(rt.EthereumAddr.String(), rt.stats.pixelsEncodedLast, rt.stats.pixelsDecodedLast, rt.stats.realTimeLastValue)
	}
}

func verifyPixels(td *TranscodeData) error {
	count := int64(0)
	for i := 0; i < len(td.Segments); i++ {
		pxls, err := countPixels(td.Segments[i].Data)
		if err != nil {
			return err
		}
		if pxls != td.Segments[i].Pixels {
			glog.Errorf("Pixel mismatch count=%v actual=%v", count, td.Pixels)
			return errors.New("pixel mismatch")
		}
	}

	return nil
}

func countPixels(data []byte) (int64, error) {
	tempfile, err := ioutil.TempFile("", common.RandName())
	if err != nil {
		return 0, fmt.Errorf("error creating temp file for pixels verification: %w", err)
	}
	defer os.Remove(tempfile.Name())

	if _, err := tempfile.Write(data); err != nil {
		tempfile.Close()
		return 0, fmt.Errorf("error writing temp file for pixels verification: %w", err)
	}

	if err = tempfile.Close(); err != nil {
		return 0, fmt.Errorf("error closing temp file for pixels verification: %w", err)
	}

	fname := tempfile.Name()
	p, err := pixels(fname)
	if err != nil {
		return 0, err
	}

	return p, nil
}

func pixels(fname string) (int64, error) {
	in := &ffmpeg.TranscodeOptionsIn{Fname: fname}
	res, err := ffmpeg.Transcode3(in, nil)
	if err != nil {
		return 0, err
	}

	return res.Decoded.Pixels, nil
}
