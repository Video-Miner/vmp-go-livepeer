package core

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/common"
	"github.com/livepeer/go-livepeer/monitor"
	"github.com/livepeer/go-livepeer/pm"
)

type PlutusMQ struct {
	Client   mqtt.Client
	ClientID string

	TopicStore []string
	TsMutex    sync.RWMutex

	Node *LivepeerNode
}

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	glog.V(common.VERBOSE).Infof("Received Message from topic: %s. Payload: %s", msg.Topic(), msg.Payload())
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	glog.Infof("connected to broker at %v on port %v", *LPNode.LivepeerConfig.MqttBrokerHost, *LPNode.LivepeerConfig.MqttBrokerPort)

	// * This was causing the client not to subscribe
	// if !LPNode.NodeInitialized {
	// 	glog.Error("Node not initialized yet, do not publish online message. NOTE: This error is expected on startup")
	// 	return
	// }

	if len(LPNode.MqttBroker.TopicStore) > 0 {
		for _, topic := range LPNode.MqttBroker.TopicStore {
			glog.V(common.VERBOSE).Infof("Resubscribing to topic: %s", topic)
			LPNode.MqttBroker.Subscribe(topic, 1, messagePubHandler)
		}
	}

	if LPNode.NodeType == OrchestratorNode {
		LPNode.MqttBroker.PublishOrchestratorOnline()
		LPNode.MqttBroker.SubOrchestrator()
	}
	if LPNode.NodeType == TranscoderNode {
		LPNode.MqttBroker.PublishTranscoderOnline()
		LPNode.MqttBroker.SubTranscoder()
	}
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	glog.Errorf("Lost connection to MQTT broker: %v", err)
}

func SetOrchestratorLastWill(opts *mqtt.ClientOptions, id string, serviceURI string) {
	topic := fmt.Sprintf("plutus/orchestrator/telemetry/%s", id)
	telem := map[string]interface{}{
		"Online":     false,
		"ServiceURI": serviceURI,
		"Load":       0,
	}
	payload, _ := json.Marshal(telem)
	opts.SetWill(topic, string(payload), 2, true)
	glog.V(common.VERBOSE).Infof("Set orchestrator last will payload: %s", string(payload))
}

func SetTranscoderLastWill(opts *mqtt.ClientOptions, uid string) {
	topic := fmt.Sprintf("plutus/transcoder/telemetry/%s", uid)
	telem := map[string]interface{}{
		"Online":        false,
		"Load":          0,
		"RemainingCap":  0,
		"TotalCapacity": 0,
	}
	payload, _ := json.Marshal(telem)
	opts.SetWill(topic, string(payload), 2, true)
	glog.V(common.VERBOSE).Infof("Set transcoder last will payload: %s", string(payload))
}

func NewMQTTClient(node *LivepeerNode, broker string, port int, username string, password string, clientID string) *PlutusMQ {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID(clientID)
	opts.SetCleanSession(true)
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.SetAutoReconnect(true)
	if *node.LivepeerConfig.Orchestrator {
		SetOrchestratorLastWill(opts, *node.LivepeerConfig.UUID, *node.LivepeerConfig.ServiceAddr)
	} else {
		SetTranscoderLastWill(opts, node.UID)
	}

	opts.SetKeepAlive(30 * time.Second)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	var pmq PlutusMQ
	glog.V(common.VERBOSE).Infof("MQTT client ID: %s", clientID)
	pmq.Client = mqtt.NewClient(opts)
	pmq.ClientID = clientID
	pmq.Node = node

	return &pmq
}

func (pmq *PlutusMQ) AddTopicToStore(topic string) {
	pmq.TsMutex.Lock()
	defer pmq.TsMutex.Unlock()

	// Check if the topic is already in the TopicStore
	for _, t := range pmq.TopicStore {
		if t == topic {
			return // Topic already exists, so return without adding it again
		}
	}

	pmq.TopicStore = append(pmq.TopicStore, topic)
}

func (pmq *PlutusMQ) RemoveTopicFromStore(topic string) {
	pmq.TsMutex.Lock()
	defer pmq.TsMutex.Unlock()
	for i, t := range pmq.TopicStore {
		if t == topic {
			pmq.TopicStore = append(pmq.TopicStore[:i], pmq.TopicStore[i+1:]...)
			break
		}
	}
}

func (pmq *PlutusMQ) ConnectToBoker() {
	if token := pmq.Client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
}

func (pmq *PlutusMQ) SubTranscoder() {
	pmq.SubOrchestratorDiscovery()

	//pmq.PLSubscribe("plutus/mothership/discovery/orchestrator/#", 1, pmq.cb_orchestrator_discovery)
	pmq.SubDebug()
}

func (pmq *PlutusMQ) SubOrchestrator() {
	pmq.SubDebug()
}

func (pmq *PlutusMQ) Subscribe(topic string, qos byte, callback mqtt.MessageHandler) {
	token := pmq.Client.Subscribe(topic, qos, callback)
	glog.V(common.VERBOSE).Infof("Subscribing to topic: %s", topic)
	pmq.AddTopicToStore(topic)
	go func() {
		_ = token.Wait() // Can also use '<-t.Done()' in releases > 1.2.0
		if token.Error() != nil {
			glog.Error(token.Error()) // Use your preferred logging technique (or just fmt.Printf)
		}
	}()
}

func (pmq *PlutusMQ) Unsubscribe(topic string) {
	token := pmq.Client.Unsubscribe(topic)
	glog.V(common.VERBOSE).Infof("Unsubscribing from topic: %s", topic)
	pmq.RemoveTopicFromStore(topic)
	go func() {
		_ = token.Wait() // Can also use '<-t.Done()' in releases > 1.2.0
		if token.Error() != nil {
			glog.Error(token.Error()) // Use your preferred logging technique (or just fmt.Printf)
		}
	}()
}
func (pmq *PlutusMQ) SubOrchestratorDiscovery() {
	topic := "plutus/mothership/discovery/orchestrator/#"
	pmq.Subscribe(topic, 1, pmq.cb_orchestrator_discovery)
	glog.V(common.VERBOSE).Info("Subscribed to orchestrator discovery topic")
}

func (pmq *PlutusMQ) SubOrchestratorTelemetry(id string) {
	topic := fmt.Sprintf("plutus/orchestrator/telemetry/%s", id)
	pmq.Subscribe(topic, 1, pmq.cb_orchestrator_telemetry)
	glog.V(common.VERBOSE).Infof("Subscribed to orchestrator telemetry: %s", id)
}

// Orchestrator Subscriptions/Publications
func (pmq *PlutusMQ) SubTranscoderTelemetry(uid string) {
	topic := fmt.Sprintf("plutus/transcoder/telemetry/%s", uid)
	pmq.Subscribe(topic, 1, pmq.cb_transcoder_telemetry)
	glog.V(common.VERBOSE).Infof("Subscribed to transcoder telemetry: %s", uid)
}

func (pmq *PlutusMQ) SubDebug() {
	topic := "plutus/debug/trigger"
	pmq.Subscribe(topic, 0, pmq.cb_debug)
	glog.V(common.VERBOSE).Info("Subscribed to orchestrator debug topic")
}

func (pmq *PlutusMQ) UnsubOrchestratorTelemetry(id string) {
	topic := fmt.Sprintf("plutus/orchestrator/telemetry/%s", id)
	pmq.Unsubscribe(topic)
}

func (pmq *PlutusMQ) cb_orchestrator_discovery(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	payload := msg.Payload()
	topic_segments := strings.Split(topic, "/")
	id := topic_segments[len(topic_segments)-1]

	defer ManageTranscoderInstance(pmq.Node, id)

	//PlutusOrchs thread safety
	pmq.Node.poMu.Lock()
	defer pmq.Node.poMu.Unlock()

	var orch PlutusOrchestrator

	if len(payload) > 0 {
		json.Unmarshal(payload, &orch)

		// if this is a newly discovered orchestrator, subscribe to its telemetry
		og_orch, existingOrch := pmq.Node.PlutusOrchs[id]
		if !existingOrch {
			pmq.SubOrchestratorTelemetry(id)
		} else {
			//Latency and connected flag are not sent in the discovery message so we need to update it with the existing value before we assign &orch to the map
			orch.Latency = og_orch.Latency
			orch.Connected = og_orch.Connected
		}

		pmq.Node.PlutusOrchs[id] = &orch

		glog.V(common.VERBOSE).Infof("Orchestrator discovery callback triggered: {ServiceURI: %s, Online: %t, Whitelisted: %t, InsufficientBalance: %t}", orch.ServiceURI, orch.Online, orch.Whitelisted, orch.InsufficientBalance)
	} else {
		delete(pmq.Node.PlutusOrchs, id)
		glog.V(common.VERBOSE).Info("Orchestrator discovery callback triggered: payload is empty, removing orchestrator")
	}

}

func (pmq *PlutusMQ) cb_debug(client mqtt.Client, msg mqtt.Message) {
	payload := string(msg.Payload())

	pmq.Node.poMu.RLock()
	defer pmq.Node.poMu.RUnlock()

	if payload == "all" || strings.Contains(payload, pmq.Node.UID) {
		glog.V(common.VERBOSE).Infof("Orchestrator debug triggered: %s", payload)
		if *pmq.Node.LivepeerConfig.Orchestrator {
			pmq.PublishOrchestratorDebug()
			return
		}

		if *pmq.Node.LivepeerConfig.Transcoder {
			pmq.PublishTranscoderDebug()
			return
		}
	}

	glog.V(common.VERBOSE).Infof("Orchestrator debug do nothing: %s", payload)
}

func (pmq *PlutusMQ) cb_orchestrator_telemetry(client mqtt.Client, msg mqtt.Message) {

	topic := msg.Topic()
	payload := msg.Payload()
	topic_segments := strings.Split(topic, "/")
	id := topic_segments[3]

	defer ManageTranscoderInstance(pmq.Node, id)

	var data map[string]interface{}
	json.Unmarshal(payload, &data)

	// PlutusOrchs thread safety
	pmq.Node.poMu.Lock()
	defer pmq.Node.poMu.Unlock()

	orch := pmq.Node.PlutusOrchs[id]
	for attrib, value := range data {
		switch attrib {
		case "Online":
			orch.Online = value.(bool)
		case "ServiceURI":
			orch.ServiceURI = value.(string)
		}
	}
	glog.V(common.VERBOSE).Infof("Orchestrator telemetry callback triggered: {ServiceURI: %s, Online: %t}", orch.ServiceURI, orch.Online)
}

func (pmq *PlutusMQ) cb_transcoder_telemetry(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	payload := msg.Payload()
	topic_segments := strings.Split(topic, "/")
	id := topic_segments[3]

	var data map[string]interface{}
	json.Unmarshal(payload, &data)

	// RTM thread safety
	pmq.Node.TranscoderManager.RTmutex.Lock()
	defer pmq.Node.TranscoderManager.RTmutex.Unlock()

	var remoteTranscoder *RemoteTranscoder
	for _, remoteTranscoder = range pmq.Node.TranscoderManager.remoteTranscoders {
		if remoteTranscoder.EthereumAddr.String() == id {
			break
		}
	}

	//If transcoder has already been removed by the orchestrator don't try to update it
	//unsubscribe from the transcoder
	if remoteTranscoder == nil {
		pmq.Unsubscribe(topic)
		return
	}

	for attrib, value := range data {
		switch attrib {
		case "Online":
			if !value.(bool) {
				pmq.Unsubscribe(topic)
			}
		case "Load":
			remoteTranscoder.TotalLoad = int(value.(float64))
			if monitor.Enabled {
				monitor.SetTranscoderTotalLoad(id, remoteTranscoder.TotalLoad)
			}
		case "RemainingCap":
			remoteTranscoder.RemainingCap = int(value.(float64))
			if monitor.Enabled {
				monitor.SetTranscoderRemainingCapacity(id, remoteTranscoder.RemainingCap)
			}
		}
	}
	glog.V(common.VERBOSE).Info("Transcoder telemetry callback triggered")
}

func (pmq *PlutusMQ) PublishOrchestratorOnline() {
	id := *pmq.Node.LivepeerConfig.UUID
	topic := fmt.Sprintf("plutus/orchestrator/telemetry/%s", id)
	load, _, _, _ := pmq.Node.TranscoderManager.totalLoadAndCapacity()
	telem := map[string]interface{}{
		"Online":     true,
		"ServiceURI": pmq.Node.LivepeerConfig.ServiceAddr,
		"Load":       load,
	}
	payload, _ := json.Marshal(telem)
	pmq.Client.Publish(topic, 0, true, payload)
	glog.V(common.VERBOSE).Info("Publish orchestrator online")
}

func (pmq *PlutusMQ) PublishTranscoderOnline() {
	topic := fmt.Sprintf("plutus/transcoder/telemetry/%s", pmq.Node.UID)

	ip, _ := common.GetPublicIP()

	telem := map[string]interface{}{
		"IP":            ip,
		"Online":        true,
		"Load":          0,
		"TotalCapacity": MaxSessions,
		"RemainingCap":  MaxSessions,
	}
	payload, _ := json.Marshal(telem)
	pmq.Client.Publish(topic, 0, true, payload)
	glog.V(common.VERBOSE).Info("Publish transcoder online")
}

func (pmq *PlutusMQ) PublishTranscoderConnection(rt *RemoteTranscoder, terminate bool) {
	topic := fmt.Sprintf("plutus/orchestrator/t_connect/%s", *pmq.Node.LivepeerConfig.UUID)
	telem := map[string]interface{}{
		"Transcoder":      rt.EthereumAddr.String(),
		"UID":             rt.Uid,
		"Sessions":        rt.LocalLoad,
		"Terminated":      terminate,
		"ConnEstablished": rt.ConnTimestamp,
		"Timestamp":       time.Now().Unix(),
	}
	payload, _ := json.Marshal(telem)
	pmq.Client.Publish(topic, 0, true, payload)
	glog.V(common.VERBOSE).Infof("Publish transcoder connection: {Transcoder: %s, Sessions: %d, Terminated: %t, ConnEstablished: %d}", rt.EthereumAddr.String(), rt.LocalLoad, terminate, rt.ConnTimestamp)
}

func (pmq *PlutusMQ) PublishTranscoderLoadCapacity() {
	load, remainingCap := pmq.Node.GetTranscoderLoadAndCapacity()

	telem := map[string]interface{}{
		"Online":        true,
		"Load":          load,
		"RemainingCap":  remainingCap,
		"TotalCapacity": MaxSessions,
	}
	payload, _ := json.Marshal(telem)

	topic := fmt.Sprintf("plutus/transcoder/telemetry/%s", pmq.Node.UID)
	pmq.Client.Publish(topic, 0, true, payload)
	glog.V(common.VERBOSE).Infof("Publish transcoder load and capacity: {Load: %d, RemainingCap: %d, TotalCapacity: %d}", load, remainingCap, pmq.Node.MaxSessions)
}

func (pmq *PlutusMQ) PublishSegmentTranscoded(segStats TranscodedSegmentStats) {
	payload, _ := json.Marshal(segStats)

	topic := fmt.Sprintf("plutus/orchestrator/segment_transcoded/%s", *pmq.Node.LivepeerConfig.UUID)
	pmq.Client.Publish(topic, 2, false, payload)
	glog.V(common.VERBOSE).Info("Publish segment transcoded log")
}

func (pmq *PlutusMQ) PublishWinningTicket(ticket *pm.Ticket, txHash string) {

	payload, _ := json.Marshal(ticket)
	topic := fmt.Sprintf("plutus/orchestrator/winning_ticket/%s/%s", *pmq.Node.LivepeerConfig.UUID, txHash)
	pmq.Client.Publish(topic, 2, false, payload)
	glog.V(common.VERBOSE).Info("Publish winning ticket")
}

func (pmq *PlutusMQ) PublishOrchestratorDebug() {
	id := pmq.Node.UID
	topic := fmt.Sprintf("plutus/debug/orchestrator/%s", id)

	load, capacity, remainingCapacity, numConnectedTranscoders := pmq.Node.TranscoderManager.totalLoadAndCapacity()

	connectedTranscoders := pmq.Node.TranscoderManager.remoteTranscoders
	telem := map[string]interface{}{
		"ServiceURI":              pmq.Node.LivepeerConfig.ServiceAddr,
		"Load":                    load,
		"Capacity":                capacity,
		"RemainingCapacity":       remainingCapacity,
		"NumConnectedTranscoders": numConnectedTranscoders,
		"ConnectedTranscoders":    connectedTranscoders,
	}
	payload, _ := json.Marshal(telem)
	pmq.Client.Publish(topic, 0, false, payload)
	glog.V(common.VERBOSE).Info("Publish orchestrator debug")
}

func (pmq *PlutusMQ) PublishTranscoderDebug() {

	// Replace all function code with transcoder-relevant data. This is simply copy/pasted from the orchestrator debug function
	id := pmq.Node.UID
	topic := fmt.Sprintf("plutus/debug/transcoder/%s", id)

	load, remainingCapacity := pmq.Node.GetTranscoderLoadAndCapacity()

	keys := make([]string, len(pmq.Node.TStopChans))

	// Iterate over the map and collect the keys
	i := 0
	for key := range pmq.Node.TStopChans {
		keys[i] = key
		i++
	}

	//plutusOrchs, _ := json.Marshal(pmq.Node.PlutusOrchs)
	telem := map[string]interface{}{
		"MaxSessions":         MaxSessions,
		"Load":                load,
		"RemainingCapacity":   remainingCapacity,
		"PlutusOrchestrators": pmq.Node.PlutusOrchs,
		"TStopChannels":       keys,
	}
	payload, _ := json.Marshal(telem)
	pmq.Client.Publish(topic, 0, false, payload)
	glog.V(common.VERBOSE).Info("Publish orchestrator debug")
}
