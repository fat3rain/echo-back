package ws

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

type IncomingTrack struct {
	Key      string
	TrackID  string
	SourceID string
	Codec    webrtc.RTPCodecCapability
}

type OutgoingTrack struct {
	Track  *webrtc.TrackLocalStaticRTP
	Sender *webrtc.RTPSender
}

type User struct {
	ID           string
	DisplayName  string
	Conn         *websocket.Conn        // WebSocket соединение с клиентом; используется для обмена сигнальными сообщениями
	PC           *webrtc.PeerConnection // PeerConnection этого пользователя; через него проходит весь RTP-трафик (аудио) и происходит SDP-переговоры
	room         *Room
	writeMtx     sync.Mutex
	stateMtx     sync.RWMutex
	VideoEnabled bool

	// outgoing хранит локальные TrackLocalStaticRTP для каждого источника
	// у одного источника - несколько треков, в которые он отправяет пакеты
	// ключ srcID - (id отправителя/источника)
	// значение - локальный трек получателя, в который приходит звук от отправителя (через сервер)
	outgoing map[string]*OutgoingTrack
	outMtx   sync.RWMutex

	incoming map[string]*IncomingTrack
	inMtx    sync.RWMutex

	// защищает SDP-переговоры от race condition
	negotiationMtx   sync.Mutex
	needsNegotiation bool

	// закрытие выполняется только один раз
	closeOnce sync.Once
}

// NewUser создаёт объект User с временным UUID
// на этом этапе пользователь ещё не аутентифицирован
// после join handler перезаписывает u.ID значением из токена
func NewUser(conn *websocket.Conn, room *Room) *User {
	u := &User{
		ID:       uuid.New().String(),
		Conn:     conn,
		outgoing: make(map[string]*OutgoingTrack),
		incoming: make(map[string]*IncomingTrack),
	}
	return u
}

func (u *User) SendJSON(v any) error {
	u.writeMtx.Lock()
	defer u.writeMtx.Unlock()

	if u.Conn == nil {
		return nil
	}
	return u.Conn.WriteJSON(v)
}

func (u *User) SetVideoEnabled(enabled bool) {
	u.stateMtx.Lock()
	u.VideoEnabled = enabled
	u.stateMtx.Unlock()
}

func (u *User) IsVideoEnabled() bool {
	u.stateMtx.RLock()
	defer u.stateMtx.RUnlock()
	return u.VideoEnabled
}

func trackKey(sourceID string, kind webrtc.RTPCodecType, trackID string) string {
	return sourceID + "|" + kind.String() + "|" + trackID
}

func drainRTCP(sender *webrtc.RTPSender) {
	if sender == nil {
		return
	}

	go func() {
		rtcpBuf := make([]byte, 1500)
		for {
			if _, _, err := sender.Read(rtcpBuf); err != nil {
				return
			}
		}
	}()
}

func (u *User) ensurePeerConnection() error {
	if u.PC != nil {
		return nil
	}

	cfg := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	}

	pc, err := webrtc.NewPeerConnection(cfg)
	if err != nil {
		return err
	}
	u.PC = pc

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		cj := c.ToJSON()
		log.Printf("server ICE candidate: %+v\n", cj)

		m := SignalMessage{Type: "candidateFromServer"}
		raw, _ := json.Marshal(cj)
		m.Candidate = raw

		if err := u.SendJSON(m); err != nil {
			log.Println("send candidate:", err)
		}
	})

	pc.OnTrack(func(remoteTrack *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		_ = receiver

		sourceID := u.ID
		room := u.room
		trackID := remoteTrack.ID()
		if trackID == "" {
			trackID = remoteTrack.Kind().String()
		}
		key := trackKey(sourceID, remoteTrack.Kind(), trackID)

		incoming := &IncomingTrack{
			Key:      key,
			TrackID:  trackID,
			SourceID: sourceID,
			Codec:    remoteTrack.Codec().RTPCodecCapability,
		}

		u.inMtx.Lock()
		u.incoming[key] = incoming
		u.inMtx.Unlock()

		log.Printf(
			"OnTrack: got %s track from %s codec=%s\n",
			remoteTrack.Kind().String(),
			sourceID,
			remoteTrack.Codec().MimeType,
		)

		if room != nil {
			room.IterateUsers(func(other *User) {
				if other.ID == sourceID {
					return
				}
				if other.PC == nil {
					log.Printf("skip adding track for user %s: PC not ready\n", other.ID)
					return
				}

				added, err := other.addOutgoingTrack(incoming)
				if err != nil {
					log.Println("other.addOutgoingTrack error:", err)
					return
				}
				if added {
					go other.Negotiate()
				}
			})
		}

		defer func() {
			u.inMtx.Lock()
			delete(u.incoming, key)
			u.inMtx.Unlock()

			if room == nil {
				return
			}

			room.IterateUsers(func(other *User) {
				if other.ID == sourceID {
					return
				}
				if other.removeOutgoingTrack(key) {
					go other.Negotiate()
				}
			})
		}()

		for {
			pkt, _, err := remoteTrack.ReadRTP()
			if err != nil {
				log.Println("remoteTrack.ReadRTP:", err)
				return
			}

			if room == nil {
				continue
			}

			room.IterateUsers(func(dest *User) {
				if dest.ID == sourceID {
					return
				}

				dest.outMtx.RLock()
				outgoing := dest.outgoing[key]
				dest.outMtx.RUnlock()

				if outgoing != nil {
					if writeErr := outgoing.Track.WriteRTP(pkt); writeErr != nil {
						log.Println("WriteRTP error:", writeErr)
					}
				}
			})
		}
	})

	return nil
}

func (u *User) addOutgoingTrack(track *IncomingTrack) (bool, error) {
	if u.PC == nil {
		return false, nil
	}

	u.outMtx.Lock()
	defer u.outMtx.Unlock()

	if _, exists := u.outgoing[track.Key]; exists {
		return false, nil
	}

	localTrack, err := webrtc.NewTrackLocalStaticRTP(track.Codec, track.TrackID, track.SourceID)
	if err != nil {
		return false, err
	}

	sender, err := u.PC.AddTrack(localTrack)
	if err != nil {
		return false, err
	}

	u.outgoing[track.Key] = &OutgoingTrack{
		Track:  localTrack,
		Sender: sender,
	}
	drainRTCP(sender)

	return true, nil
}

func (u *User) removeOutgoingTrack(key string) bool {
	u.outMtx.Lock()
	outgoing, exists := u.outgoing[key]
	if exists {
		delete(u.outgoing, key)
	}
	u.outMtx.Unlock()

	if !exists || u.PC == nil {
		return false
	}

	if err := u.PC.RemoveTrack(outgoing.Sender); err != nil {
		log.Println("RemoveTrack error:", err)
		return false
	}

	return true
}

func (u *User) syncExistingTracksFromRoom() {
	if u.room == nil || u.PC == nil {
		return
	}

	u.room.IterateUsers(func(other *User) {
		if other.ID == u.ID {
			return
		}

		other.inMtx.RLock()
		tracks := make([]*IncomingTrack, 0, len(other.incoming))
		for _, track := range other.incoming {
			tracks = append(tracks, track)
		}
		other.inMtx.RUnlock()

		for _, track := range tracks {
			if _, err := u.addOutgoingTrack(track); err != nil {
				log.Println("syncExistingTracksFromRoom:", err)
			}
		}
	})
}

// ReadPump слушает сообщения по WebSocket и обрабатывает сигнальные команды:
// - join (offer) — клиент отправил offer при первом join
// - candidate — ICE кандидат от клиента
// - answer — ответ клиента на offer сервера
// - leave — закрыть соединение
func (u *User) ReadPump() {
	defer u.Close()

	for {
		// чтение сообщения WebSocket
		_, raw, err := u.Conn.ReadMessage()
		if err != nil {
			log.Println("ws read:", err)
			return
		}
		var msg SignalMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Println("invalid signal json:", err)
			continue
		}
		switch msg.Type {
		case "join":
			// объединяем join + offer, потому что при первом подключении клиент сразу присылает offer
			// и сервер должен ответить answer. Если SDP есть и это offer, обрабатываем его.
			if msg.SDP != "" && msg.SDPType == "offer" {
				if err := u.ReceiveOfferAndAnswerBack(msg.SDP); err != nil {
					log.Println("error answering join offer:", err)
					return
				}
			}
		case "offer":
			if msg.SDP != "" && msg.SDPType == "offer" {
				if err := u.ReceiveOfferAndAnswerBack(msg.SDP); err != nil {
					log.Println("error answering offer:", err)
					return
				}
			}
		case "candidate":
			// ICE кандидаты от клиента приходят отдельными сообщениями
			var cand webrtc.ICECandidateInit
			if len(msg.Candidate) > 0 {
				if err := json.Unmarshal(msg.Candidate, &cand); err == nil {
					// проверяем, что PeerConnection уже создан
					if u.PC != nil {
						// добавляем кандидата в PeerConnection
						// после добавления ICE-агент будет пробовать установить соединение с этим кандидатом
						if err := u.PC.AddICECandidate(cand); err != nil {
							log.Println("AddICECandidate error:", err)
						}
					}
				}
			}
		case "answer":
			if msg.SDP != "" && msg.SDPType == "answer" {
				// если PeerConnection ещё не создан — ничего не делаем, логируем
				if u.PC == nil {
					log.Println("received answer but PC is nil")
					continue
				}

				// создаём объект SessionDescription с типом Answer
				// это SDP, которое клиент сформировал в ответ на наш offer
				sdp := webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer,
					SDP:  msg.SDP,
				}

				// устанавливаем это описание как remote description в PeerConnection
				// после этого WebRTC знает, какие кодеки, форматы, ICE кандидаты использует клиент
				// теперь наш PeerConnection может начать отправлять и получать RTP/RTCP потоки
				if err := u.PC.SetRemoteDescription(sdp); err != nil {
					log.Println("SetRemoteDescription answer:", err)
				} else {
					u.negotiationMtx.Lock()
					needsNegotiation := u.needsNegotiation
					u.negotiationMtx.Unlock()
					if needsNegotiation {
						go u.Negotiate()
					}
				}
			}
		case "mediaState":
			if msg.VideoEnabled != nil {
				u.SetVideoEnabled(*msg.VideoEnabled)
				if u.room != nil {
					u.room.BroadcastParticipants()
				}
			}
		case "leave":
			return
		default:
			log.Println("unknown msg type:", msg.Type)
		}
	}
}

// ReceiveOfferAndAnswerBack создаёт PeerConnection, привязывает обработчики
// ICE кандидатов и OnTrack. Затем устанавливает remote offer, создаёт answer
// и отсылает его клиенту. Также OnTrack реплицирует потоки другим участникам.
func (u *User) ReceiveOfferAndAnswerBack(offerSDP string) error {
	if err := u.ensurePeerConnection(); err != nil {
		return err
	}

	u.negotiationMtx.Lock()
	defer u.negotiationMtx.Unlock()

	// преобразуем offer клиента в SessionDescription и ставим как remote description
	offer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP, // SDP клиента с его кодеками, треками и ICE

	}

	// устанавливаем remote description на серверной PeerConnection чтобы, PC знал треки, кодеки и ICE-кандидаты
	if err := u.PC.SetRemoteDescription(offer); err != nil {
		return err
	}

	// перед созданием answer добавляем уже активные треки остальных участников комнаты.
	// так поздно вошедший клиент сразу получает существующие аудио/видео потоки без повторного входа.
	u.syncExistingTracksFromRoom()

	// создаём ответ сервера (answer) и ставим как локальное описание
	// теперь сервер знает, какие треки/кодеки/ICE он предлагает клиенту
	answer, err := u.PC.CreateAnswer(nil)
	if err != nil {
		return err
	}

	// устанавливаем локальное описание на сервере — answer
	// теперь сервер знает, какие треки/кодеки/ICE он предлагает клиенту
	if err := u.PC.SetLocalDescription(answer); err != nil {
		return err
	}

	// ждём, пока ICE-агент соберёт все локальные кандидаты для PeerConnection
	gatherComplete := webrtc.GatheringCompletePromise(u.PC)
	<-gatherComplete

	/// берем локальное описание (answer + локальные ICE кандидаты) для отправки клиенту через WebSocket
	local := u.PC.LocalDescription()
	resp := SignalMessage{
		Type:    "answer",
		SDP:     local.SDP,
		SDPType: local.Type.String(),
	}
	// отправляем клиенту answer через WebSocket
	// после этого клиент сможет установить remote description и начать передачу аудио
	if err := u.SendJSON(resp); err != nil {
		return err
	}
	return nil
}

// Negotiate запускает SDP-переговоры с клиентом.
// вызывается, когда на серверной PeerConnection меняется набор треков (добавили, удалили итд)
func (u *User) Negotiate() {
	if u.PC == nil {
		return
	}
	u.negotiationMtx.Lock()
	defer u.negotiationMtx.Unlock()
	u.needsNegotiation = true

	if u.PC == nil || u.PC.SignalingState() != webrtc.SignalingStateStable {
		return
	}
	u.needsNegotiation = false

	// создаём SDP offer — описание текущего состояния PeerConnection:
	// какие треки, кодеки и направления передачи сервер предлагает клиенту
	offer, err := u.PC.CreateOffer(nil)
	if err != nil {
		log.Println("CreateOffer:", err)
		return
	}

	// устанавливаем offer как LocalDescription.
	// этим мы фиксируем состояние PeerConnection и запускаем ICE-gathering
	if err := u.PC.SetLocalDescription(offer); err != nil {
		log.Println("SetLocalDescription:", err)
		return
	}

	// ожидаем завершения ICE gathering,
	// чтобы LocalDescription содержал собранные ICE-кандидаты
	gatherComplete := webrtc.GatheringCompletePromise(u.PC)
	<-gatherComplete
	local := u.PC.LocalDescription()

	// отправляем offer клиенту через signaling (WebSocket)
	msg := SignalMessage{
		Type:    "offer",
		SDP:     local.SDP,
		SDPType: local.Type.String(),
	}
	if err := u.SendJSON(msg); err != nil {
		log.Println("send offer:", err)
	}
}

// Close аккуратно закрывает ресурсы: удаляет пользователя из комнаты,
// закрывает PeerConnection и WebSocket. Выполняется один раз (closeOnce).
func (u *User) Close() {
	u.closeOnce.Do(func() {
		log.Println("closing user", u.ID)
		if u.room != nil {
			u.room.RemoveUser(u)
		}
		if u.PC != nil {
			_ = u.PC.Close()
		}
		if u.Conn != nil {
			_ = u.Conn.Close()
		}
	})
}
