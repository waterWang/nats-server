// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats-server/v2/server/archive"
)

type SnapshotConsumerState struct {
	*ConsumerConfig `json:"config"`
	*ConsumerState  `json:"state"`
}

// Create a snapshot of this stream and its consumer's state along with messages.
// sa is passed in when the stream is clustered, so we can find child consumer assignments.
func (js *jetStream) CreateStreamSnapshotV2(store StreamStore, deadline time.Duration, includeConsumers bool, sa *streamAssignment) (*SnapshotResult, error) {
	pr, pw := net.Pipe()

	// Set a write deadline here to protect ourselves.
	if deadline > 0 {
		pw.SetWriteDeadline(time.Now().Add(deadline))
	}

	// We can add to our stream while snapshotting but not "user" delete anything.
	var state StreamState
	store.FastState(&state)

	// Stream in separate Go routine.
	errCh := make(chan error, 1)
	go js.streamSnapshotV2(store, &state, pw, includeConsumers, sa, errCh)

	return &SnapshotResult{pr, state, errCh}, nil
}

// Stream our snapshot through S2 compression and the custom archive format.
func (js *jetStream) streamSnapshotV2(store StreamStore, state *StreamState, w io.WriteCloser, includeConsumers bool, sa *streamAssignment, errCh chan error) {
	defer close(errCh)
	defer w.Close()

	enc := s2.NewWriter(w)
	defer enc.Close()

	tw := archive.NewWriter(enc)
	defer tw.Close()

	now := time.Now()
	clustered := js.isClustered()

	writeGeneric := func(name string, mod int64, seq uint64, headerSize, payloadSize int64, buf []byte) error {
		hdr := &archive.Header{
			Name:        name,
			Timestamp:   mod,
			Sequence:    seq,
			HeaderSize:  headerSize,
			PayloadSize: payloadSize,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := tw.Write(buf); err != nil {
			return err
		}
		// Need to wait for flush here as the tar/s2 writer is handing off to a
		// flow-controlled publisher, it's important that we handle backpressure.
		return tw.Flush()
	}

	writeStoreMsg := func(msg *StoreMsg) error {
		return writeGeneric(
			msg.subj,
			msg.ts,
			msg.seq,
			int64(len(msg.hdr)),
			int64(len(msg.msg)),
			msg.buf,
		)
	}

	writeConsumerMsg := func(scs SnapshotConsumerState) error {
		ssj, err := json.Marshal(scs)
		if err != nil {
			return err
		}
		return writeGeneric(
			path.Join("consumers", scs.Name),
			now.UnixNano(),
			0,
			0,
			int64(len(ssj)),
			ssj,
		)
	}

	// If we aren't including consumers here then make sure the consumer count
	// is set accordingly, this helps on the restore path.
	var consumerAssignments map[string]*consumerAssignment
	var consumerStores []ConsumerStore
	var streamState = *state
	if !includeConsumers {
		streamState.Consumers = 0
	} else if clustered {
		if sa == nil {
			errCh <- errors.New("stream assignment not present in clustered mode")
			return
		}
		js.mu.RLock()
		consumerAssignments = make(map[string]*consumerAssignment, len(sa.consumers))
		for name, ca := range sa.consumers {
			consumerAssignments[name] = ca.copyGroup()
		}
		streamState.Consumers = len(consumerAssignments)
		js.mu.RUnlock()
	} else {
		consumerStores = slices.Collect(store.Consumers())
		streamState.Consumers = len(consumerStores)
	}

	ssj, err := json.Marshal(streamState)
	if err != nil {
		errCh <- err
		return
	}
	if err := writeGeneric("state.json", now.UnixNano(), 0, 0, int64(len(ssj)), ssj); err != nil {
		errCh <- err
		return
	}

	// Do consumers first, if the stream is interest/WQ then this may be
	// important for message retention.
	if includeConsumers {
		consumerStateFromInfo := func(ci *ConsumerInfo) *ConsumerState {
			state := &ConsumerState{
				Delivered: SequencePair{
					Consumer: ci.Delivered.Consumer,
					Stream:   ci.Delivered.Stream,
				},
				AckFloor: SequencePair{
					Consumer: ci.AckFloor.Consumer,
					Stream:   ci.AckFloor.Stream,
				},
			}
			if ci.NumAckPending <= 0 || ci.Delivered.Stream <= ci.AckFloor.Stream || ci.Delivered.Consumer <= ci.AckFloor.Consumer {
				return state
			}

			pending := uint64(ci.NumAckPending)
			if maxPending := ci.Delivered.Stream - ci.AckFloor.Stream; pending > maxPending {
				pending = maxPending
			}
			if maxPending := ci.Delivered.Consumer - ci.AckFloor.Consumer; pending > maxPending {
				pending = maxPending
			}
			if pending == 0 {
				return state
			}

			// Cluster consumer info does not include sparse pending details.
			// Approximate pending with a contiguous range above the ack floor.
			state.Pending = make(map[uint64]*Pending, int(pending))
			ts := now.UnixNano()
			for i := uint64(0); i < pending; i++ {
				state.Pending[ci.AckFloor.Stream+1+i] = &Pending{
					Sequence:  ci.AckFloor.Consumer + 1 + i,
					Timestamp: ts,
				}
			}
			return state
		}

		if clustered {
			for _, ca := range consumerAssignments {
				ci, err := sysRequest[ConsumerInfo](js.srv, clusterConsumerInfoT, sa.Client.serviceAccount(), sa.Config.Name, ca.Name)
				if err != nil || ci == nil {
					errCh <- fmt.Errorf("failed to get consumer state for '%s > %s'", sa.Config.Name, ca.Name)
					return
				}
				if err := writeConsumerMsg(SnapshotConsumerState{
					ConsumerConfig: ca.Config,
					ConsumerState:  consumerStateFromInfo(ci),
				}); err != nil {
					errCh <- err
					return
				}
			}
		} else {
			for _, o := range consumerStores {
				config := o.GetConfig()
				state, err := o.State()
				if err != nil {
					errCh <- fmt.Errorf("couldn't load consumer '%s' state: %s", config.Name, err)
					return
				}
				if err := writeConsumerMsg(SnapshotConsumerState{
					ConsumerConfig: config,
					ConsumerState:  state,
				}); err != nil {
					errCh <- err
					return
				}
			}
		}
	}

	var sm StoreMsg
	for seq := state.FirstSeq - 1; seq < state.LastSeq; {
		if _, seq, err = store.LoadNextMsg(fwcs, true, seq+1, &sm); err != nil {
			if err == ErrStoreEOF {
				break
			}
			errCh <- fmt.Errorf("couldn't load next message after seq %d: %s", seq+1, err)
			return
		}
		if err = writeStoreMsg(&sm); err != nil {
			errCh <- err
			return
		}
	}

	// End of backup sentinel. A clear marker makes it obvious when
	// a backup has been truncated or not without having to count
	// messages or from first/last sequence, which may not be possible
	// during the rewrite of a large stream backup.
	if err = writeGeneric(_EMPTY_, 0, 0, 0, 0, nil); err != nil {
		errCh <- err
	}
}

// RestoreStreamSnapshotV2 will restore a stream from a snapshot.
func (a *Account) RestoreStreamV2(ncfg *StreamConfig, r io.Reader) (*stream, error) {
	dec := s2.NewReader(r)
	tr := archive.NewReader(dec)

	var nstate StreamState

	// Load the stream state.
	hdr, err := tr.Next()
	if err != nil {
		return nil, err
	}
	if hdr.Name != "state.json" {
		return nil, fmt.Errorf("expected state.json first")
	}
	state, err := io.ReadAll(tr)
	if err != nil {
		return nil, fmt.Errorf("expected state.json contents")
	}
	if err := json.Unmarshal(state, &nstate); err != nil {
		return nil, fmt.Errorf("error in state.json: %w", err)
	}

	s, jsa, err := a.checkForJetStream()
	if err != nil {
		return nil, err
	}
	js := jsa.js
	if js == nil {
		return nil, NewJSNotEnabledForAccountError()
	}
	if _, err := a.lookupStream(ncfg.Name); err == nil {
		return nil, NewJSStreamNameExistRestoreFailedError()
	}

	cfg, apiErr := s.checkStreamCfg(ncfg, a, false)
	if apiErr != nil {
		return nil, apiErr
	}

	_, isClustered := jsa.jetStreamAndClustered()
	jsa.usageMu.RLock()
	selected, tier, hasTier := jsa.selectLimits(cfg.Replicas)
	jsa.usageMu.RUnlock()
	reserved := int64(0)
	if hasTier {
		if isClustered {
			js.mu.RLock()
			_, reserved = js.tieredStreamAndReservationCount(a.Name, tier, &cfg)
			js.mu.RUnlock()
		} else {
			reserved = jsa.tieredReservation(tier, &cfg)
		}
	}
	var bc int64
	js.mu.RLock()
	err = js.checkAllLimits(&selected, tier, &cfg, reserved, bc)
	js.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	mset, err := a.addStream(&cfg)
	if err != nil {
		return nil, fmt.Errorf("error adding stream: %w", err)
	}

	// Start off at the right sequence number. This is important in particular
	// when the backup contains no messages or would restore to no interest.
	if _, err = mset.store.Compact(nstate.FirstSeq); err != nil {
		return nil, fmt.Errorf("error purging stream: %w", err)
	}

	for range nstate.Consumers {
		hdr, err := tr.Next()
		if err != nil {
			return nil, err
		}
		bc += hdr.HeaderSize + hdr.PayloadSize
		js.mu.RLock()
		err = js.checkAllLimits(&selected, tier, &cfg, reserved, bc)
		js.mu.RUnlock()
		if err != nil {
			return nil, err
		}
		name, found := strings.CutPrefix(hdr.Name, "consumers/")
		if !found {
			return nil, fmt.Errorf("expected consumer, found %q", hdr.Name)
		}
		buf, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("failed to read consumer %q state: %w", name, err)
		}
		var consumer SnapshotConsumerState
		if err := json.Unmarshal(buf, &consumer); err != nil {
			return nil, fmt.Errorf("failed to decode consumer %q state: %w", name, err)
		}
		o, err := mset.addConsumer(consumer.ConsumerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to add consumer %q: %w", name, err)
		}
		o.mu.Lock()
		err = o.setStoreState(consumer.ConsumerState)
		o.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("failed to set consumer %q state: %w", name, err)
		}
	}

	store := mset.store
	lseq := nstate.FirstSeq - 1
	eob := false
	for {
		hdr, err := tr.Next()
		if err != nil {
			return nil, err
		}
		seq := hdr.Sequence
		if seq == 0 {
			// Sentinel "end of backup" if all fields are zero.
			if hdr.Timestamp == 0 && hdr.HeaderSize == 0 && hdr.PayloadSize == 0 {
				eob = true
				break
			}
			return nil, fmt.Errorf("expected message sequence")
		}
		if hdr.HeaderSize < 0 || hdr.PayloadSize < 0 {
			return nil, fmt.Errorf("invalid message lengths for sequence %d", seq)
		}
		buf, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("failed to read message sequence %d: %w", seq, err)
		}
		if hdr.HeaderSize > int64(len(buf)) {
			return nil, fmt.Errorf("failed to parse message sequence %d: invalid header length", seq)
		}
		if int64(len(buf)) != hdr.HeaderSize+hdr.PayloadSize {
			return nil, fmt.Errorf("failed to read message sequence %d: unexpected payload size", seq)
		}
		subj := hdr.Name
		mhdr := buf[:hdr.HeaderSize]
		msg := buf[hdr.HeaderSize : hdr.HeaderSize+hdr.PayloadSize]
		if seq <= lseq {
			return nil, fmt.Errorf("message sequence %d out of order", seq)
		}
		// We could have deleted messages since the last message we stored, if so
		// we should work out what the gap is and skip those sequences.
		if gap := seq - lseq - 1; gap > 0 {
			if err := store.SkipMsgs(lseq+1, gap); err != nil {
				return nil, fmt.Errorf("failed to process gap: %w", err)
			}
		}
		lseq = seq
		switch cfg.Storage {
		case MemoryStorage:
			bc += int64(memStoreMsgSize(subj, mhdr, msg))
		default:
			bc += int64(fileStoreMsgSize(subj, mhdr, msg))
		}
		js.mu.RLock()
		err = js.checkAllLimits(&selected, tier, &cfg, reserved, bc)
		js.mu.RUnlock()
		if err != nil {
			return nil, err
		}
		ttl, err := getMessageTTL(mhdr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse message TTL: %w", err)
		}
		hdrTime := time.Unix(0, hdr.Timestamp)
		if ttl > 0 && time.Now().After(hdrTime.Add(time.Duration(ttl)*time.Second)) {
			// If the TTL has exceeded then there isn't much point in storing the message,
			// but we still need to preserve the sequence.
			if err := store.SkipMsgs(seq, 1); err != nil {
				return nil, fmt.Errorf("failed to process expired message sequence %d: %w", seq, err)
			}
			continue
		}
		if err = store.StoreRawMsg(subj, mhdr, msg, seq, hdr.Timestamp, ttl, false); err != nil {
			return nil, fmt.Errorf("failed to store message sequence %d: %w", seq, err)
		}
	}

	if !eob {
		return mset, fmt.Errorf("backup was truncated")
	}

	// Need to make sure that we pad out with skip msgs to preserve the last
	// sequence, otherwise trailing deleted messages could reuse sequence numbers.
	if lseq < nstate.LastSeq {
		gap := nstate.LastSeq - lseq
		if err := store.SkipMsgs(lseq+1, gap); err != nil {
			return nil, fmt.Errorf("failed to process trailing gap: %w", err)
		}
	}
	return mset, nil
}
