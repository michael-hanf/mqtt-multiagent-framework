package logger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// MQTTLogger writes incoming MQTT messages to weekly Markdown files (readable in Obsidian).
// path is treated as a directory; files are named MQTT-Log-YYYY-WWW.md.
type MQTTLogger struct {
	dir           string
	topics        []string
	maxPayloadLen int // 0 = unlimited
	mu            sync.Mutex
}

// New creates a new MQTTLogger. dir is the directory for weekly log files.
// topics supports MQTT wildcards (# and +). maxPayloadLen=0 means unlimited.
func New(dir string, topics []string, maxPayloadLen int) *MQTTLogger {
	return &MQTTLogger{dir: dir, topics: topics, maxPayloadLen: maxPayloadLen}
}

// weeklyPath returns the log file path for the current ISO week.
func (l *MQTTLogger) weeklyPath(now time.Time) string {
	year, week := now.ISOWeek()
	filename := fmt.Sprintf("MQTT-Log-%d-W%02d.md", year, week)
	return filepath.Join(l.dir, filename)
}

// Log writes a single MQTT message to the current weekly log file.
func (l *MQTTLogger) Log(topic string, from string, msgType string, payload []byte) {
	if !l.matches(topic) {
		return
	}

	// Fall back to payload-parsed fields when caller doesn't provide them
	if from == "" || msgType == "" {
		pFrom, pType := extractMsgMeta(payload)
		if from == "" {
			from = pFrom
		}
		if msgType == "" {
			msgType = pType
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	path := l.weeklyPath(now)

	if err := os.MkdirAll(l.dir, 0755); err != nil {
		return
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	// Write header if file is new/empty
	info, _ := f.Stat()
	if info.Size() == 0 {
		year, week := now.ISOWeek()
		header := fmt.Sprintf("# MQTT Log – %d W%02d\n\n", year, week)
		f.WriteString(header)
	}

	ts := now.Format("2006-01-02 15:04:05")
	payloadStr := compactPayload(payload, l.maxPayloadLen)

	// Collapsible callout: summary line collapsed by default, payload inside
	entry := fmt.Sprintf("> [!info]- %s · %s · `%s`\n> **From:** %s\n> ```json\n> %s\n> ```\n\n",
		ts, msgType, topic, from, payloadStr)

	f.WriteString(entry)
}

// matches checks whether a topic matches any of the configured log topics (MQTT wildcard support).
func (l *MQTTLogger) matches(topic string) bool {
	if len(l.topics) == 0 {
		return true
	}
	for _, pattern := range l.topics {
		if matchTopic(pattern, topic) {
			return true
		}
	}
	return false
}

// matchTopic implements MQTT wildcard matching (# and +).
func matchTopic(pattern, topic string) bool {
	pp := strings.Split(pattern, "/")
	tp := strings.Split(topic, "/")

	for i, p := range pp {
		if p == "#" {
			return true
		}
		if i >= len(tp) {
			return false
		}
		if p != "+" && p != tp[i] {
			return false
		}
	}
	return len(pp) == len(tp)
}

// extractMsgMeta parses from/type out of the standard Message JSON envelope.
func extractMsgMeta(payload []byte) (from, msgType string) {
	var msg struct {
		From string `json:"from"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &msg); err == nil {
		return msg.From, msg.Type
	}
	return "", ""
}

// compactPayload returns a compact JSON preview or truncated string.
func compactPayload(payload []byte, maxLen int) string {
	var s string
	if json.Valid(payload) {
		var buf bytes.Buffer
		if err := json.Compact(&buf, payload); err == nil {
			s = buf.String()
		} else {
			s = string(payload)
		}
	} else {
		s = string(payload)
	}
	// Escape pipe chars for Markdown table
	s = strings.ReplaceAll(s, "|", "\\|")
	if maxLen > 0 && len(s) > maxLen {
		return s[:maxLen] + "…"
	}
	return s
}
