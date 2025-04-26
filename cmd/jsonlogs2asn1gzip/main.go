package main

import (
	"bufio"
	"encoding/asn1"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log"
	"os"
	"strings"
	"time"

	bg "github.com/takanoriyanagitani/go-blobs2gzip"
)

var ErrInvalidTime error = errors.New("invalid time")

type Severity = asn1.Enumerated

const (
	SeverityUnspecified Severity = 0
	SeverityTrace       Severity = 1
	SeverityDebug       Severity = 5
	SeverityInfo        Severity = 9
	SeverityWarn        Severity = 13
	SeverityError       Severity = 17
	SeverityFatal       Severity = 21
)

type SeverityStringToEnum func(string) Severity

func SeverityStringToEnumFromMap(m map[string]Severity) SeverityStringToEnum {
	return func(s string) Severity {
		val, found := m[s]
		switch found {
		case true:
			return val
		default:
			return SeverityUnspecified
		}
	}
}

var SeverityStringToEnumMapDefault map[string]Severity = map[string]Severity{
	"trace":   SeverityTrace,
	"Trace":   SeverityTrace,
	"TRACE":   SeverityTrace,
	"debug":   SeverityDebug,
	"Debug":   SeverityDebug,
	"DEBUG":   SeverityDebug,
	"info":    SeverityInfo,
	"Info":    SeverityInfo,
	"INFO":    SeverityInfo,
	"warn":    SeverityWarn,
	"Warn":    SeverityWarn,
	"WARN":    SeverityWarn,
	"warning": SeverityWarn,
	"Warning": SeverityWarn,
	"WARNING": SeverityWarn,
	"error":   SeverityError,
	"Error":   SeverityError,
	"ERROR":   SeverityError,
	"fatal":   SeverityFatal,
	"Fatal":   SeverityFatal,
	"FATAL":   SeverityFatal,
}

var SeverityStringToEnumDefault SeverityStringToEnum = SeverityStringToEnumFromMap(
	SeverityStringToEnumMapDefault,
)

type KeyVal[T any] struct {
	Key string `asn1:"utf8"`
	Val T
}

type KeyValI KeyVal[int64]

type KeyValB KeyVal[bool]

type KeyValS struct {
	Key string `asn1:"utf8"`
	Val string `asn1:"utf8"`
}

type Attributes struct {
	Strs  []KeyValS
	Ints  []KeyValI
	Bools []KeyValB
}

type Resource struct {
	Strs  []KeyValS
	Ints  []KeyValI
	Bools []KeyValB
}

type UnixtimeUs int64

type LogItem struct {
	Timestamp UnixtimeUs
	Severity  Severity
	Message   string `asn1:"utf8"`
	Resource
	Attributes
}

func (l LogItem) ToDerBytes() ([]byte, error) {
	return asn1.Marshal(l)
}

func (l LogItem) WithMessage(msg any) LogItem {
	switch s := msg.(type) {
	case string:
		l.Message = s
	default:
		l.Message = fmt.Sprintf("%v", msg)
	}
	return l
}

func (l LogItem) WithTime(unixtime UnixtimeUs) LogItem {
	l.Timestamp = unixtime
	return l
}

func (l LogItem) WithSeverity(severity Severity) LogItem {
	l.Severity = severity
	return l
}

type FlatLog map[string]any

func KeySetFromStr(s string, sep string) map[string]struct{} {
	ret := map[string]struct{}{}
	var splited []string = strings.Split(s, sep)
	for _, key := range splited {
		ret[key] = struct{}{}
	}
	return ret
}

type ResourceKeys map[string]struct{}

func (r ResourceKeys) SetStr(key, val string, i LogItem) LogItem {
	i.Resource.Strs = append(i.Resource.Strs, KeyValS{
		Key: key,
		Val: val,
	})
	return i
}

func (r ResourceKeys) SetInt(key string, val int64, i LogItem) LogItem {
	i.Resource.Ints = append(i.Resource.Ints, KeyValI{
		Key: key,
		Val: val,
	})
	return i
}

func (r ResourceKeys) SetBool(key string, val bool, i LogItem) LogItem {
	i.Resource.Bools = append(i.Resource.Bools, KeyValB{
		Key: key,
		Val: val,
	})
	return i
}

func (r ResourceKeys) SetResource(f FlatLog, i LogItem) LogItem {
	for key := range r {
		val, found := f[key]
		if !found {
			continue
		}

		switch s := val.(type) {
		case string:
			i = r.SetStr(key, s, i)
		case float64:
			i = r.SetInt(key, int64(s), i)
		case bool:
			i = r.SetBool(key, s, i)
		default:
			continue
		}
	}

	return i
}

type AttributeKeys map[string]struct{}

func (a AttributeKeys) SetStr(key, val string, i LogItem) LogItem {
	i.Attributes.Strs = append(i.Attributes.Strs, KeyValS{
		Key: key,
		Val: val,
	})
	return i
}

func (a AttributeKeys) SetInt(key string, val int64, i LogItem) LogItem {
	i.Attributes.Ints = append(i.Attributes.Ints, KeyValI{
		Key: key,
		Val: val,
	})
	return i
}

func (a AttributeKeys) SetBool(key string, val bool, i LogItem) LogItem {
	i.Attributes.Bools = append(i.Attributes.Bools, KeyValB{
		Key: key,
		Val: val,
	})
	return i
}

func (a AttributeKeys) SetAttrs(f FlatLog, i LogItem) LogItem {
	for key := range a {
		val, found := f[key]
		if !found {
			continue
		}

		switch s := val.(type) {
		case string:
			i = a.SetStr(key, s, i)
		case float64:
			i = a.SetInt(key, int64(s), i)
		case bool:
			i = a.SetBool(key, s, i)
		default:
			continue
		}
	}

	return i
}

type TimestampKey string

var TimestampKeyDefault TimestampKey = "time"

type TimeParser func(string) (time.Time, error)

type LogToTime func(FlatLog) (time.Time, error)

func (l LogToTime) SetTime(f FlatLog, i LogItem) LogItem {
	t, e := l(f)
	if nil == e {
		return i.WithTime(UnixtimeUs(t.UnixMicro()))
	}
	return i
}

func (p TimeParser) LogToTime(k TimestampKey) LogToTime {
	return func(f FlatLog) (time.Time, error) {
		var empty time.Time

		var val any = f[string(k)]
		switch s := val.(type) {
		case bool:
			return empty, fmt.Errorf("%w: %v", ErrInvalidTime, s)
		case string:
			return p(s)
		case float64:
			return time.UnixMicro(int64(s * 1000 * 1000)), nil
		default:
			return empty, fmt.Errorf("%w: %v", ErrInvalidTime, val)
		}
	}
}

type TimeLayout string

func (l TimeLayout) ToParser() TimeParser {
	return func(s string) (time.Time, error) {
		return time.Parse(string(l), s)
	}
}

var TimeLayoutDefault TimeLayout = time.RFC3339Nano

var TimeParserDefault TimeParser = TimeLayoutDefault.ToParser()

var LogToTimeDefault LogToTime = TimeParserDefault.
	LogToTime(TimestampKeyDefault)

type SeverityKey string

var SeverityKeyDefault SeverityKey = "severity"

type LogToSev func(FlatLog) Severity

func (l LogToSev) SetSeverity(f FlatLog, i LogItem) LogItem {
	return i.WithSeverity(l(f))
}

func (c SeverityStringToEnum) LogToSeverity(k SeverityKey) LogToSev {
	return func(f FlatLog) Severity {
		var val any = f[string(k)]
		switch s := val.(type) {
		case string:
			return c(s)
		default:
			return SeverityUnspecified
		}
	}
}

type MessageKey string

var MessageKeyDefault MessageKey = "body"

func (m MessageKey) SetMsg(f FlatLog, l LogItem) LogItem {
	var val any = f[string(m)]
	return l.WithMessage(val)
}

type FlatToAsn1Config struct {
	MessageKey
	ResourceKeys
	AttributeKeys
	TimestampKey
	TimeLayout
	SeverityKey
	SeverityStrToMap map[string]Severity
}

var FlatToAsn1ConfigDefault FlatToAsn1Config = FlatToAsn1Config{
	MessageKey:       MessageKeyDefault,
	ResourceKeys:     map[string]struct{}{},
	AttributeKeys:    map[string]struct{}{},
	TimestampKey:     TimestampKeyDefault,
	TimeLayout:       TimeLayoutDefault,
	SeverityKey:      SeverityKeyDefault,
	SeverityStrToMap: SeverityStringToEnumMapDefault,
}

func (c FlatToAsn1Config) WithKeysR(keys ResourceKeys) FlatToAsn1Config {
	c.ResourceKeys = keys
	return c
}

func (c FlatToAsn1Config) WithKeysA(keys AttributeKeys) FlatToAsn1Config {
	c.AttributeKeys = keys
	return c
}

func (c FlatToAsn1Config) ToLogToTime() LogToTime {
	var tp TimeParser = c.TimeLayout.ToParser()
	return tp.LogToTime(c.TimestampKey)
}

func (c FlatToAsn1Config) ToLogToSev() LogToSev {
	var s2e SeverityStringToEnum = SeverityStringToEnumFromMap(
		c.SeverityStrToMap,
	)
	return s2e.LogToSeverity(c.SeverityKey)
}

func (c FlatToAsn1Config) ToConverter() FlatToAsn1 {
	return FlatToAsn1{
		MessageKey:    c.MessageKey,
		ResourceKeys:  c.ResourceKeys,
		AttributeKeys: c.AttributeKeys,
		LogToTime:     c.ToLogToTime(),
		LogToSev:      c.ToLogToSev(),
	}
}

type FlatToAsn1 struct {
	MessageKey
	ResourceKeys
	AttributeKeys
	LogToTime
	LogToSev
}

var FlatToAsn1Default FlatToAsn1 = FlatToAsn1ConfigDefault.ToConverter()

func (c FlatToAsn1) SetTime(f FlatLog, i LogItem) LogItem {
	return c.LogToTime.SetTime(f, i)
}

func (c FlatToAsn1) SetSeverity(f FlatLog, i LogItem) LogItem {
	return c.LogToSev.SetSeverity(f, i)
}

func (c FlatToAsn1) SetMsg(f FlatLog, i LogItem) LogItem {
	return c.MessageKey.SetMsg(f, i)
}

func (c FlatToAsn1) SetResources(f FlatLog, i LogItem) LogItem {
	return c.ResourceKeys.SetResource(f, i)
}

func (c FlatToAsn1) SetAttributes(f FlatLog, i LogItem) LogItem {
	return c.AttributeKeys.SetAttrs(f, i)
}

func (c FlatToAsn1) Convert(f FlatLog) (i LogItem) {
	fa := []func(FlatLog, LogItem) LogItem{
		c.SetTime,
		c.SetSeverity,
		c.SetMsg,
		c.SetResources,
		c.SetAttributes,
	}
	for _, fn := range fa {
		i = fn(f, i)
	}
	return
}

func (c FlatToAsn1) FlatLogsToItems(
	f iter.Seq2[FlatLog, error],
) iter.Seq2[LogItem, error] {
	return func(yield func(LogItem, error) bool) {
		var empty LogItem
		for fl, e := range f {
			if nil != e {
				yield(empty, e)
				return
			}

			if !yield(c.Convert(fl), nil) {
				return
			}
		}
	}
}

func LinesToFlatLogs(lines iter.Seq[[]byte]) iter.Seq2[FlatLog, error] {
	return func(yield func(FlatLog, error) bool) {
		var buf FlatLog
		for line := range lines {
			clear(buf)

			e := json.Unmarshal(line, &buf)
			if !yield(buf, e) {
				return
			}
		}
	}
}

type LogItems iter.Seq2[LogItem, error]

func (l LogItems) ToBlobs() iter.Seq2[bg.Blob, error] {
	return func(yield func(bg.Blob, error) bool) {
		var empty bg.Blob
		for logitem, e := range l {
			if nil != e {
				yield(empty, e)
				return
			}
			der, e := logitem.ToDerBytes()
			if !yield(der, e) {
				return
			}
		}
	}
}

func (c FlatToAsn1) LinesToBlobs(
	lines iter.Seq[[]byte],
) iter.Seq2[bg.Blob, error] {
	var flogs iter.Seq2[FlatLog, error] = LinesToFlatLogs(lines)
	var items iter.Seq2[LogItem, error] = c.FlatLogsToItems(flogs)
	return LogItems(items).ToBlobs()
}

func (c FlatToAsn1) LinesToBlobsToStdout(lines iter.Seq[[]byte]) error {
	var blobs iter.Seq2[bg.Blob, error] = c.LinesToBlobs(lines)
	var bw *bufio.Writer = bufio.NewWriter(os.Stdout)
	defer bw.Flush()
	return bg.BlobsToWriter(bw)(bg.Blobs(blobs))
}

func (c FlatToAsn1) StdinToLinesToBlobsToStdout() error {
	var s *bufio.Scanner = bufio.NewScanner(os.Stdin)
	var lines iter.Seq[[]byte] = func(
		yield func([]byte) bool,
	) {
		for s.Scan() {
			var line []byte = s.Bytes()
			if !yield(line) {
				return
			}
		}
	}
	return c.LinesToBlobsToStdout(lines)
}

func KeysFromEnv(ekey string) map[string]struct{} {
	var val string = os.Getenv(ekey)
	return KeySetFromStr(val, ",")
}

var KeysFromEnvR map[string]struct{} = KeysFromEnv("ENV_KEYS_R")

var KeysFromEnvA map[string]struct{} = KeysFromEnv("ENV_KEYS_A")

func main() {
	var fc FlatToAsn1Config = FlatToAsn1ConfigDefault.
		WithKeysR(ResourceKeys(KeysFromEnvR)).
		WithKeysA(AttributeKeys(KeysFromEnvA))
	var f2a FlatToAsn1 = fc.ToConverter()

	var err error = f2a.StdinToLinesToBlobsToStdout()
	if nil != err {
		log.Printf("%v\n", err)
	}
}
