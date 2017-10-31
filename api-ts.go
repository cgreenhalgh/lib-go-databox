package libDatabox

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"
)

// TimeSeries API for databox v 0.2.0.
//
// Strictly a subset of the API offered by the store-json and store-timeseries
// including common methods.
//
// Note that a single store value is a JSON-encoded object with fields timestamp 
// (float64) and data (value type). 
type TimeSeries_0_2_0 interface{
	// GetLatest returns the datasource's latest value, if any.
	// 
	// Returns a single json-encoded value object, or "" if there is no value.
	ReadLatest() (string, error) 
	
	// GetSince returns an array of values since time, inclusive.
	// 
	// Returns a json-encoded array of value objects.
	ReadSince(startTime time.Time) (string, error)
	
	// GetSince returns an array of values between startTime (inclusive) and
	// endTime (inclusive).
	// 
	// Returns a json-encoded array of value objects.
	ReadRange(startTime time.Time, endTime time.Time) (string, error) 
	
	// WriteData writes a JSON-encoded value with no surrounding value object
	// or timestamp.
	// The time associated with the data will be assigned by the store.
	WriteRawValue(data string) error

	// WriteDataAndTime writes a JSON-encoded value with no surrounding value
	// object for the specified timestamp.
	WriteRawValueAt(data string, time time.Time) error

	// Time2Timestamp returns the (possibly store-specific) timestamp
	// for the given time.
	Time2Timestamp(t time.Time) float64
	
	// Timestamp2Time converts the (possibly store-specific) timestamp
	// from a value object to a time.
	Timestamp2Time(ts float64) time.Time
	
	// GetStoreURL returns the store URL (only)
	StoreURL() (string, error) 
}

// Concrete implementation for store-json
// not exported
type jsonStore_TimeSeries_0_2_0 struct{
	href string
}

func (s jsonStore_TimeSeries_0_2_0) ReadLatest() (string, error) {

	data, err := makeStoreRequest(s.href+"/ts/latest", "GET")
	if err != nil {
		return "", err
	}
	// store-json returns an array, possible empty.
	if data == "[]" {
		// no value
		return "", nil
	} else {
		// remove enclosing [...]
		l := len(data)
		if data[0]=='[' && data[l-1]==']' {
			return data[1:l-1], nil
		} else {
			return "", errors.New("Unexpected return value from latest: "+data)
		}
	}
	
	return data, nil
}

func (s jsonStore_TimeSeries_0_2_0) ReadSince(startTime time.Time) (string, error) {

	startTimestamp := s.Time2Timestamp(startTime)
	params :=  "{\"startTimestamp\": "+strconv.FormatFloat(startTimestamp, 'f', -1, 64)+"}"
	data, err := makeStoreRequestJson(s.href+"/ts/since", "GET",  params)
	if err != nil {
		return "", err
	}

	return data, nil
}
func (s jsonStore_TimeSeries_0_2_0) ReadRange(startTime time.Time, endTime time.Time) (string, error) {

	startTimestamp := s.Time2Timestamp(startTime)
	endTimestamp := s.Time2Timestamp(endTime)
	params :=  "{\"startTimestamp\": "+strconv.FormatFloat(startTimestamp, 'f', -1, 64)+",\"endTimestamp\": "+strconv.FormatFloat(endTimestamp, 'f', -1, 64)+"}"
	data, err := makeStoreRequestJson(s.href+"/ts/range", "GET",  params)
	if err != nil {
		return "", err
	}

	return data, nil
}

func (s jsonStore_TimeSeries_0_2_0) WriteRawValue(data string) error {

	value := "{\"data\": "+data+"}"
	_, err := makeStoreRequestPOST(s.href+"/ts", value)
	if err != nil {
		return err
	}

	return nil
}

func (s jsonStore_TimeSeries_0_2_0) WriteRawValueAt(data string, time time.Time) error {

	value := "{\"data\": "+data+",\"timestamp\":"+strconv.FormatFloat(s.Time2Timestamp(time), 'f', -1, 64)+"}"
	_, err := makeStoreRequestPOST(s.href+"/ts", value)
	if err != nil {
		return err
	}

	return nil
}

func (jsonStore_TimeSeries_0_2_0) Time2Timestamp(t time.Time) float64 {
	// Json Store time is ms since unix epoch, int.
	return float64(t.Unix()*1000 + int64(t.Nanosecond()/1000000))
}

func (jsonStore_TimeSeries_0_2_0) Timestamp2Time(ts float64) time.Time {
	seconds := int64(ts)
	return time.Unix(seconds, int64((ts-float64(seconds))*1000000000))
}

func (s jsonStore_TimeSeries_0_2_0) StoreURL() (string, error) {
	return GetStoreURLFromDsHref(s.href)
}

const STORE_JSON = "store-json"

// Factory for a TimeSeries_0_2_0 object suitable for the store type.
// Ideally the created object should depend on the runtime databox version
// and the store type.
// Supported storeType currently 'store-json' only.
func MakeStoreTimeSeries_0_2_0(storeHref string, datasourceId string, storeType string) (TimeSeries_0_2_0, error) {
	if storeType == STORE_JSON {
		return jsonStore_TimeSeries_0_2_0{ href: storeHref+"/"+datasourceId }, nil
	} else {
		return nil, errors.New("(MakeStoreTimeSeries_0_2_0) Unsupported store type: "+storeType)
	}
}

// Factory for a TimeSeries_0_2_0 object suitable for the datasource.
// Ideally the created object should depend on the runtime databox version 
// and the datasource store type. 
// datasourceMetadata is the environment string passed to the app from the container
// manager which includes the datasource endpoint (href) and item-metadata 
// rel 'urn:X-databox:rels:hasStoreType' 
func MakeSourceTimeSeries_0_2_0(dsMetadata string) (TimeSeries_0_2_0, error) {
	meta := hypercat{}
	if err := json.Unmarshal([]byte(dsMetadata), &meta); err != nil {
		return nil, err
	}
	href := meta.Href
	storeType := getDsStoreType(meta)
	if storeType == STORE_JSON {
		return jsonStore_TimeSeries_0_2_0{ href: href }, nil
	} else if storeType == "" {
		return nil, errors.New("(MakeStoreTimeSeries_0_2_0) Unspecified store type for "+href)
	} else {
		return nil, errors.New("(MakeStoreTimeSeries_0_2_0) Unsupported store type: "+storeType)
	}
}
