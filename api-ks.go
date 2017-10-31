package libDatabox

import (
	"encoding/json"
	"errors"
)

// Datasource KeyValue API for databox v 0.2.0.
//
// Strictly a subset of the API offered by the store-json and store-timeseries.
type KeyValue_0_2_0 interface{
	// Read json-encoded value.
	//
	// Returns an error (404 not found) if no value was previously written.
	Read() (string, error)
	
	// Write json-encoded value.
	Write(data string) error

	// GetStoreURL returns the store URL (only)
	StoreURL() (string, error) 
}

// Concrete implementation for store-json
// not exported
type jsonStore_KeyValue_0_2_0 struct{
	href string
}

func (s jsonStore_KeyValue_0_2_0) Write(data string) error {

	_, err := makeStoreRequestPOST(s.href+"/kv", data)
	if err != nil {
		return err
	}

	return nil
}

func (s jsonStore_KeyValue_0_2_0) Read() (string, error) {

	data, err := makeStoreRequest(s.href+"/kv", "GET")
	if err != nil {
		return "", err
	}

	return data, nil
}

func (s jsonStore_KeyValue_0_2_0) StoreURL() (string, error) {
	return GetStoreURLFromDsHref(s.href)
}

// Factory for a KeyValue_0_2_0 object suitable for the store type.
// Ideally the created object should depend on the runtime databox version
// and the store type.
// Supported storeType currently 'store-json' only.
func MakeStoreKeyValue_0_2_0(storeHref string, datasourceId string, storeType string) (KeyValue_0_2_0, error) {
	if storeType == STORE_JSON {
		return jsonStore_KeyValue_0_2_0{ href: storeHref+"/"+datasourceId }, nil
	} else {
		return nil, errors.New("(MakeStoreKeyValue_0_2_0) Unsupported store type: "+storeType)
	}
}

// Factory for a KeyValue_0_2_0 object suitable for the datasource.
// Ideally the created object should depend on the runtime databox version 
// and the datasource store type. 
// datasourceMetadata is the environment string passed to the app from the container
// manager which includes the datasource endpoint (href) and item-metadata 
// rel 'urn:X-databox:rels:hasStoreType' 
func MakeSourcKeyValue_0_2_0(dsMetadata string) (KeyValue_0_2_0, error) {
	meta := hypercat{}
	if err := json.Unmarshal([]byte(dsMetadata), &meta); err != nil {
		return nil, err
	}
	href := meta.Href
	storeType := getDsStoreType(meta)
	if storeType == STORE_JSON {
		return jsonStore_KeyValue_0_2_0{ href: href }, nil
	} else if storeType == "" {
		return nil, errors.New("(MakeStoreKeyValue_0_2_0) Unspecified store type for "+href)
	} else {
		return nil, errors.New("(MakeStoreKeyValue_0_2_0) Unsupported store type: "+storeType)
	}
}
