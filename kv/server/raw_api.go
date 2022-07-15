package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawGetResponse{Value: val}
	if val == nil {
		resp.NotFound = true
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	if err := server.storage.Write(nil, []storage.Modify{{Data: put}}); err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	if err := server.storage.Write(nil, []storage.Modify{{Data: del}}); err != nil {
		return nil, err
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	iterator := reader.IterCF(req.GetCf())
	iterator.Seek(req.StartKey)
	kvs := make([]*kvrpcpb.KvPair, 0)
	for i := 0; i < int(req.GetLimit()); i++ {
		if !iterator.Valid() {
			break
		}
		k := iterator.Item().Key()
		v, _ := iterator.Item().Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		})
		iterator.Next()
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
