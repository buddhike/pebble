// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.1
// 	protoc        v5.28.2
// source: messages.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type UserRecord struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	RecordID     []byte `protobuf:"bytes,1,opt,name=recordID,proto3" json:"recordID,omitempty"`
	PartitionKey string `protobuf:"bytes,2,opt,name=partitionKey,proto3" json:"partitionKey,omitempty"`
	Data         []byte `protobuf:"bytes,3,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *UserRecord) Reset() {
	*x = UserRecord{}
	mi := &file_messages_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UserRecord) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UserRecord) ProtoMessage() {}

func (x *UserRecord) ProtoReflect() protoreflect.Message {
	mi := &file_messages_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UserRecord.ProtoReflect.Descriptor instead.
func (*UserRecord) Descriptor() ([]byte, []int) {
	return file_messages_proto_rawDescGZIP(), []int{0}
}

func (x *UserRecord) GetRecordID() []byte {
	if x != nil {
		return x.RecordID
	}
	return nil
}

func (x *UserRecord) GetPartitionKey() string {
	if x != nil {
		return x.PartitionKey
	}
	return ""
}

func (x *UserRecord) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type Record struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ShardID     string        `protobuf:"bytes,1,opt,name=shardID,proto3" json:"shardID,omitempty"`
	UserRecords []*UserRecord `protobuf:"bytes,2,rep,name=userRecords,proto3" json:"userRecords,omitempty"`
}

func (x *Record) Reset() {
	*x = Record{}
	mi := &file_messages_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Record) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Record) ProtoMessage() {}

func (x *Record) ProtoReflect() protoreflect.Message {
	mi := &file_messages_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Record.ProtoReflect.Descriptor instead.
func (*Record) Descriptor() ([]byte, []int) {
	return file_messages_proto_rawDescGZIP(), []int{1}
}

func (x *Record) GetShardID() string {
	if x != nil {
		return x.ShardID
	}
	return ""
}

func (x *Record) GetUserRecords() []*UserRecord {
	if x != nil {
		return x.UserRecords
	}
	return nil
}

type QueryLeaderRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *QueryLeaderRequest) Reset() {
	*x = QueryLeaderRequest{}
	mi := &file_messages_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *QueryLeaderRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryLeaderRequest) ProtoMessage() {}

func (x *QueryLeaderRequest) ProtoReflect() protoreflect.Message {
	mi := &file_messages_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryLeaderRequest.ProtoReflect.Descriptor instead.
func (*QueryLeaderRequest) Descriptor() ([]byte, []int) {
	return file_messages_proto_rawDescGZIP(), []int{2}
}

type QueryLeaderResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	LeaderID  string `protobuf:"bytes,1,opt,name=leaderID,proto3" json:"leaderID,omitempty"`
	ErrorCode string `protobuf:"bytes,2,opt,name=errorCode,proto3" json:"errorCode,omitempty"`
}

func (x *QueryLeaderResponse) Reset() {
	*x = QueryLeaderResponse{}
	mi := &file_messages_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *QueryLeaderResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryLeaderResponse) ProtoMessage() {}

func (x *QueryLeaderResponse) ProtoReflect() protoreflect.Message {
	mi := &file_messages_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryLeaderResponse.ProtoReflect.Descriptor instead.
func (*QueryLeaderResponse) Descriptor() ([]byte, []int) {
	return file_messages_proto_rawDescGZIP(), []int{3}
}

func (x *QueryLeaderResponse) GetLeaderID() string {
	if x != nil {
		return x.LeaderID
	}
	return ""
}

func (x *QueryLeaderResponse) GetErrorCode() string {
	if x != nil {
		return x.ErrorCode
	}
	return ""
}

type CheckpointRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ShardID        string `protobuf:"bytes,1,opt,name=shardID,proto3" json:"shardID,omitempty"`
	WorkerID       string `protobuf:"bytes,2,opt,name=workerID,proto3" json:"workerID,omitempty"`
	SequenceNumber string `protobuf:"bytes,3,opt,name=sequenceNumber,proto3" json:"sequenceNumber,omitempty"`
}

func (x *CheckpointRequest) Reset() {
	*x = CheckpointRequest{}
	mi := &file_messages_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CheckpointRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CheckpointRequest) ProtoMessage() {}

func (x *CheckpointRequest) ProtoReflect() protoreflect.Message {
	mi := &file_messages_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CheckpointRequest.ProtoReflect.Descriptor instead.
func (*CheckpointRequest) Descriptor() ([]byte, []int) {
	return file_messages_proto_rawDescGZIP(), []int{4}
}

func (x *CheckpointRequest) GetShardID() string {
	if x != nil {
		return x.ShardID
	}
	return ""
}

func (x *CheckpointRequest) GetWorkerID() string {
	if x != nil {
		return x.WorkerID
	}
	return ""
}

func (x *CheckpointRequest) GetSequenceNumber() string {
	if x != nil {
		return x.SequenceNumber
	}
	return ""
}

type CheckpointResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Success   bool   `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	ErrorCode string `protobuf:"bytes,2,opt,name=errorCode,proto3" json:"errorCode,omitempty"`
}

func (x *CheckpointResponse) Reset() {
	*x = CheckpointResponse{}
	mi := &file_messages_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CheckpointResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CheckpointResponse) ProtoMessage() {}

func (x *CheckpointResponse) ProtoReflect() protoreflect.Message {
	mi := &file_messages_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CheckpointResponse.ProtoReflect.Descriptor instead.
func (*CheckpointResponse) Descriptor() ([]byte, []int) {
	return file_messages_proto_rawDescGZIP(), []int{5}
}

func (x *CheckpointResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *CheckpointResponse) GetErrorCode() string {
	if x != nil {
		return x.ErrorCode
	}
	return ""
}

var File_messages_proto protoreflect.FileDescriptor

var file_messages_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x06, 0x70, 0x65, 0x62, 0x62, 0x6c, 0x65, 0x22, 0x60, 0x0a, 0x0a, 0x55, 0x73, 0x65, 0x72,
	0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x12, 0x1a, 0x0a, 0x08, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64,
	0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x08, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64,
	0x49, 0x44, 0x12, 0x22, 0x0a, 0x0c, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x4b,
	0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74,
	0x69, 0x6f, 0x6e, 0x4b, 0x65, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x22, 0x58, 0x0a, 0x06, 0x52, 0x65,
	0x63, 0x6f, 0x72, 0x64, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x68, 0x61, 0x72, 0x64, 0x49, 0x44, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x73, 0x68, 0x61, 0x72, 0x64, 0x49, 0x44, 0x12, 0x34,
	0x0a, 0x0b, 0x75, 0x73, 0x65, 0x72, 0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x18, 0x02, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x70, 0x65, 0x62, 0x62, 0x6c, 0x65, 0x2e, 0x55, 0x73, 0x65,
	0x72, 0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x52, 0x0b, 0x75, 0x73, 0x65, 0x72, 0x52, 0x65, 0x63,
	0x6f, 0x72, 0x64, 0x73, 0x22, 0x14, 0x0a, 0x12, 0x51, 0x75, 0x65, 0x72, 0x79, 0x4c, 0x65, 0x61,
	0x64, 0x65, 0x72, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x4f, 0x0a, 0x13, 0x51, 0x75,
	0x65, 0x72, 0x79, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x1a, 0x0a, 0x08, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x49, 0x44, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x08, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x49, 0x44, 0x12, 0x1c, 0x0a,
	0x09, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x09, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x22, 0x71, 0x0a, 0x11, 0x43,
	0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x18, 0x0a, 0x07, 0x73, 0x68, 0x61, 0x72, 0x64, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x07, 0x73, 0x68, 0x61, 0x72, 0x64, 0x49, 0x44, 0x12, 0x1a, 0x0a, 0x08, 0x77, 0x6f,
	0x72, 0x6b, 0x65, 0x72, 0x49, 0x44, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x77, 0x6f,
	0x72, 0x6b, 0x65, 0x72, 0x49, 0x44, 0x12, 0x26, 0x0a, 0x0e, 0x73, 0x65, 0x71, 0x75, 0x65, 0x6e,
	0x63, 0x65, 0x4e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0e,
	0x73, 0x65, 0x71, 0x75, 0x65, 0x6e, 0x63, 0x65, 0x4e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x22, 0x4c,
	0x0a, 0x12, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x12, 0x1c,
	0x0a, 0x09, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x09, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x42, 0x06, 0x5a, 0x04,
	0x2e, 0x2f, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_messages_proto_rawDescOnce sync.Once
	file_messages_proto_rawDescData = file_messages_proto_rawDesc
)

func file_messages_proto_rawDescGZIP() []byte {
	file_messages_proto_rawDescOnce.Do(func() {
		file_messages_proto_rawDescData = protoimpl.X.CompressGZIP(file_messages_proto_rawDescData)
	})
	return file_messages_proto_rawDescData
}

var file_messages_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_messages_proto_goTypes = []any{
	(*UserRecord)(nil),          // 0: pebble.UserRecord
	(*Record)(nil),              // 1: pebble.Record
	(*QueryLeaderRequest)(nil),  // 2: pebble.QueryLeaderRequest
	(*QueryLeaderResponse)(nil), // 3: pebble.QueryLeaderResponse
	(*CheckpointRequest)(nil),   // 4: pebble.CheckpointRequest
	(*CheckpointResponse)(nil),  // 5: pebble.CheckpointResponse
}
var file_messages_proto_depIdxs = []int32{
	0, // 0: pebble.Record.userRecords:type_name -> pebble.UserRecord
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_messages_proto_init() }
func file_messages_proto_init() {
	if File_messages_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_messages_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_messages_proto_goTypes,
		DependencyIndexes: file_messages_proto_depIdxs,
		MessageInfos:      file_messages_proto_msgTypes,
	}.Build()
	File_messages_proto = out.File
	file_messages_proto_rawDesc = nil
	file_messages_proto_goTypes = nil
	file_messages_proto_depIdxs = nil
}